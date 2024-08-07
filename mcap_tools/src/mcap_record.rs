/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away

mod misc;
use misc::get_params;
use misc::get_master_client;
use std::{collections::BTreeMap, fs, io::BufWriter};
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

roslibrust_codegen_macro::find_and_generate_ros_messages!();

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // view log messages from roslibrust in stdout
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
        // .without_timestamps() // required for running wsl2
        .init()
        .unwrap();

    let mut params = HashMap::<String, String>::new();
    params.insert("_name".to_string(), "mcap_record".to_string());
    let (ns, full_node_name, _unused_args) = get_params(&mut params);
    let master_client = get_master_client(&full_node_name).await?;
    let nh = {
        let master_uri = std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());
        roslibrust::ros1::NodeHandle::new(&master_uri, &full_node_name).await?
    };
    let mcap_name = "out.mcap";
    let mut mcap_out = mcap::Writer::new(BufWriter::new(fs::File::create(mcap_name)?))?;
    let mcap_out = Arc::new(Mutex::new(mcap_out));
    /*
    let mut writer = mcap::WriteOptions::new()
        .compression(None)
        .profile("")
        .create
    */

    // Setup a task to kill this process when ctrl_c comes in:
    {
        let mcap_out_copy = mcap_out.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            mcap_out_copy.lock().unwrap().finish();
            std::process::exit(0);
        });
    }

    let mut subscribers = HashMap::new();
    // let mut channels = HashMap::new();

    let mut old_topics = HashSet::<(String, String)>::new();
    loop {
        // TODO(lucasw) optionally limit to namespace of this node (once this node can be launched into
        // a namespace)
        let topics = master_client.get_published_topics(ns.clone()).await?;
        let mut cur_topics = HashSet::<(String, String)>::new();
        for (topic_name, topic_type) in &topics {
            cur_topics.insert(((*topic_name).clone(), (*topic_type).clone()));
            // println!("{topic_name} - {topic_type}");
        }

        for topic_and_type in &old_topics {
            if !cur_topics.contains(topic_and_type) {
                println!("removed {topic_and_type:?}");
            }
        }
        for topic_and_type in &cur_topics {
            if !old_topics.contains(topic_and_type) {
                println!("added {topic_and_type:?}");
                let (topic, topic_type) = topic_and_type;
                let topic_copy = topic.clone();
                // TODO(lucasw) the type is almost certainly not std_msgs::ByteMultiArray,
                // but need to provide some type to downstream machinery
                let mut subscriber = nh.subscribe_any::<std_msgs::ByteMultiArray>(topic, topic_type, 10).await?;

                // maybe should do arc mutex
                let nh_copy = nh.clone();
                let mcap_out = mcap_out.clone();

                let rv = tokio::spawn(async move {
                    let mut sequence = 0;
                    // TODO(lucasw) make these Option?
                    let mut have_definition = false;
                    let mut definition;
                    let mut channel_id = 0;
                    // TODO(lucasw) seeing some message arrive repeatedly, but only if the
                    // publisher starts after this node does?
                    while let Some(data) = subscriber.next_raw().await {
                        if let Ok(data) = data {
                            log::debug!("Got raw message data: {} bytes, {:?}", data.len(), data);
                            // log::debug("{:}",
                            if !have_definition {
                                println!("get definition");
                                definition = nh_copy.inner.get_definition(topic_copy.clone()).await;
                                if let Ok(definition) = definition {
                                    have_definition = true;
                                    println!("definition: '{definition}'");
                                    let schema = mcap::Schema {
                                        name: topic_copy.clone(),
                                        encoding: "ros1msg".to_string(),
                                        // TODO(lucasw) definition doesn't live long enough
                                        data: std::borrow::Cow::Borrowed(definition.as_bytes()),
                                    };
                                    let channel = mcap::Channel {
                                        topic: topic_copy.clone(),
                                        schema: Some(std::sync::Arc::new(schema.to_owned())),
                                        message_encoding: "ros1msg".to_string(),
                                        metadata: BTreeMap::default(),
                                    };
                                    // TODO(lucasw) how to do ? in async block?
                                    channel_id = mcap_out.lock().unwrap().add_channel(&channel).unwrap();
                                    // channels.insert(topic_copy.clone(), channel_id);
                                }
                            }
                            // let channel_id = channels.get(&topic_copy).unwrap();
                            // TODO(lucasw) u64 will only get us to the year 2554...
                            let ns_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
                            mcap_out.lock().unwrap().write_to_known_channel(
                                &mcap::records::MessageHeader {
                                    channel_id,
                                    sequence,
                                    log_time: ns_epoch,
                                    // TODO(lucasw) get this from somewhere
                                    publish_time: ns_epoch,
                                },
                                &*data,
                            ).unwrap();
                            sequence += 1;
                        } else {
                            log::error!("{data:?}");
                        }
                    }
                });

                subscribers.insert(topic.clone(), rv);
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        old_topics = cur_topics;
    }

    // Ok(())
}
