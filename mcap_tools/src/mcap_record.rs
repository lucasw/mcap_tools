/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away

mod misc;
use misc::get_params;
use misc::get_master_client;
use std::borrow::Cow;
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
    let mcap_out = mcap::Writer::new(BufWriter::new(fs::File::create(mcap_name)?))?;
    let mcap_out = Arc::new(Mutex::new(mcap_out));

    let schemas = Arc::new(Mutex::new(HashMap::new()));
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
            log::debug!("closing mcap");
            let _ = mcap_out_copy.lock().unwrap().finish();
            std::process::exit(0);
        });
    }

    let mut old_topics = HashSet::<(String, String)>::new();
    loop {
        // TODO(lucasw) optionally limit to namespace of this node (once this node can be launched into
        // a namespace)
        let topics = master_client.get_published_topics(ns.clone()).await?;
        let cur_topics = {
            let mut cur_topics = HashSet::<(String, String)>::new();
            for (topic_name, topic_type) in &topics {
                cur_topics.insert(((*topic_name).clone(), (*topic_type).clone()));
                // log::debug!("{topic_name} - {topic_type}");
            }
            cur_topics
        };

        for topic_and_type in &old_topics {
            if !cur_topics.contains(topic_and_type) {
                log::debug!("removed {topic_and_type:?}");
            }
        }

        for topic_and_type in &cur_topics {
            if old_topics.contains(topic_and_type) {
                continue;
            }

            log::debug!("added {topic_and_type:?}");
            let (topic, topic_type) = topic_and_type.clone();
            // TODO(lucasw) the type is almost certainly not std_msgs::ByteMultiArray,
            // but need to provide some type to downstream machinery
            let queue_size = 500;
            let mut subscriber = nh.subscribe_any(&topic, queue_size).await?;

            // maybe should do arc mutex
            let nh_copy = nh.clone();
            let mcap_out = mcap_out.clone();

            let schemas_copy = schemas.clone();
            let _sub = tokio::spawn(async move {
                let mut sequence = 0;
                let mut channel_id = None;
                // TODO(lucasw) seeing some message arrive repeatedly, but only if the
                // publisher starts after this node does?
                while let Some(data) = subscriber.next().await {
                    if let Ok(data) = data {
                        // log::debug!("Got raw message data: {} bytes, {:?}", data.len(), data);
                        if channel_id.is_none() {
                            if !schemas_copy.lock().unwrap().contains_key(&topic_type.clone()) {
                                log::debug!("get definition");
                                let definition = nh_copy.inner.get_definition(topic.clone()).await.unwrap();
                                log::debug!("definition: '{definition}'");
                                let schema = mcap::Schema {
                                    name: topic_type.clone(),
                                    encoding: "ros1msg".to_string(),
                                    // TODO(lucasw) definition doesn't live long enough
                                    data: Cow::from(definition.as_bytes().to_owned()),
                                };
                                let schema = Some(Arc::new(schema.to_owned()));
                                schemas_copy.lock().unwrap().insert(topic_type.to_owned().to_string(), schema);
                                // log::warn!("can't get definition {topic} {topic_type}");
                            }
                            let schema = schemas_copy.lock().unwrap().get(&topic_type).unwrap().clone();

                            let channel = mcap::Channel {
                                topic: topic.clone(),
                                schema: schema,
                                message_encoding: "ros1msg".to_string(),
                                metadata: BTreeMap::default(),
                            };

                            channel_id = Some(mcap_out.lock().unwrap().add_channel(&channel).unwrap());
                        }

                        // TODO(lucasw) u64 will only get us to the year 2554...
                        let ns_epoch = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64;
                        mcap_out.lock().unwrap().write_to_known_channel(
                            &mcap::records::MessageHeader {
                                channel_id: channel_id.unwrap(),
                                sequence,
                                log_time: ns_epoch,
                                // TODO(lucasw) get this from somewhere
                                publish_time: ns_epoch,
                            },
                            &data[4..],  // chop off header bytes
                        ).unwrap();

                        sequence += 1;
                    } else {
                        log::error!("{data:?}");
                    }
                }  // get new messages in loop
            });  // subscriber
        }  // loop through current topics
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        old_topics = cur_topics.clone().to_owned();
    }

    // Ok(())
}
