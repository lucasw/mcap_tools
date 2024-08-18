/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away
use mcap_tools::misc;
use regex::Regex;
use std::borrow::Cow;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::{collections::BTreeMap, fs, io::BufWriter};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // view log messages from roslibrust in stdout
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        // .without_timestamps() // required for running wsl2
        .init()
        .unwrap();

    let mut params = HashMap::<String, String>::new();
    params.insert("_name".to_string(), "mcap_record".to_string());
    let (ns, full_node_name, unused_args) = misc::get_params(&mut params);

    let re;
    if unused_args.len() > 1 {
        let regex_str = &unused_args[1];
        re = Some(Regex::new(regex_str)?);
        log::debug!("{re:?}");
    } else {
        re = None;
    }

    let master_client = misc::get_master_client(&full_node_name).await?;
    let nh = {
        let master_uri =
            std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());
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

    use std::sync::atomic::{AtomicUsize, Ordering};
    let receive_messages = Arc::new(AtomicUsize::new(0));

    // create a tokio thread for every topic that appears, and send all received data to a thread
    // that does writing to mcap
    // TODO(lucasw) is this any more performant than the writer inside the receive thread?
    let (sender, receiver) = std::sync::mpsc::sync_channel(8000);

    {
        let mcap_out = mcap_out.clone();
        let receive_messages = receive_messages.clone();
        tokio::spawn(async move {
            let mut count = 0;
            loop {
                let rv = receiver.recv();
                match rv {
                    Ok(rv) => {
                        let (channel_id, arrival_ns_epoch, sequence, data): (
                            u16,
                            u64,
                            u32,
                            Vec<u8>,
                        ) = rv;
                        mcap_out
                            .lock()
                            .unwrap()
                            .write_to_known_channel(
                                &mcap::records::MessageHeader {
                                    channel_id,
                                    sequence,
                                    log_time: arrival_ns_epoch,
                                    // TODO(lucasw) get this from somewhere
                                    publish_time: arrival_ns_epoch,
                                },
                                &data[4..], // chop off header bytes
                            )
                            .unwrap();
                        if count % 1000 == 0 {
                            log::info!(
                                "{count} written / {} received",
                                receive_messages.load(Ordering::SeqCst)
                            );
                        }
                        count += 1;
                    }
                    Err(e) => {
                        log::error!("{e:?}");
                        break;
                    }
                }
            } // loop
            log::info!("{count} message written");
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

            let (topic, topic_type) = topic_and_type.clone();

            // TODO(lucasw) topic_type matching would be useful as well
            if let Some(ref re) = re {
                if re.captures(&topic).is_none() {
                    log::debug!("not recording from {topic}");
                    continue;
                }
            }

            log::info!("recording from {topic} {topic_type}");
            let queue_size = 2000;
            let mut subscriber = nh.subscribe_any(&topic, queue_size).await?;

            // maybe should do arc mutex
            let nh_copy = nh.clone();
            let mcap_out = mcap_out.clone();
            let sender = sender.clone();
            let receive_messages = receive_messages.clone();

            let schemas_copy = schemas.clone();

            let _sub = tokio::spawn(async move {
                let mut sequence = 0;
                let mut channel_id = None;
                // TODO(lucasw) seeing some message arrive repeatedly, but only if the
                // publisher starts after this node does?
                while let Some(data) = subscriber.next().await {
                    // TODO(lucasw) u64 will only get us to the year 2554...
                    let arrival_ns_epoch = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;

                    if let Ok(data) = data {
                        // log::debug!("Got raw message data: {} bytes, {:?}", data.len(), data);
                        if channel_id.is_none() {
                            // setup schema and channel for this topic
                            if !schemas_copy
                                .lock()
                                .unwrap()
                                .contains_key(&topic_type.clone())
                            {
                                log::debug!("get definition");
                                let definition =
                                    nh_copy.inner.get_definition(topic.clone()).await.unwrap();
                                // log::debug!("definition: '{definition}'");
                                let schema = mcap::Schema {
                                    name: topic_type.clone(),
                                    encoding: "ros1msg".to_string(),
                                    // TODO(lucasw) definition doesn't live long enough
                                    data: Cow::from(definition.as_bytes().to_owned()),
                                };
                                let schema = Some(Arc::new(schema.to_owned()));
                                schemas_copy
                                    .lock()
                                    .unwrap()
                                    .insert(topic_type.to_owned().to_string(), schema);
                                // log::warn!("can't get definition {topic} {topic_type}");
                            }
                            let schema = schemas_copy
                                .lock()
                                .unwrap()
                                .get(&topic_type)
                                .unwrap()
                                .clone();

                            let channel = mcap::Channel {
                                topic: topic.clone(),
                                schema,
                                message_encoding: "ros1".to_string(),
                                metadata: BTreeMap::default(),
                            };

                            channel_id =
                                Some(mcap_out.lock().unwrap().add_channel(&channel).unwrap());
                        }

                        let send_rv =
                            sender.send((channel_id.unwrap(), arrival_ns_epoch, sequence, data));
                        match send_rv {
                            Ok(()) => {}
                            Err(e) => {
                                log::error!("{channel_id:?} {arrival_ns_epoch} {sequence} {e:?}");
                            }
                        }

                        sequence += 1;
                        receive_messages.fetch_add(1, Ordering::SeqCst);
                    } else {
                        log::error!("{data:?}");
                    }
                } // get new messages in loop
            }); // subscriber
        } // loop through current topics
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        old_topics = cur_topics.clone().to_owned();
    }

    // Ok(())
}
