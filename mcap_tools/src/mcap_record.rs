/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away
use clap::{arg, command};
use mcap_tools::misc;
use regex::Regex;
use std::borrow::Cow;
use std::collections::HashSet;
use std::collections::{hash_map, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use std::{collections::BTreeMap, fs, io::BufWriter};
use tokio::time::Duration;

use crate::misc::std_srvs;

fn rename_active(mcap_name: &str) -> std::io::Result<()> {
    // TODO(lucasw) only replace last occurence
    let inactive_name = mcap_name.replace(".mcap.active", ".mcap");
    std::fs::rename(mcap_name, inactive_name)
}

// start all the threads
async fn mcap_record(
    full_node_name: String,
    prefix: String,
    channel_receiver: mpsc::Receiver<(String, String, String)>,
    connection_summaries: Arc<Mutex<HashMap<String, BTreeMap<String, String>>>>,
    msg_receiver: mpsc::Receiver<(String, String, u64, u32, Vec<u8>)>,
    finish: Arc<AtomicBool>,
    num_received_messages: Arc<AtomicUsize>,
    size_limit: u64,
) -> Result<(), anyhow::Error> {
    // schemas should be able to persist across mcap file boundaries
    let schemas = Arc::new(Mutex::new(HashMap::new()));

    // Setup a task to kill this process when ctrl_c comes in:
    {
        let finish = finish.clone();
        let full_node_name = full_node_name.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            // TODO(lucasw) give the msg receiver thread a chance to cleanly finish the mcap
            finish.store(true, Ordering::SeqCst);
            log::warn!("{full_node_name} ctrl-c, going to finish");
            tokio::time::sleep(Duration::from_secs(2)).await;
            log::error!("{full_node_name} exiting without finished mcap");
            std::process::exit(0);
        });
    }

    // a thread to listen on channel_receiver
    {
        let schemas = schemas.clone();
        tokio::spawn(async move {
            let mut count = 0;
            loop {
                let rv = channel_receiver.recv();
                match rv {
                    Ok(channel_data) => {
                        let (_topic, topic_type, definition) = channel_data;
                        // setup schema this topic
                        if !schemas.lock().unwrap().contains_key(&topic_type.clone()) {
                            let schema = mcap::Schema {
                                name: topic_type.clone(),
                                encoding: "ros1msg".to_string(),
                                // TODO(lucasw) definition doesn't live long enough
                                data: Cow::from(definition.as_bytes().to_owned()),
                            };
                            let schema = Some(Arc::new(schema.to_owned()));
                            log::debug!("{count} new schema '{topic_type}'\n{schema:?}\n\n");
                            schemas
                                .lock()
                                .unwrap()
                                .insert(topic_type.to_owned().to_string(), schema);
                            // log::warn!("can't get definition {topic} {topic_type}");
                        }
                        count += 1;
                    } // make new schema
                    Err(e) => {
                        log::error!("{e:?}");
                        // break;
                    }
                }
            } // loop on new channels
        });
    }

    // a thread to listen on msg_receiver
    tokio::spawn(async move {
        // TODO(lucasw) put these into struct
        let mut mcap_sequence = 0;
        let mut mcap_name = "".to_string();
        let mut file_handle: Option<fs::File> = None;
        let mut mcap_out: Option<mcap::write::Writer<BufWriter<fs::File>>> = None;

        // these will need to be added to each new mcap
        let mut channel_ids: HashMap<(String, String), u16> = HashMap::new();

        let mut count = 0;
        loop {
            //if file size > some amount then make a new mcap
            if count % 10 == 0 {
                // let file_handle_metadata = file_handle.unwrap_or_else(None).metadata();
                if let Some(ref file_handle) = file_handle {
                    let file_handle_metadata = file_handle.metadata();
                    if let Ok(file_handle_metadata) = file_handle_metadata {
                        let size_mb = file_handle_metadata.len() as f64 / (1024.0 * 1024.0);
                        if count % 2000 == 1 {
                            log::info!("size {} / {size_limit} MB", size_mb as u64);
                        }
                        if size_mb > size_limit as f64 {
                            log::info!("{full_node_name} mcap {mcap_name} size: {size_mb}MB > {size_limit}MB, rolling over");
                            mcap_out.unwrap().finish().unwrap();
                            mcap_out = None;
                            let _ = rename_active(&mcap_name);
                        }
                    }
                }
            }

            if mcap_out.is_none() {
                mcap_name = format!("{}{:05}.mcap.active", prefix, mcap_sequence);
                log::info!("{full_node_name} new mcap {mcap_name}, size limit {size_limit}MB");
                mcap_sequence += 1;
                match fs::File::create(mcap_name.clone()) {
                    Ok(file_handle_raw) => {
                        file_handle = Some(file_handle_raw.try_clone().unwrap());
                        mcap_out =
                            Some(mcap::Writer::new(BufWriter::new(file_handle_raw)).unwrap());
                        fn print_type_of<T>(_: &T) {
                            println!("type: {}", std::any::type_name::<T>());
                        }
                        print_type_of(&mcap_out);

                        channel_ids.clear();
                    }
                    Err(e) => {
                        log::error!("{full_node_name} couldn't create '{mcap_name}' {e:?}");
                        std::process::exit(2);
                    }
                }
            }

            if finish.load(Ordering::SeqCst) {
                log::warn!("{full_node_name} finishing writing to {mcap_name} then exiting");
                mcap_out.unwrap().finish().unwrap();
                let _ = rename_active(&mcap_name);
                std::process::exit(0);
            }

            let rv = msg_receiver.recv_timeout(Duration::from_millis(400));
            match rv {
                Ok(rv) => {
                    let (topic, topic_type, arrival_ns_epoch, sequence, data): (
                        String,
                        String,
                        u64,
                        u32,
                        Vec<u8>,
                    ) = rv;

                    // create channel id if it isn't in hash set for this mcap
                    // have to create channels per mcap file, recreate them after a transition
                    // to a new file
                    let channel_key = (topic.clone(), topic_type.clone());
                    let channel_id = match channel_ids.entry(channel_key) {
                        hash_map::Entry::Occupied(occupied) => *occupied.get(), //.clone(),
                        hash_map::Entry::Vacant(vacant) => {
                            log::debug!("creating channel for {topic} {topic_type}");
                            // the message could be received before the schema has been created
                            // TODO(lucasw) replace with all in one block get-or-fail
                            if !schemas.lock().unwrap().contains_key(&topic_type) {
                                log::warn!("{full_node_name} no schema for '{topic_type}' yet, dropping message");
                                continue;
                            }

                            let schema = schemas.lock().unwrap().get(&topic_type).unwrap().clone();

                            // TODO(lucasw) use Entry
                            let connection_summary = {
                                match connection_summaries.lock().unwrap().get(&topic) {
                                    Some(connection_summary) => {
                                        log_once::debug_once!("{full_node_name} '{topic}' have summary {connection_summary:?}");
                                        connection_summary.clone()
                                    }
                                    None => {
                                        log_once::warn_once!("{full_node_name} '{topic}' no connection summary for '{topic_type}' yet, leaving empty");
                                        BTreeMap::<String, String>::default()
                                    }
                                }
                            };

                            // TODO(lucasw) need to create a BTreeMap and at least store the
                            // md5sum and topic (though that's redundant with the channel topic)
                            // in it to match what is in bags converted to mcap
                            // by `mcap convert`, and if latching and callerid are available
                            // put those in metadata also.

                            let channel = mcap::Channel {
                                topic: topic.clone(),
                                schema,
                                message_encoding: "ros1".to_string(),
                                metadata: connection_summary,
                            };

                            let channel_id =
                                mcap_out.as_mut().unwrap().add_channel(&channel).unwrap();
                            vacant.insert(channel_id);
                            log::debug!(
                                "{full_node_name} {topic} {topic_type} new channel {channel:?}"
                            );
                            channel_id
                        }
                    };

                    mcap_out
                        .as_mut()
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
                    if count % 2000 == 0 {
                        log::debug!(
                            "{full_node_name} {count} written / {} received",
                            num_received_messages.load(Ordering::SeqCst)
                        );
                    }
                    count += 1;
                }
                Err(e) => {
                    // this covers the case where no more message have been received but ctrl-c is
                    // pressed
                    if e == mpsc::RecvTimeoutError::Timeout {
                        if finish.load(Ordering::SeqCst) {
                            log::warn!(
                                "{full_node_name} finishing writing to {mcap_name} then exiting"
                            );
                            mcap_out.unwrap().finish().unwrap();
                            let _ = rename_active(&mcap_name);
                            std::process::exit(0);
                        }
                        continue;
                    }
                    log::error!("{full_node_name} {e:?}");
                    break;
                }
            }
        } // loop
        log::info!("{full_node_name} {count} message written");
    });

    Ok(())
} // mcap_record

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
    // TODO(lucasw) parse := ros arguments with clap?
    let mut _remaps = HashMap::<String, String>::new();
    let (ns, full_node_name, unused_args) = misc::get_params_remaps(&mut params, &mut _remaps);

    let matches = command!()
        .arg(
            arg!(
                -s --size <SIZE> "record a bag of maximum size SIZE MB. (Default: 2048)"
            )
            .default_value("1024")
            .value_parser(clap::value_parser!(u64))
            .required(false)
        )
        .arg(
            arg!(
                -e --regex <REGEX> "match topics using regular expressions"
            )
            .required(false)
        )
        .arg(
            arg!(
                -x --exclude <EXCLUDE_REGEX> "exclude topics matching the follow regular expression\r(subtracts from -a or regex"
            )
            .required(false)
        )
        .arg(
            arg!(
                // TODO(lucasw) can't have dashes in -- name in clap here, so no 'output-prefix'?
                -o --outputprefix <PREFIX> "Prepend PREFIX to beginning of bag name before date stamp"
            )
            .default_value("")
            .required(false)
        )
        .get_matches_from(unused_args);

    let size_limit = *matches.get_one::<u64>("size").unwrap();

    let include_re;
    if let Some(regex_str) = matches.get_one::<String>("regex") {
        include_re = Some(Regex::new(regex_str)?);
        log::info!("include regular expression {include_re:?}");
    } else {
        include_re = None;
    }

    let exclude_re;
    if let Some(regex_str) = matches.get_one::<String>("exclude") {
        exclude_re = Some(Regex::new(regex_str)?);
        log::info!("exclude regular expression {exclude_re:?}");
    } else {
        exclude_re = None;
    }

    let time_str = {
        let now = chrono::prelude::Local::now();
        let offset = now.offset().to_string().replace(':', "_");
        format!("{}_{}_", now.format("%Y_%m_%d_%H_%M_%S"), offset)
    };
    let prefix = matches
        .get_one::<String>("outputprefix")
        .unwrap()
        .to_owned()
        + &time_str;

    let master_client = misc::get_master_client(&full_node_name).await?;
    let nh = {
        let master_uri =
            std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());
        roslibrust::ros1::NodeHandle::new(&master_uri, &full_node_name).await?
    };

    let num_received_messages = Arc::new(AtomicUsize::new(0));

    // create a tokio thread for every topic that appears, and send all received data to a thread
    // that does writing to mcap
    let (channel_sender, channel_receiver) = mpsc::sync_channel(800);
    let (msg_sender, msg_receiver) = mpsc::sync_channel(24000);

    let connection_summaries = Arc::new(Mutex::new(HashMap::new()));

    // signal to stop recording- TODO(lucasw) should be a sync_channel?
    let finish = Arc::new(AtomicBool::new(false));

    // service that can shut down recording
    // TODO(lucasw) later have this start or stop recording, and optionally the node can start
    let _stop_server_handle;
    {
        let finish = finish.clone();
        let full_node_name = full_node_name.clone();
        let server_fn = move |request: std_srvs::SetBoolRequest| -> Result<
            std_srvs::SetBoolResponse,
            Box<dyn std::error::Error + Send + Sync>,
        > {
            let mut text = format!("{full_node_name} set recording to: {request:?}");
            log::info!("{text}");
            if !request.data {
                let warn_text = "- stop request, going to finish and exit".to_string();
                log::warn!("{warn_text}");
                text += &warn_text;
                finish.store(true, Ordering::SeqCst);
            }
            Ok(std_srvs::SetBoolResponse {
                success: true,
                message: text.to_string(),
            })
        };
        _stop_server_handle = nh
            .advertise_service::<std_srvs::SetBool, _>("~/set_recording", server_fn)
            .await?;
    };

    let _ = mcap_record(
        full_node_name,
        prefix.to_string(),
        channel_receiver,
        connection_summaries.clone(),
        msg_receiver,
        finish.clone(),
        num_received_messages.clone(),
        size_limit,
    )
    .await;

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
            if let Some(ref re) = include_re {
                if re.captures(&topic).is_none() {
                    log::debug!("not recording from {topic}");
                    continue;
                }
            }

            if let Some(ref re) = exclude_re {
                if re.captures(&topic).is_some() {
                    log::info!("excluding recording from {topic}");
                    continue;
                }
            }

            // TODO(lucasw) should this be inside the thread?
            log::info!("Subscribing to {topic} {topic_type}");
            let queue_size = 2000;
            let mut subscriber = nh.subscribe_any(&topic, queue_size).await?;

            // maybe should do arc mutex
            let nh = nh.clone();
            let msg_sender = msg_sender.clone();
            let channel_sender = channel_sender.clone();
            let num_received_messages = num_received_messages.clone();
            let connection_summaries = connection_summaries.clone();

            // let num_topics = topics.len();

            let _sub = tokio::spawn(async move {
                // distribute initial subscriber load in time
                // tokio::time::sleep(tokio::time::Duration::from_millis(100 + rand::random::<u64>() % 800)).await;
                // tokio::time::sleep(tokio::time::Duration::from_millis(200 + num_topics as u64 * 2)).await;
                // tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                let mut sequence = 0;
                let mut channel_data = None;
                // TODO(lucasw) seeing some message arrive repeatedly, but only if the
                // publisher starts after this node does?
                while let Some(data) = subscriber.next().await {
                    // TODO(lucasw) u64 will only get us to the year 2554...
                    let arrival_ns_epoch = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64;

                    match data {
                        Ok(data) => {
                            if channel_data.is_none() {
                                log::debug!("get definition");
                                // TODO(lucasw) combine these calls, put the definition in the
                                // summary?
                                let connection_header =
                                    nh.inner.get_connection_header(topic.clone()).await.unwrap();
                                let definition = connection_header.msg_definition;
                                log::debug!("definition: '{definition}'");

                                let connection_summary = {
                                    let mut connection_summary = BTreeMap::new();
                                    // TODO(lucasw) which caller_id is this if there are multiple publishers
                                    // on this topic?
                                    connection_summary.insert(
                                        "caller_id".to_string(),
                                        connection_header.caller_id,
                                    );
                                    connection_summary.insert(
                                        "latching".to_string(),
                                        (connection_header.latching as u8).to_string(),
                                    );
                                    if let Some(md5sum) = connection_header.md5sum {
                                        connection_summary.insert("md5sum".to_string(), md5sum);
                                    }
                                    // TODO(lucasw) not sure why this is stored here, but `mcap convert`
                                    // puts it in mcap channel metadata
                                    if let Some(topic) = connection_header.topic {
                                        connection_summary.insert("topic".to_string(), topic);
                                    }
                                    connection_summary
                                };

                                log::info!(
                                    "'{topic}' connection summary: '{connection_summary:?}'"
                                );
                                connection_summaries
                                    .lock()
                                    .unwrap()
                                    .insert(topic.clone(), connection_summary);

                                let channel_data_inner =
                                    (topic.clone(), topic_type.clone(), definition);
                                let send_rv = channel_sender.send(channel_data_inner.clone());
                                match send_rv {
                                    Ok(()) => {
                                        channel_data = Some(());
                                        // give some time for the receiver to process the
                                        // new channel before sending a message below
                                        tokio::time::sleep(Duration::from_millis(200)).await;
                                    }
                                    Err(e) => {
                                        log::error!(
                                            "{channel_data_inner:?} {arrival_ns_epoch} {e:?}"
                                        );
                                        continue;
                                    }
                                }
                            }

                            let send_rv = msg_sender.send((
                                topic.clone(),
                                topic_type.clone(),
                                arrival_ns_epoch,
                                sequence,
                                data,
                            ));
                            match send_rv {
                                Ok(()) => {}
                                Err(e) => {
                                    // panic will only exit this one receiver thread, but should
                                    // stop everything here
                                    log::error!("couldn't send message {topic} {topic_type} {arrival_ns_epoch} {sequence} {e:?}");
                                    std::process::exit(1);
                                }
                            }
                            sequence += 1;
                            num_received_messages.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(e) => {
                            log::error!("{topic} {topic_type} get message data error {e:?}");
                            continue;
                        }
                    }
                } // get new messages on this topic
            }); // subscriber
        } // loop through current topics
        tokio::time::sleep(Duration::from_millis(250)).await;

        old_topics = cur_topics.clone().to_owned();
    }

    // Ok(())
}
