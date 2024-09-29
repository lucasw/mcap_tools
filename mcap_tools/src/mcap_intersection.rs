/// Copyright 2024 Lucas Walter
/// given two mcap files, find the messages that are unique to each and output them into new mcaps
use mcap_tools::misc;
use std::collections::HashSet;

/// return a HashSet of tuples of topic_name, topic_type, and timestamp
/// TODO(lucasw) multiple message with the same timestamp in the same channel will be ignored, later add a hash of
/// message content to the tuple
/// TODO(lucasw) can't rosbag play then rosbag record and get the same timestamps, even with
/// --clock (should fix that with mcap_play, if a sim clock parameter is set set the publish time
/// to the stored publish time and make sure the mcap_record uses the same value
async fn mcap_to_hashset(mcap_name: &str) -> Result<HashSet<(String, String, u64)>, anyhow::Error> {
    let mapped = misc::map_mcap(mcap_name)?;

    let mut msg_hash = HashSet::new();

    let mut count = 0;
    for message_raw in mcap::MessageStream::new(&mapped)? {
        count += 1;
        match message_raw {
            Ok(message) => {
                let channel = message.channel;
                {
                    /*
                    if channel.message_encoding != "ros1" {
                        // TODO(lucasw) warn on first occurrence
                        log::warn!("{}", channel.message_encoding);
                        continue;
                    }
                    */
                    if let Some(schema) = &channel.schema {
                        // TODO(lucasw) remove most of the timestamp precision to make timestamps
                        // more likely to line up
                        // let timestamp = message.publish_time / 100_000_000;
                        let timestamp = message.publish_time;
                        let msg_tuple = (channel.topic.clone(), schema.name.clone(), timestamp);
                        if count % 10000 == 0 {
                            log::debug!("{mcap_name} {} {msg_tuple:?}", message.sequence);
                        }
                        msg_hash.insert(msg_tuple);
                    } else {
                        log::warn!("{mcap_name} couldn't get schema in {:?}", channel.schema);
                        continue;
                    }
                }
            }
            Err(e) => {
                log::warn!("{mcap_name} error reading message: {:?}", e);
            }
        }
    } // loop through all messages

    log::debug!("{mcap_name} processed {count} messages");

    Ok(msg_hash)
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // view log messages from roslibrust in stdout
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        // .without_timestamps() // required for running wsl2
        .init()
        .unwrap();

    // Setup a task to kill this process when ctrl_c comes in:
    {
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            log::debug!("closing");
            std::process::exit(0);
        });
    }

    let args: Vec<String> = std::env::args().collect();
    let mcap_name0 = &args[1];
    let mcap_name1 = &args[2];
    // TODO(lucasw) tokio spawn these so they can run concurrently
    let msg_hash0 = mcap_to_hashset(mcap_name0).await?;
    let msg_hash1 = mcap_to_hashset(mcap_name1).await?;

    /*
    let mut count = 0;
    for msg in msg_hash1.iter() {
        log::info!("{msg:?} {:?}", msg_hash0.contains(msg));
        count += 1;
        if count >= 10 {
            break;
        }
    }
    count = 0;
    log::info!("");
    for msg in msg_hash0.iter() {
        log::info!("{msg:?} {:?}", msg_hash1.contains(msg));
        count += 1;
        if count >= 10 {
            break;
        }
    }
    */

    let common_msgs_iter = msg_hash0.intersection(&msg_hash1);
    let mut common_msgs = HashSet::new();
    common_msgs.extend(common_msgs_iter.clone());
    // let common_msgs = HashSet::from_iter(common_msgs_iter.cloned());
    log::info!("{} common messages", common_msgs.len());

    let msg_hash0_unique: HashSet<(String, String, u64)> = msg_hash0
        .iter()
        .filter(|m| !common_msgs.contains(m))
        .cloned()
        .collect();
    let msg_hash1_unique: HashSet<(String, String, u64)> = msg_hash1
        .iter()
        .filter(|m| !common_msgs.contains(m))
        .cloned()
        .collect();

    log::info!(
        "{mcap_name0} {} messages, {} unique",
        msg_hash0.len(),
        msg_hash0_unique.len()
    );
    log::info!(
        "{mcap_name1} {} messages, {} unique",
        msg_hash1.len(),
        msg_hash1_unique.len()
    );

    Ok(())
}
