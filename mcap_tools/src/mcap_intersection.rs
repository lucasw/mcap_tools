/// Copyright 2024 Lucas Walter
/// given two mcap files, find the messages that are unique to each and output them into new mcaps
use mcap_tools::misc;
use std::collections::HashSet;


/// return a HashSet of tuples of channel id and timestamp  // topic_name, topic_type, and timestamp
/// TODO(lucasw) multiple message with the same timestamp in the same channel will be ignored, later add a hash of
/// message content to the tuple
// async fn mcap_to_hashset(String: mcap_name) -> Result<HashSet<(String, String, u64)>, anyhow::Error> {
async fn mcap_to_hashset(mcap_name: &str) -> Result<HashSet<(u16, u64)>, anyhow::Error> {
    let mapped = misc::map_mcap(mcap_name)?;

    let mut msg_hash = HashSet::new();

    let mut count = 0;
    for message_raw in mcap::MessageStream::new(&mapped)? {
        match message_raw {
            Ok(message) => {
                let channel = message.channel;
                // let msg_tuple = (std::sync::Arc::get_mut(&channel.clone()?).id, message.log_time);
                msg_hash.insert(msg_tuple);
                {
                    /*
                    if channel.message_encoding != "ros1" {
                        // TODO(lucasw) warn on first occurrence
                        log::warn!("{}", channel.message_encoding);
                        continue;
                    }
                    */
                    if let Some(schema) = &channel.schema {
                        // let msg_tuple = (channel.topic.clone(), schema.name, message.log_time);
                    } else {
                        log::warn!("couldn't get schema {:?}", channel.schema);
                        continue;
                    }
                }
            }
            Err(e) => {
                log::warn!("{:?}", e);
            }
        }
    } // loop through all messages

    log::info!("published {count} messages in mcap");

    msg_hash
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // view log messages from roslibrust in stdout
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
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

    log::info!("{mcap_name0} {} messages", msg_hash0.len());
    log::info!("{mcap_name1} {} messages", msg_hash1.len());

    // let common_msgs_iter: HashSet<(u16, u64)> = msg_hash0.intersection(&msg_hash1);  // .collect();
    let common_msgs_iter = msg_hash0.intersection(&msg_hash1);  // .collect();
    let common_msgs = HashSet::new();
    for common_msg in common_msgs_iter {
        common_msgs.insert(common_msg);
    }
    log::info!("{} common messages", common_msgs.len());

    Ok(())
}
