/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away
use mcap_tools::misc;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // view log messages from roslibrust in stdout
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        // .without_timestamps() // required for running wsl2
        .init()
        .unwrap();

    let mut params = HashMap::<String, String>::new();
    params.insert("_name".to_string(), "mcap_play".to_string());
    let (_ns, full_node_name, _unused_args) = misc::get_params(&mut params);
    let nh = {
        let master_uri =
            std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());
        roslibrust::ros1::NodeHandle::new(&master_uri, &full_node_name).await?
    };

    let args: Vec<String> = std::env::args().collect();
    let mcap_name = &args[1];
    let mapped = misc::map_mcap(mcap_name)?;

    log::info!("opening '{mcap_name}' for playback");

    // let schemas = Arc::new(Mutex::new(HashMap::new()));

    // Setup a task to kill this process when ctrl_c comes in:
    {
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.unwrap();
            log::debug!("closing mcap");
            std::process::exit(0);
        });
    }

    let mut t0 = None;
    let mut t_old = 0.0;

    let mut pubs = HashMap::new();

    let mut count = 0;
    for message_raw in mcap::MessageStream::new(&mapped)? {
        match message_raw {
            Ok(message) => {
                let channel = message.channel;
                {
                    if channel.message_encoding != "ros1" {
                        // TODO(lucasw) warn on first occurrence
                        log::warn!("{}", channel.message_encoding);
                        continue;
                    }
                    if let Some(schema) = &channel.schema {
                        if schema.encoding != "ros1msg" {
                            // TODO(lucasw) warn on first occurrence
                            log::warn!("{}", schema.encoding);
                            continue;
                        }
                        if !pubs.contains_key(&channel.topic) {
                            log::info!("{} {:?}", channel.topic, schema);
                            let publisher = nh
                                .advertise_any(
                                    &channel.topic,
                                    &schema.name,
                                    std::str::from_utf8(&schema.data.clone()).unwrap(),
                                    10,
                                    false,
                                )
                                .await;
                            pubs.insert(channel.topic.clone(), publisher);
                            // channel.message_encoding.clone());
                        }
                    } else {
                        log::warn!("couldn't get schema {:?}", channel.schema);
                        continue;
                    }
                    if pubs.contains_key(&channel.topic) {
                        count += 1;
                        log::debug!("{count} {} publish", channel.topic);
                        let msg_with_header = misc::get_message_data_with_header(message.data);
                        if let Some(Ok(publisher)) = pubs.get(&channel.topic) {
                            let t_cur = message.log_time as f64 / 1e9;
                            // initialize the start time
                            if t0.is_none() {
                                t0 = Some(t_cur);
                                t_old = t_cur;
                                log::info!("start time {t_cur}s");
                            }

                            let dt = t_cur - t_old;
                            if dt > 0.0 {
                                tokio::time::sleep(tokio::time::Duration::from_millis((dt * 1000.0) as u64)).await;
                            }
                            t_old = t_cur;

                            let _ = publisher.publish(&msg_with_header).await;

                            // TODO(lucasw) publish a clock message
                        }
                    } else {
                        log::warn!("no publisher for {}", channel.topic);
                    }
                }
            }
            Err(e) => {
                log::warn!("{:?}", e);
            }
        }
    } // loop through all messages

    log::info!("published {count} messages in mcap");

    Ok(())
}
