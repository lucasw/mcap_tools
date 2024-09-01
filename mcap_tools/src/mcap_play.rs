/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away
use chrono::prelude::DateTime;
use mcap_tools::misc;
use std::collections::HashMap;
use std::time::SystemTime;

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

    // message timestamps of first message published, the message time and the wall clock time
    let mut playback_start_times = None;

    let mut pubs = HashMap::new();

    let mut count = 0;
    // TODO(lucasw) loop optionally
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
                            let msg_time = message.log_time as f64 / 1e9;
                            let wall_time = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_secs_f64();

                            // initialize the start time
                            if playback_start_times.is_none() {
                                playback_start_times = Some((msg_time, wall_time));
                                let d = SystemTime::UNIX_EPOCH
                                    + std::time::Duration::from_secs_f64(msg_time);
                                let utc_datetime = DateTime::<chrono::Utc>::from(d);
                                let local_datetime: DateTime<chrono::prelude::Local> =
                                    DateTime::from(utc_datetime);
                                log::info!("first message time {msg_time:.3}s ({:?}), wall time {wall_time:.3}",
                                    local_datetime,
                                );
                            }

                            if let Some((msg_t0, wall_t0)) = playback_start_times {
                                let msg_elapsed = msg_time - msg_t0;
                                let wall_elapsed = wall_time - wall_t0;
                                // if dt < 0.0 then playback is lagging behind the wallclock
                                // need to play back messages as fast as possible without sleeping
                                // until caught up
                                let dt = msg_elapsed - wall_elapsed;
                                if dt > 0.0 {
                                    tokio::time::sleep(tokio::time::Duration::from_millis(
                                        (dt * 1000.0) as u64,
                                    ))
                                    .await;
                                }

                                let _ = publisher.publish(&msg_with_header).await;

                                // TODO(lucasw) publish a clock message
                            }
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
