/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away
use chrono::prelude::DateTime;
use clap::{arg, command};
use mcap_tools::misc;
use memmap::Mmap;
use regex::Regex;
use roslibrust::ros1::PublisherAny;
use std::collections::HashMap;
use std::time::SystemTime;
use tokio::sync::broadcast;

fn f64_secs_to_local_datetime(secs: f64) -> DateTime<chrono::prelude::Local> {
    let d = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs_f64(secs);
    let utc_datetime = DateTime::<chrono::Utc>::from(d);
    let local_datetime: DateTime<chrono::prelude::Local> = DateTime::from(utc_datetime);
    local_datetime
}

fn use_topic(topic: &str, include_re: &Option<Regex>, exclude_re: &Option<Regex>) -> bool {
    // TODO(lucasw) topic_type matching would be useful as well
    if let Some(ref re) = include_re {
        if re.captures(topic).is_none() {
            log::debug!("not recording from {topic}");
            return false;
        }
    }

    if let Some(ref re) = exclude_re {
        if re.captures(topic).is_some() {
            log::info!("excluding recording from {topic}");
            return false;
        }
    }

    true
}

fn get_wall_time() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

async fn mcap_playback_init(
    nh: &roslibrust::ros1::NodeHandle,
    mcap_name: &str,
    mapped: &Mmap,
    include_re: &Option<Regex>,
    exclude_re: &Option<Regex>,
) -> Result<(HashMap<String, PublisherAny>, f64, f64), anyhow::Error> {
    let mut pubs = HashMap::new();

    // TODO(lucasw) could create every publisher from the summary
    let summary = mcap::read::Summary::read(mapped)?;
    // let summary = summary.ok_or(Err(anyhow::anyhow!("no summary")))?;
    let summary = summary.ok_or(anyhow::anyhow!("{mcap_name} no summary"))?;

    for channel in summary.channels.values() {
        if !use_topic(&channel.topic, include_re, exclude_re) {
            continue;
        }

        if channel.message_encoding != "ros1" {
            // TODO(lucasw) warn on first occurrence
            log::warn!("{mcap_name} {}", channel.message_encoding);
            continue;
        }
        if let Some(schema) = &channel.schema {
            if schema.encoding != "ros1msg" {
                // TODO(lucasw) warn on first occurrence
                log::warn!("{mcap_name} {}", schema.encoding);
                continue;
            }
            if !pubs.contains_key(&channel.topic) {
                log::debug!("{mcap_name} topic {}, schema {:?}", channel.topic, schema);
                let publisher = nh
                    .advertise_any(
                        &channel.topic,
                        &schema.name,
                        std::str::from_utf8(&schema.data.clone()).unwrap(),
                        10,
                        false,
                    )
                    .await?;
                pubs.insert(channel.topic.clone(), publisher);
            }
        } else {
            log::warn!(
                "{mcap_name} couldn't get schema {:?} {}",
                channel.schema,
                channel.topic
            );
            continue;
        }
    } // loop through channels

    let stats = summary
        .stats
        .ok_or(anyhow::anyhow!("{mcap_name} no stats"))?;
    let msg_t0 = stats.message_start_time as f64 / 1e9;
    let msg_t1 = stats.message_end_time as f64 / 1e9;
    log::info!("{mcap_name} start time {msg_t0}, end time {msg_t1}");

    Ok((pubs, msg_t0, msg_t1))
}

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
    let (_ns, full_node_name, unused_args) = misc::get_params(&mut params);
    let master_uri =
        std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());

    // non-ros cli args
    let matches = command!()
        .arg(
            arg!(
                -l --loop <LOOP> "loop playback n time, set to zero to loop without end"
            )
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
                <mcaps> ... "mcaps to load"
            )
            .trailing_var_arg(true)
        )
        .get_matches_from(unused_args);

    // convert Option(&u64) to Option(u64) with the copied()
    let max_loops = matches.get_one::<u64>("loop").copied();
    log::info!("max loop {max_loops:?}");

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

    // Setup a task to kill this process when ctrl_c comes in:
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        log::debug!("closing mcap playback");
        std::process::exit(0);
    });

    // TODO(lucasw) there must be a shorter way to get vector of strings here
    let mcap_names: Vec<_> = matches.get_many::<String>("mcaps").unwrap().collect();
    let mcap_names: Vec<String> = mcap_names.iter().map(|s| (**s).clone()).collect();
    log::info!("{mcap_names:?}");

    let mut msg_t_start = f64::MAX;
    let mut msg_t_end: f64 = 0.0;

    let nh = roslibrust::ros1::NodeHandle::new(&master_uri, &full_node_name).await?;
    let play_data = {
        let mut play_data = Vec::new();

        for (ind, mcap_name) in mcap_names.iter().enumerate() {
            let nh_name = format!(
                "{full_node_name}_{ind}_{}",
                mcap_name.replace(['/', '.'], "_")
            );
            log::info!("{ind} opening '{mcap_name}' for playback with inner node {nh_name}");
            // TODO(lucasw) create multiple node handles per thread didn't work, they didn't publish properly
            // let nh = roslibrust::ros1::NodeHandle::new(&master_uri, &nh_name).await?;
            let mapped = misc::map_mcap(mcap_name)?;

            // initialize the start times and publishers
            let (pubs, msg_t0, msg_t1) =
                mcap_playback_init(&nh, mcap_name, &mapped, &include_re, &exclude_re).await?;

            msg_t_start = msg_t_start.min(msg_t0);
            msg_t_end = msg_t_end.max(msg_t1);

            play_data.push((mcap_name.clone(), mapped, pubs, msg_t0, msg_t1));
        }
        play_data
    };

    {
        let mut handles = Vec::new();
        // TODO(lucasw) also want to be able to pause playback
        let (tx, mut _rx0) = broadcast::channel(4);
        for (mcap_name, mapped, pubs, msg_t0, _msg_t1) in play_data {
            // let msg_t_start = msg_t_start.clone();
            let rx = tx.subscribe();
            let handle = tokio::spawn(async move {
                log::info!(
                    "first message time {msg_t0:.3}s ({:?})",
                    f64_secs_to_local_datetime(msg_t0),
                );

                play(&mcap_name, &mapped, &pubs, msg_t_start, rx, max_loops)
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        let mut loop_count = 0;
        loop {
            let wall_t0 = get_wall_time();
            log::info!("{loop_count} send wall start time to play threads {wall_t0:.3}");
            tx.send(wall_t0)?;

            let duration = msg_t_end - msg_t_start;
            log::info!("{loop_count} waiting for {duration:.3}s for playback to end");
            tokio::time::sleep(tokio::time::Duration::from_secs((duration + 1.0) as u64)).await;

            loop_count += 1;
            if let Some(max_loops) = max_loops {
                if max_loops > 0 && loop_count >= max_loops {
                    break;
                }
            } else {
                break;
            }
        }

        log::info!("waiting for {} tokio handles", handles.len());
        for handle in handles {
            handle.await?;
        }
        log::info!("done with tokio handles");
    }

    Ok(())
}

async fn play(
    mcap_name: &str,
    mapped: &Mmap,
    pubs: &HashMap<String, PublisherAny>,
    msg_t0: f64,
    mut wall_t0_rx: broadcast::Receiver<f64>,
    max_loops: Option<u64>,
) -> Result<(), anyhow::Error> {
    let mut loop_count = 0;
    loop {
        // TODO(lucasw) if a new wall_t0 is received mid playback it should interrupt it
        // and start over if needed, which means we need to try_recv on wall_t0_rx after every message
        // publish and periodically while sleeping between messages
        let wall_t0 = wall_t0_rx.recv().await?;
        log::info!("{loop_count} {mcap_name} wall_t0 received {wall_t0}");

        let mut count = 0;
        for message_raw in mcap::MessageStream::new(mapped)? {
            match message_raw {
                Ok(message) => {
                    let channel = message.channel;

                    // TODO(lucasw) unnecessary with pubs.get below?
                    if !pubs.contains_key(&channel.topic) {
                        continue;
                    }

                    let msg_with_header = misc::get_message_data_with_header(message.data);
                    if let Some(publisher) = pubs.get(&channel.topic) {
                        let msg_time = message.log_time as f64 / 1e9;
                        let wall_time = get_wall_time();

                        let msg_elapsed = msg_time - msg_t0;
                        let wall_elapsed = wall_time - wall_t0;
                        // if dt < 0.0 then playback is lagging behind the wallclock
                        // need to play back messages as fast as possible without sleeping
                        // until caught up
                        let dt = msg_elapsed - wall_elapsed;
                        // TODO(lucasw) if dt is too negative then skip publishing until caught up?
                        if dt > 0.0 {
                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                (dt * 1000.0) as u64,
                            ))
                            .await;
                        }

                        let _ = publisher.publish(&msg_with_header).await;

                        count += 1;
                        log::debug!("{loop_count} {mcap_name} {count} {} publish", channel.topic);
                        // TODO(lucasw) publish a clock message
                    }
                }
                Err(e) => {
                    log::warn!("{mcap_name} {:?}", e);
                }
            }
        } // loop through all messages

        log::info!("{loop_count} {mcap_name} published {count} messages");
        loop_count += 1;
        if let Some(max_loops) = max_loops {
            if max_loops > 0 && loop_count >= max_loops {
                break;
            }
        } else {
            break;
        }
    }

    Ok(())
}
