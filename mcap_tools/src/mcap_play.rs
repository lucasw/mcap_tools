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
) -> Result<(HashMap<String, PublisherAny>, f64), anyhow::Error> {
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

    let msg_t0 = summary
        .stats
        .ok_or(anyhow::anyhow!("{mcap_name} no stats"))?
        .message_start_time as f64
        / 1e9;
    log::info!("{mcap_name} start time {}", msg_t0);

    Ok((pubs, msg_t0))
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

    let nh = roslibrust::ros1::NodeHandle::new(&master_uri, &full_node_name).await?;
    let play_data = {
        let mut play_data = Vec::new();

        for (ind, mcap_name) in mcap_names.iter().enumerate() {
            let nh_name = format!(
                "{full_node_name}_{ind}_{}",
                mcap_name.replace(['/', '.'], "_")
            );
            log::info!("{ind} opening '{mcap_name}' for playback with inner node {nh_name}");
            // let nh = roslibrust::ros1::NodeHandle::new(&master_uri, &nh_name).await?;
            let mapped = misc::map_mcap(mcap_name)?;

            // initialize the start times and publishers
            let (pubs, msg_t0) =
                mcap_playback_init(&nh, mcap_name, &mapped, &include_re, &exclude_re).await?;

            play_data.push((mcap_name.clone(), mapped, /* nh,*/ pubs, msg_t0));
        }
        play_data
    };

    {
        let mut handles = Vec::new();
        let wall_t0 = get_wall_time();
        for (mcap_name, mapped, /*_nh, */ pubs, msg_t0) in play_data {
            let handle = tokio::spawn(async move {
                log::info!(
                    "first message time {msg_t0:.3}s ({:?}), wall time {wall_t0:.3}",
                    f64_secs_to_local_datetime(msg_t0),
                );

                // TODO(lucasw) loop optionally
                play(&mcap_name, &mapped, &pubs, msg_t0, wall_t0)
                    .await
                    .unwrap();
            });
            handles.push(handle);
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
    wall_t0: f64,
) -> Result<(), anyhow::Error> {
    // message timestamps of first message published, the message time and the wall clock time

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
                    if dt > 0.0 {
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            (dt * 1000.0) as u64,
                        ))
                        .await;
                    }

                    let _ = publisher.publish(&msg_with_header).await;

                    count += 1;
                    log::debug!("{mcap_name} {count} {} publish", channel.topic);
                    // TODO(lucasw) publish a clock message
                }
            }
            Err(e) => {
                log::warn!("{mcap_name} {:?}", e);
            }
        }
    } // loop through all messages

    log::info!("{mcap_name} published {count} messages");

    Ok(())
}
