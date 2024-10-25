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

use std::time::Duration;

use futures::{future::FutureExt, select, StreamExt};
use futures_timer::Delay;

use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers},
    terminal::{disable_raw_mode, enable_raw_mode},
};

use crate::misc::{rosgraph_msgs, tf2_msgs};

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
            log::debug!("not playing back {topic}");
            return false;
        }
    }

    if let Some(ref re) = exclude_re {
        if re.captures(topic).is_some() {
            log::info!("excluding playing back {topic}");
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
    remaps: &HashMap<String, String>,
) -> Result<(HashMap<String, PublisherAny>, tf2_msgs::TFMessage, f64, f64), anyhow::Error> {
    // TODO(lucasw) could create every publisher from the summary
    let summary = mcap::read::Summary::read(mapped)?;
    // let summary = summary.ok_or(Err(anyhow::anyhow!("no summary")))?;
    let summary = summary.ok_or(anyhow::anyhow!("{mcap_name} no summary"))?;

    let stats = summary
        .stats
        .ok_or(anyhow::anyhow!("{mcap_name} no stats"))?;

    if stats.message_count == 0 {
        return Err(anyhow::anyhow!("{mcap_name} has no messages"));
    }

    let msg_t0 = stats.message_start_time as f64 / 1e9;
    let msg_t1 = stats.message_end_time as f64 / 1e9;
    log::info!(
        "{mcap_name} start time {msg_t0:.1}, end time {msg_t1:.1}: duration {:.1}s",
        msg_t1 - msg_t0
    );

    let mut tf_static_aggregated = tf2_msgs::TFMessage::default();
    let mut pubs = HashMap::new();

    for channel in summary.channels.values() {
        if !use_topic(&channel.topic, include_re, exclude_re) {
            continue;
        }

        if channel.message_encoding != "ros1" {
            // TODO(lucasw) warn on first occurrence
            log::warn!("{mcap_name} {}", channel.message_encoding);
            continue;
        }

        // ugly special case to handle tf static aggregation
        // TODO(lucasw) this maybe be a little funny if the mcap contains
        // multiple static transforms with the same parent and child, could
        // scan through them and only take the most recent, or build a hash map here
        if channel.topic == "/tf_static" {
            // && channel.schema.unwrap().name == "tf2_msgs/TFMessage" {
            // TODO(lucasw) how to get through an mcap as quickly as possible to get a single
            // topic?  The easiest thing would be to save tf_static to a separate mcap in the
            // first place, more advanced would be for mcap_record to put all the statics
            // in a separate chunk but then 'mcap convert' wouldn't do that.
            for message in (mcap::MessageStream::new(mapped)?).flatten() {
                if message.channel.topic != "/tf_static" {
                    continue;
                }
                let msg_with_header = misc::get_message_data_with_header(message.data);
                match serde_rosmsg::from_slice::<tf2_msgs::TFMessage>(&msg_with_header) {
                    Ok(tf_msg) => {
                        log::info!(
                            "{mcap_name} adding {} transforms to tf_static, total {}\r",
                            tf_msg.transforms.len(),
                            tf_msg.transforms.len() + tf_static_aggregated.transforms.len(),
                        );
                        for transform in tf_msg.transforms {
                            tf_static_aggregated.transforms.push(transform);
                        }
                    }
                    Err(err) => {
                        log::error!("{mcap_name} {err:?}");
                    }
                }
            }
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
                let latching = {
                    match channel.metadata.get("latching") {
                        Some(latching) => latching == "1",
                        None => false,
                    }
                };

                // remap a topic if it's in the remaps hashmap
                let topic = {
                    match remaps.get(&channel.topic) {
                        Some(topic) => topic.to_string(),
                        None => channel.topic.clone(),
                    }
                };
                let publisher = nh
                    .advertise_any(
                        &topic,
                        &schema.name,
                        std::str::from_utf8(&schema.data.clone()).unwrap(),
                        10,
                        latching,
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

    Ok((pubs, tf_static_aggregated, msg_t0, msg_t1))
}

type PlayArgs = (
    Option<u64>,
    bool,
    (Option<Regex>, Option<Regex>),
    f64,
    Vec<String>,
);

fn get_non_ros_cli_args(unused_args: Vec<String>) -> Result<PlayArgs, anyhow::Error> {
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
                -c --clock <CLOCK> "publish the clock time, most useful in combination with rosparam set /use_sim_time true"
            )
            .action(clap::ArgAction::SetTrue)
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
                -x --exclude <EXCLUDE_REGEX> "exclude topics matching the follow regular expression (subtracts from include regex)"
            )
            .required(false)
        )
        .arg(
            arg!(
                -s --start <SEC> "start SEC seconds into the mcap files"
            )
            .value_parser(clap::value_parser!(f64))
            .default_value("0.0")
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

    // TODO(lucasw) error if there is a /clock message in an mcap
    let publish_clock = matches.get_one::<bool>("clock").copied().unwrap();
    log::info!("publishing clock");

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

    let start_secs_offset = matches.get_one::<f64>("start").copied().unwrap();
    log::info!("start_secs_offset {start_secs_offset:?}");

    // TODO(lucasw) there must be a shorter way to get vector of strings here
    let mcap_names: Vec<_> = matches.get_many::<String>("mcaps").unwrap().collect();
    let mcap_names: Vec<String> = mcap_names.iter().map(|s| (**s).clone()).collect();
    log::info!("{mcap_names:?}");

    Ok((
        max_loops,
        publish_clock,
        (include_re, exclude_re),
        start_secs_offset,
        mcap_names,
    ))
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // view log messages from roslibrust in stdout
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        // .without_timestamps() // required for running wsl2
        .init()
        .unwrap();

    let (full_node_name, unused_args, remaps) = {
        let mut params = HashMap::<String, String>::new();
        params.insert("_name".to_string(), "mcap_play".to_string());
        let mut remaps = HashMap::<String, String>::new();
        let (_ns, full_node_name, unused_args) = misc::get_params_remaps(&mut params, &mut remaps);
        (full_node_name, unused_args, remaps)
    };

    let master_uri =
        std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());

    let (max_loops, publish_clock, (include_re, exclude_re), start_secs_offset, mcap_names) =
        get_non_ros_cli_args(unused_args)?;

    // Setup a task to kill this process when ctrl_c comes in:
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        log::debug!("closing mcap playback");
        std::process::exit(0);
    });

    let nh = roslibrust::ros1::NodeHandle::new(&master_uri, &full_node_name).await?;
    let (msg_t_start, tf_static_aggregated, msg_t_end, play_data) = {
        let mut msg_t_start = f64::MAX;
        let mut msg_t_end: f64 = 0.0;
        let mut play_data = Vec::new();
        let mut tf_static_aggregated = tf2_msgs::TFMessage::default();

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
            let rv = mcap_playback_init(&nh, mcap_name, &mapped, &include_re, &exclude_re, &remaps)
                .await;

            match rv {
                Ok((pubs, tf_statics, msg_t0, msg_t1)) => {
                    msg_t_start = msg_t_start.min(msg_t0);
                    msg_t_end = msg_t_end.max(msg_t1);
                    play_data.push((mcap_name.clone(), mapped, pubs, msg_t0, msg_t1));

                    for transform in tf_statics.transforms {
                        tf_static_aggregated.transforms.push(transform);
                    }
                }
                Err(err) => {
                    log::error!("{err:?}");
                    continue;
                }
            }
        }

        msg_t_start += start_secs_offset;
        log::info!(
            ">>> start time {msg_t_start:.2}, end time {msg_t_end:.2}: duration {:.1}s <<<",
            msg_t_end - msg_t_start
        );

        if play_data.is_empty() {
            panic!("no mcaps successfully loaded");
        }
        (msg_t_start, tf_static_aggregated, msg_t_end, play_data)
    };

    // publish all the tf statics from all the mcaps together in one latched message
    // unless we're excluding tf_static
    let _static_publisher = {
        if !tf_static_aggregated.transforms.is_empty() {
            for (ind, transform) in tf_static_aggregated.transforms.iter().enumerate() {
                log::debug!(
                    "{ind} static {} -> {}",
                    transform.header.frame_id,
                    transform.child_frame_id
                );
            }

            let topic = {
                match remaps.get("/tf_static") {
                    Some(topic) => topic.to_string(),
                    None => "/tf_static".to_string(),
                }
            };
            let latching = true;
            let static_publisher = nh
                .advertise::<tf2_msgs::TFMessage>(&topic, 10, latching)
                .await?;
            static_publisher.publish(&tf_static_aggregated).await?;
            Some(static_publisher)
        } else {
            None
        }
    };

    let mut handles = Vec::new();
    {
        let clock_publisher = {
            let latching = false;
            let queue_size = 10;
            match publish_clock {
                true => Some(
                    nh.advertise::<rosgraph_msgs::Clock>("/clock", queue_size, latching)
                        .await
                        .unwrap(),
                ),
                false => None,
            }
        };

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        enable_raw_mode()?;

        // TODO(lucasw) also want to be able to pause playback
        let (clock_tx, mut _clock_rx0) = broadcast::channel(20);
        for (mcap_name, mapped, pubs, msg_t0, _msg_t1) in play_data {
            // let msg_t_start = msg_t_start.clone();
            let clock_rx = clock_tx.subscribe();
            let handle = tokio::spawn(async move {
                // have to add the carriage return in raw mode (which allows keypresses
                // to trigger events without pressing enter)
                log::info!(
                    "{mcap_name} first message time {msg_t0:.3}s ({:?})\r",
                    f64_secs_to_local_datetime(msg_t0),
                );

                let _rv = play_one_mcap(&mcap_name, &mapped, &pubs, clock_rx).await;
            });
            handles.push(handle);
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        // a flag to exit playback early
        let mut finish_playback = false;
        let mut loop_count = 0;
        let mut paused = false;
        loop {
            // loop over all mcaps
            // how much time has elapsed of playback time, excluding any time spent paused
            let mut playback_elapsed = 0.0;

            let mut wall_t0 = get_wall_time();
            let duration = msg_t_end - msg_t_start;

            // look for key presses during this playback iteration
            let mut term_reader = EventStream::new();

            // TODO(lucasw) put this inside a function
            log::info!("starting clock\r");
            loop {
                // advance time within one playback cycle
                let wall_t = get_wall_time();
                if !paused {
                    playback_elapsed = wall_t - wall_t0;
                    if playback_elapsed > duration + 1.0 {
                        break;
                    }

                    let clock_seconds = msg_t_start + playback_elapsed;
                    clock_tx.send((clock_seconds, msg_t_start))?;

                    if let Some(ref clock_publisher) = clock_publisher {
                        let clock_msg = rosgraph_msgs::Clock {
                            clock: roslibrust_codegen::Time {
                                secs: clock_seconds as u32,
                                nsecs: ((clock_seconds % 1.0) * 1e9_f64) as u32,
                            },
                        };
                        clock_publisher.publish(&clock_msg).await?;
                    }
                }

                let mut delay = Delay::new(Duration::from_millis(10)).fuse();
                let mut event = term_reader.next().fuse();

                select! {
                    _ = delay => {},
                    maybe_event = event => {
                        match maybe_event {
                            Some(Ok(event)) => {

                                // spacebar
                                if event == Event::Key(KeyCode::Char(' ').into()) {
                                    // broadcast a new msg_t0, wall_t0, and pause status
                                    // to the playing threads, and make it so no messages are lost
                                    // in the transition
                                    paused = !paused;
                                    if !paused {
                                        // update wall_t0 to the value it would be had the pause
                                        // never happened, future dated by a little
                                        // to delay playback
                                        // TODO(lucasw) maybe that is confusing and a better pause
                                        // system is needed that doesn't require a fictional
                                        // wall_t0
                                        wall_t0 = wall_t - playback_elapsed + 1.0;
                                    }
                                    log::info!("paused: {paused}\r");
                                }

                                if event == Event::Key(KeyEvent::new(
                                    KeyCode::Char('c'),
                                    KeyModifiers::CONTROL,
                                    )
                                ) {
                                    log::info!("ctrl-c, quitting\r");
                                    finish_playback = true;
                                    break;
                                }
                                if event == Event::Key(KeyCode::Esc.into()) {
                                    log::info!("escape key, quitting\r");
                                    finish_playback = true;
                                    break;
                                }
                            }
                            Some(Err(e)) => println!("Error: {:?}\r", e),
                            None => break,
                        }
                    }
                };
            }

            if finish_playback {
                // TODO(lucasw) need to send a finished message to player threads
                break;
            }

            loop_count += 1;
            if let Some(max_loops) = max_loops {
                if max_loops > 0 && loop_count >= max_loops {
                    break;
                }
            } else {
                break;
            }
        }

        /*
        if !finish_playback {
            log::info!("waiting for {} tokio handles\r", handles.len());
            for handle in handles {
                handle.await?;
            }
            log::info!("done with tokio handles\r");
        }
        */
        disable_raw_mode()?;
    }

    // clock_tx going out of scope should bring down every play_one_mcap task
    log::info!("awaiting {} handles", handles.len());
    for handle in handles {
        let _ = handle.await;
    }
    log::info!("done");

    Ok(())
}

async fn wait_for_playback_time(
    clock_rx: &mut broadcast::Receiver<(f64, f64)>,
    msg_time: Option<f64>,
) -> Result<(f64, f64), broadcast::error::RecvError> {
    let mut lagged_count = 0;
    let mut last_clock_t = 0.0;
    loop {
        // if the clock_tx goes out of scope this will return an error
        let rv = clock_rx.recv().await;
        match rv {
            // get the current playback time and start of playback time
            Ok((clock_t, msg_t0)) => {
                if lagged_count > 0 {
                    log::warn!("done being lagged {lagged_count}\r");
                    lagged_count = 0;
                }
                if clock_t < last_clock_t {
                    // TODO(lucasw) enum within Ok, or return an error?
                    log::warn!(
                        "TODO(lucasw) time went backwards, need to go back to start of mcap\r"
                    );
                }
                last_clock_t = clock_t;

                match msg_time {
                    None => {
                        // if no time is provided, just wait for the start time
                        // give the playback time to be able to skip per msg_t0 messages
                        let margin_s = 2.0;
                        if clock_t > msg_t0 - margin_s {
                            return Ok((clock_t, msg_t0));
                        }
                    }
                    Some(msg_time) => {
                        if clock_t > msg_time {
                            return Ok((clock_t, msg_t0));
                        }
                    }
                }
            }
            Err(broadcast::error::RecvError::Lagged(num)) => {
                if lagged_count == 0 {
                    log::warn!("lagged, need to catch up {num}\r");
                }
                lagged_count += 1;
            }
            Err(broadcast::error::RecvError::Closed) => {
                return Err(broadcast::error::RecvError::Closed);
            }
        }
    }
}

async fn play_one_mcap(
    mcap_name: &str,
    mapped: &Mmap,
    pubs: &HashMap<String, PublisherAny>,
    mut clock_rx: broadcast::Receiver<(f64, f64)>,
    // TODO(lucasw) instead of anyhow map all the actual errors into new error, first one is
    // RecvError::Closed
) -> Result<(u64, u64), anyhow::Error> {
    // TODO(lucasw) could return number of messages published as well as loop count
    let mut loop_count = 0;
    let mut count = 0;
    loop {
        // don't want to start getting messages out of mcap until it's the right time, I think that
        // may cause memory to be used unnecessarily (which will be a big issue if playing back 20
        // mcaps that go in sequence)
        // TODO(lucasw) maybe shouldn't even have the mapped handle yet, wait to open
        let (mut clock_t, mut msg_t0) = wait_for_playback_time(&mut clock_rx, None).await?;
        log::info!(
            "{loop_count} {mcap_name}, clock {clock_t:.1}, msg t start {msg_t0:.1}, elapsed {:.1}\r", clock_t - msg_t0
        );

        let mut skipping = 0;
        for message_raw in mcap::MessageStream::new(mapped)? {
            match message_raw {
                Ok(message) => {
                    let topic = &message.channel.topic;

                    let rv = pubs.get(topic);
                    match rv {
                        None => {
                            // probably excluded this topic
                            continue;
                        }
                        Some(publisher) => {
                            // All the messages are extracted in log_time order (or publish_time?
                            // They're the same here)
                            let msg_with_header = misc::get_message_data_with_header(message.data);
                            let msg_time = message.log_time as f64 / 1e9;
                            if msg_time < msg_t0 {
                                continue;
                            }

                            let elapsed = clock_t - msg_t0;
                            let threshold_s = 1.0;
                            // expect this to be positive, with the first message in the future
                            let delta_s = msg_time - clock_t;
                            // but if it's negative we're lagging some, a little is okay though
                            if -delta_s > threshold_s {
                                if skipping == 0 {
                                    log::warn!("{loop_count} {mcap_name} lagging {delta_s:.2}s, elapsed {elapsed:0.1}, skipping playback of this message {topic}, {count} already published\r");
                                }
                                skipping += 1;
                                continue;
                            } else if delta_s > 0.0 {
                                // clock_t < msg_time, normal case
                                // don't want mcap looping to return an error here because this
                                // convenient exits this entire function,
                                // TODO(lucasw) though could make a single
                                // pass through an mcap a dedicated function, then handle errors
                                // differently in the looping caller
                                let (new_clock_t, new_msg_t0) =
                                    wait_for_playback_time(&mut clock_rx, Some(msg_time)).await?;
                                clock_t = new_clock_t;
                                // TODO(lucasw) there's no mechanism to alter msg_t0 yet, but this
                                // supports it if one is added
                                msg_t0 = new_msg_t0;
                            }

                            // having made it here, the message is in the time window to be able to publish
                            skipping = 0;

                            let _ = publisher.publish(&msg_with_header).await;

                            count += 1;
                            if count % 1000 == 0 {
                                log::debug!("{loop_count} {mcap_name} {count} {topic} publish\r");
                            }
                        }
                    }
                }
                Err(e) => {
                    log::warn!("{loop_count} {mcap_name} {:?}\r", e);
                }
            }
        } // loop through all messages

        log::info!("{loop_count} {mcap_name} published {count} messages\r");
        loop_count += 1;
    }

    // There are multiple paths above to exit out, all via '?', but would rather they made it here
    // instead if it's the normal process of RecvError::Closed
    // Ok((loop_count, count))
}
