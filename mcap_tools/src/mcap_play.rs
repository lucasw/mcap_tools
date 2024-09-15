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
        "{mcap_name} start time {msg_t0}, end time {msg_t1}: duration {:.1}s",
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
            for message_raw in mcap::MessageStream::new(mapped)? {
                if let Ok(message) = message_raw {
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
            }
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

fn get_non_ros_cli_args(
    unused_args: Vec<String>,
) -> Result<
    (
        Option<u64>,
        bool,
        Option<Regex>,
        Option<Regex>,
        f64,
        Vec<String>,
    ),
    anyhow::Error,
> {
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
        include_re,
        exclude_re,
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

    let (max_loops, publish_clock, include_re, exclude_re, start_secs_offset, mcap_names) =
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

    // publish all teh tf statics from all the mcaps together in one latched message
    // unless we're excluding tf_static
    if !tf_static_aggregated.transforms.is_empty() {
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
        // Does having this go out of scope make it not work?
        // No apparently not
        static_publisher.publish(&tf_static_aggregated).await?;
    }

    {
        enable_raw_mode()?;

        let mut handles = Vec::new();
        // TODO(lucasw) also want to be able to pause playback
        let (tx, mut _rx0) = broadcast::channel(4);
        for (mcap_name, mapped, pubs, msg_t0, _msg_t1) in play_data {
            // let msg_t_start = msg_t_start.clone();
            let rx = tx.subscribe();
            let handle = tokio::spawn(async move {
                // have to add the carriage return in raw mode (which allows keypresses
                // to trigger events without pressing enter)
                log::info!(
                    "{mcap_name} first message time {msg_t0:.3}s ({:?})\r",
                    f64_secs_to_local_datetime(msg_t0),
                );

                play(&mcap_name, &mapped, &pubs, rx).await.unwrap();
            });
            handles.push(handle);
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

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

        // a flag to exit playback early
        let mut finish_playback = false;
        let mut loop_count = 0;
        let mut paused = false;
        loop {
            // how much time has elapsed of playback time, excluding any time spent paused
            let mut playback_elapsed = 0.0;

            let mut wall_t0 = get_wall_time();
            let duration = msg_t_end - msg_t_start;
            log::info!("{loop_count} ##### send wall start time to play threads {wall_t0:.3} for {duration:.1}s\r");
            tx.send((wall_t0, msg_t_start, paused, loop_count))?;

            // look for key presses during this playback iteration
            let mut term_reader = EventStream::new();

            loop {
                let wall_t = get_wall_time();
                if !paused {
                    playback_elapsed = wall_t - wall_t0;
                    if playback_elapsed > duration + 1.0 {
                        break;
                    }

                    if let Some(ref clock_publisher) = clock_publisher {
                        let clock_seconds = msg_t_start + playback_elapsed;
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
                                    tx.send((wall_t0, msg_t_start, paused, loop_count))?;
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

            // signal all the threads they need to restart their loops
            tx.send((wall_t0, msg_t_start, paused, loop_count))?;
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
    }

    disable_raw_mode()?;

    Ok(())
}

async fn play(
    mcap_name: &str,
    mapped: &Mmap,
    pubs: &HashMap<String, PublisherAny>,
    mut rx: broadcast::Receiver<(f64, f64, bool, u64)>,
) -> Result<(), anyhow::Error> {
    let mut loop_count = 0;
    loop {
        // TODO(lucasw) if a new wall_t0 is received mid playback it should interrupt it
        // and start over if needed, which means we need to try_recv on wall_t0_rx after every message
        // publish and periodically while sleeping between messages
        let mut wall_t0;
        let mut msg_t0;
        let mut paused;
        let mut main_loop_count;

        // wait for loop start
        // TODO(lucasw) or only do this optionally
        loop {
            (wall_t0, msg_t0, paused, main_loop_count) = rx.recv().await?;
            match main_loop_count {
                _ if main_loop_count < loop_count => {
                    log::info!("{loop_count} wait for main loop count to catch up to this one > {main_loop_count}\r");
                    continue;
                }
                _ if main_loop_count == loop_count => {
                    log::info!("{loop_count} main loop count and this one in sync, start playing back messages\r");
                    break;
                }
                _ => {
                    log::warn!("{loop_count} somehow have skipped entire loop/s, catching up -> {main_loop_count}\r");
                    loop_count = main_loop_count;
                    break;
                }
            }
        }
        log::info!(
            "{loop_count} {mcap_name} ## start {wall_t0:.3} ({:.3}), {msg_t0:.3}, paused {paused}\r",
            get_wall_time() - wall_t0
        );

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
                        if msg_time < msg_t0 {
                            continue;
                        }

                        // get a new pause state possibly and wait for right time to publish this
                        // message
                        loop {
                            let wall_time = get_wall_time();

                            let rv = rx.try_recv();
                            match rv {
                                Err(_err) => {
                                    // There's no new message, continue on
                                    // TODO(lucasw) handle TryRecvError::Lagged
                                }
                                Ok((new_wall_t0, new_msg_t0, new_paused, new_loop_count)) => {
                                    wall_t0 = new_wall_t0;
                                    msg_t0 = new_msg_t0;
                                    paused = new_paused;
                                    main_loop_count = new_loop_count;
                                    if main_loop_count != loop_count {
                                        log::info!("{loop_count} not publishing this message, have a new loop count {main_loop_count}");
                                        break;
                                    }
                                    log::info!("{loop_count} {mcap_name} {wall_t0:.3} ({:.3}), {msg_t0:.3}, ({:.3}), paused {paused}\r",
                                        wall_time - wall_t0,
                                        msg_time - msg_t0);
                                }
                            }

                            if paused {
                                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                                continue;
                            }

                            // TODO(lucasw) check again if msg_time < msg_t0, because msg_t0
                            // may be different?

                            let msg_elapsed = msg_time - msg_t0;
                            let wall_elapsed = wall_time - wall_t0;
                            let dt = msg_elapsed - wall_elapsed;

                            // if dt < 0.0 then playback is lagging behind the wallclock
                            // need to play back messages as fast as possible without sleeping
                            // until caught up
                            // TODO(lucasw) if dt is too negative then skip publishing until caught up?
                            if dt <= 50.0 {
                                if dt > 0.0 {
                                    // sleep remainder then publish
                                    tokio::time::sleep(tokio::time::Duration::from_millis(
                                        (dt * 1000.0) as u64,
                                    ))
                                    .await;
                                }
                                break;
                            }

                            // need to loop if it isn't time to publish yet and continue to look
                            // for changes in pause state
                            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                        } // check for new received state repeatedly

                        if main_loop_count != loop_count {
                            log::info!("{loop_count} ending this loop, have a new loop count {main_loop_count}");
                            break;
                        }

                        let _ = publisher.publish(&msg_with_header).await;

                        count += 1;
                        log::debug!(
                            "{loop_count} {mcap_name} {count} {} publish\r",
                            channel.topic
                        );
                        // TODO(lucasw) publish a clock message
                    }
                }
                Err(e) => {
                    log::warn!("{mcap_name} {:?}\r", e);
                }
            }
        } // loop through all messages

        log::info!("{loop_count} {mcap_name} published {count} messages\r");
        loop_count += 1;
    }

    // TODO(lucasw) need to be able to receive a signal to exit the looping
    // Ok(())
}
