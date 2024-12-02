/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away
use chrono::prelude::DateTime;
use clap::{arg, command};
use regex::Regex;
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

use roslibrust_util::{rosgraph_msgs, tf2_msgs};

fn f64_secs_to_local_datetime(secs: f64) -> DateTime<chrono::prelude::Local> {
    let d = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs_f64(secs);
    let utc_datetime = DateTime::<chrono::Utc>::from(d);
    let local_datetime: DateTime<chrono::prelude::Local> = DateTime::from(utc_datetime);
    local_datetime
}

fn get_wall_time() -> f64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
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
        let (_ns, full_node_name, unused_args) =
            roslibrust_util::get_params_remaps(&mut params, &mut remaps);
        (full_node_name, unused_args, remaps)
    };

    let master_uri =
        std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());

    let (max_loops, publish_clock, (include_re, exclude_re), start_secs_offset, mcap_names) =
        get_non_ros_cli_args(unused_args)?;

    // TODO(lucasw) if nh is shut down too abruptly by going out of scope it won't unregister all the publishers-
    // that needs to happen in the node handle drop just like with subscribers,
    // but for now leave out side of the giant block here and wait extra at the end
    let nh = roslibrust::ros1::NodeHandle::new(&master_uri, &full_node_name).await?;
    let mut handles = Vec::new();
    {
        // TODO(lucasw) put this entire block in a function
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
                let mapped = roslibrust_util::map_mcap(mcap_name)?;

                // initialize the start times and publishers
                let rv = mcap_tools::mcap_playback_init(
                    &nh,
                    mcap_name,
                    &mapped,
                    &include_re,
                    &exclude_re,
                    &remaps,
                )
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

                let tf_static_topic = {
                    match remaps.get("/tf_static") {
                        Some(topic) => topic.to_string(),
                        None => "/tf_static".to_string(),
                    }
                };
                let latching = true;
                let static_publisher = nh
                    .advertise::<tf2_msgs::TFMessage>(&tf_static_topic, 10, latching)
                    .await?;
                static_publisher.publish(&tf_static_aggregated)?;
                Some(static_publisher)
            } else {
                None
            }
        };

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

                let _rv = mcap_tools::play_one_mcap(&mcap_name, &mapped, &pubs, clock_rx).await;
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
                                secs: clock_seconds as i32,
                                nsecs: ((clock_seconds % 1.0) * 1e9_f64) as i32,
                            },
                        };
                        clock_publisher.publish(&clock_msg)?;
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
    // let publishers unregister
    // TODO(lucasw) need a way to await this instead of magic sleep number
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    Ok(())
}
