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
                    "first message time {msg_t0:.3}s ({:?})\r",
                    f64_secs_to_local_datetime(msg_t0),
                );

                play(&mcap_name, &mapped, &pubs, rx).await.unwrap();
            });
            handles.push(handle);
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

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
                }

                let mut delay = Delay::new(Duration::from_millis(1_000)).fuse();
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
            if main_loop_count < loop_count {
                log::info!("{loop_count} wait for main loop count to catch up to this one > {main_loop_count}\r");
                continue;
            } else if main_loop_count == loop_count {
                log::info!("{loop_count} main loop count and this one in sync, start playing back messages\r");
                break;
            } else {
                log::warn!("{loop_count} somehow have skipped entire loop/s, catching up -> {main_loop_count}\r");
                loop_count = main_loop_count;
                break;
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
