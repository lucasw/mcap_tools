use memmap::Mmap;
use regex::Regex;
use roslibrust::ros1::PublisherAny;
use roslibrust_util::tf2_msgs;
use std::collections::HashMap;
use tokio::sync::broadcast;

// duplicated in tf_roslibrust, use that one instead of this
pub fn get_sorted_indices<T: PartialOrd>(list: &[T]) -> Vec<usize> {
    let mut indices = (0..list.len()).collect::<Vec<_>>();
    indices.sort_by(|&a, &b| list[a].partial_cmp(&list[b]).unwrap());
    indices
}

pub fn bins_text<T: std::fmt::Debug>(bins: &Vec<T>) -> String {
    let mut text = "".to_string();
    for bin in bins {
        text += &format!(" {bin:.3?}");
    }
    text
}

pub fn get_bins<T: Copy>(vals: &[T], sort_indices: &[usize], num_bins: usize) -> Vec<T> {
    // TODO(lucasw) can a fn have a same-size requirement for input vectors?
    // TODO(lucasw) return a Result and error on these
    assert!(vals.len() == sort_indices.len());
    assert!(!vals.is_empty());
    let num = vals.len();
    let mut bins = Vec::with_capacity(num_bins); // new().resize(bins, 0.0);
    for i in 0..(num_bins + 1) {
        let mut ind = num * i / num_bins;
        if ind == vals.len() {
            ind -= 1;
        }
        bins.push(vals[sort_indices[ind]]);
    }
    bins
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

pub async fn mcap_playback_init(
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
                let msg_with_header = roslibrust_util::get_message_data_with_header(message.data);
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

async fn wait_for_playback_time(
    clock_rx: &mut broadcast::Receiver<(f64, f64)>,
    msg_time: Option<f64>,
) -> Result<(f64, f64), broadcast::error::RecvError> {
    let mut lagged_count = 0;
    let mut last_clock_t = 0.0;
    loop {
        // if the clock_tx goes out of scope this should return RecvError::Closed
        match clock_rx.recv().await {
            // get the current playback time and start of playback time
            Ok((clock_t, msg_t0)) => {
                print!("-");
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
                        if clock_t > (msg_t0 - margin_s) {
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
                if lagged_count % 20 == 0 {
                    log::warn!("lagged, need to catch up {num}\r");
                }
                lagged_count += 1;
            }
            Err(broadcast::error::RecvError::Closed) => {
                log::warn!("channel is closed");
                return Err(broadcast::error::RecvError::Closed);
            }
        }
    }
}

pub async fn play_one_mcap(
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
        // TODO(lucasw) this isn't erroring out properly when clock_tx is done
        let (mut clock_t, mut msg_t0) = wait_for_playback_time(&mut clock_rx, None).await?;
        log::info!(
            "{loop_count} {mcap_name}, clock {clock_t:.1}, msg t start {msg_t0:.1}, elapsed {:.1}\r", clock_t - msg_t0
        );

        let mut last_msg_time = 0.0;
        let mut skipping = 0;
        let mut backwards_count = 0;
        // TODO(lucasw) refactor into a loop that get the next message and publisher from
        // the message stream, then does the right thing with waiting or exiting after that
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
                            let msg_time = message.log_time as f64 / 1e9;
                            if msg_time < msg_t0 {
                                // this message is from before the first message we want to
                                // publish, so skip it
                                continue;
                            }
                            if msg_time < last_msg_time {
                                backwards_count += 1;
                                let text = format!(
                                    "msg time moving backwards {:.2}, {:.2}s, run mcap sort",
                                    last_msg_time - msg_time,
                                    msg_time - msg_t0
                                );
                                log::warn!("{}", text);
                                return Err(anyhow::anyhow!(text));
                            }
                            last_msg_time = msg_time;
                            let msg_with_header =
                                roslibrust_util::get_message_data_with_header(message.data);

                            let elapsed = clock_t - msg_t0;
                            let threshold_s = 1.0;
                            // expect this to be positive, with the first message in the future
                            let delta_s = msg_time - clock_t;
                            // but if it's negative we're lagging some, a little is okay though
                            if -delta_s > threshold_s {
                                if skipping == 0 {
                                    let mut text =
                                        format!("{loop_count} {mcap_name} lagging {delta_s:.2}s, ");
                                    text += &format!("elapsed {elapsed:0.1}, skipping playback of this message {topic}, ");
                                    text += &format!("{count} other messages already published, {skipping} skipped\r");
                                    log::warn!("{}", text);
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
                                if new_clock_t == clock_t {
                                    log::error!("time isn't advancing {clock_t} {new_clock_t}");
                                } else if new_clock_t < clock_t {
                                    log::error!("time moving backwards {clock_t} {new_clock_t}");
                                    break;
                                }
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

        log::info!("{loop_count} {mcap_name} published {count} messages, {skipping} skipped\r");
        loop_count += 1;
    }

    // There are multiple paths above to exit out, all via '?', but would rather they made it here
    // instead if it's the normal process of RecvError::Closed
    // Ok((loop_count, count))
}
