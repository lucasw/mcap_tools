/// Copyright 2024 Lucas Walter
/// BSD 3-Clause
///
/// analyze the update rates of every channel in an mcap, output a report
/// average rate across the whole mcap
/// when the first message arrived relative to summary message start time, and last message to end
/// time
/// longest and shortest gap between messages, and standard deviation
///
/// TBD rolling window update rates, output in a new mcap for plotting in another tool
/// (e.g. plotjuggler, maybe rerun, or make tools to make plots in egui, or save images of graphs
/// to disk)
use clap::{arg, command};
use mcap_tools::{bins_text, get_bins, get_sorted_indices};
use roslibrust_util::TopicStats;
// use ordered_float::NotNan;
use simple_logger::SimpleLogger;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::prelude::*;

fn main() -> Result<(), anyhow::Error> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .without_timestamps() // reduce clutter, timestamps don't matter too much unless this is
        // really slow
        .init()?;

    let mut errors_observed = false;
    // TODO(lucasw) make into clap arg
    let threshold = 0.1;
    // lowest possible rate
    let epsilon = 0.01;

    let args = std::env::args();
    let matches = command!()
        .arg(
            arg!(
                -i --input <INPUT> "expected channel/topic rates in toml file, error if current mcap is too different"
            ), // .default_value("expected_mcap_channel_rates.toml")
        )
        .arg(
            arg!(
                -o --output <OUTPUT> "record channel/topic rates and types to this toml"
            ), // .default_value("observed_mcap_channel_rates.toml")
        )
        .arg(
            arg!(
                -r --round <ROUND> "round rates to this number of digits after the decimal"
            )
            .value_parser(clap::value_parser!(usize))
            .required(false)
        )
        .arg(
            arg!(
                <mcaps> ... "mcaps to load"
            )
            .trailing_var_arg(true),
        )
        .get_matches_from(args);

    let expected_stats: Option<HashMap<String, TopicStats>>;
    let input_toml_name = matches.get_one::<String>("input");
    if let Some(input_toml_name) = input_toml_name {
        log::info!("getting expected rates from toml: {input_toml_name}");
        expected_stats = Some(roslibrust_util::get_expected_rates_from_toml(
            input_toml_name,
        )?);
    } else {
        expected_stats = None;
    }

    let mut output_toml = None;
    let output_toml_name = matches.get_one::<String>("output");
    if let Some(output_toml_name) = output_toml_name {
        log::info!("outputting rates to toml: {output_toml_name}");
        output_toml = Some(File::create(output_toml_name)?);
    }

    let mcap_names: Vec<_> = matches.get_many::<String>("mcaps").unwrap().collect();
    let mcap_names: Vec<String> = mcap_names.iter().map(|s| (**s).clone()).collect();
    log::info!("mcaps: {mcap_names:?}");

    let round_digits = matches.get_one::<usize>("round");
    log::info!("rounding digits: {round_digits:?}");

    // TODO(lucasw) not yet sure how multiple mcaps are supposed to be handled-
    // commingle them all together?  Should duplicate messages be identified and deduplicated?
    for mcap_name in mcap_names {
        log::info!("analyzing '{mcap_name}'");
        let mapped_mcap = roslibrust_util::map_mcap(&mcap_name)?;

        struct TopicData {
            log_time: f64,
            size: usize,
        }

        // TODO(lucasw) combine data structures
        let mut topic_datas = HashMap::new();
        let mut topic_types = HashMap::new();

        let summary = mcap::read::Summary::read(&mapped_mcap)?;
        // let summary = summary.ok_or(Err(anyhow::anyhow!("no summary")))?;
        let summary = summary.ok_or(anyhow::anyhow!("{mcap_name} no summary"))?;

        for channel in summary.channels.values() {
            // log::info!("{:?}", channel);
            topic_datas.insert(channel.topic.clone(), Vec::new());
            let topic_type = channel.schema.clone().unwrap().name.clone();
            topic_types.insert(channel.topic.clone(), topic_type);
        }

        let stats = summary
            .stats
            .ok_or(anyhow::anyhow!("{mcap_name} no stats"))?;

        let message_start_time = stats.message_start_time as f64 / 1e9;
        let message_end_time = stats.message_end_time as f64 / 1e9;
        let elapsed = message_end_time - message_start_time;

        for message in (mcap::MessageStream::new(&mapped_mcap)?).flatten() {
            let topic_data = TopicData {
                log_time: message.log_time as f64 / 1e9,
                size: message.data.len(),
            };

            topic_datas
                .get_mut(&message.channel.topic)
                .unwrap()
                .push(topic_data);
        }

        let mut topic_names: Vec<&String> = topic_datas.keys().clone().collect();
        topic_names.sort_unstable();

        let mut topic_stats_for_toml = Vec::new();

        // look for topics in expected that aren't in the observed
        if let Some(ref expected_stats) = expected_stats {
            // TODO(lucaw) one liner for this
            let mut observed_topics = HashSet::<String>::new();
            for topic_name in &topic_names {
                observed_topics.insert((*topic_name).clone());
            }

            for (_expected_topic, expected_stat) in expected_stats.iter() {
                let expected_topic: &str = &expected_stat.topic;
                if !observed_topics.contains(expected_topic) {
                    errors_observed = true;
                    log::error!(
                        "mcap is missing '{}' {}",
                        expected_stat.topic,
                        expected_stat.topic_type
                    );
                }
            }
        }

        for topic_name in topic_names {
            let topic_data = topic_datas.get(topic_name).unwrap();
            let topic_type = topic_types.get(topic_name).unwrap().to_string();
            let gap_start = topic_data.first().unwrap().log_time - message_start_time;
            let gap_end = message_end_time - topic_data.last().unwrap().log_time;

            let rate;
            let num = topic_data.len();
            if num < 2 {
                println!("{topic_name} {num} message");
                println!("    start gap: {gap_start:.3}, end gap: {gap_end:.3}");

                // if only a few messages then will only compare to new mcaps based
                // on presence of any messages at all, won't penalize them for the wrong rate
                rate = None;
            } else {
                // can get rate from the summary as well
                let mut observed_rate = num as f64 / elapsed;
                // TODO(lucasw) if only one message maybe need to set a different
                // field, make that field and rate optional, if neither is present
                // there will ?
                if let Some(round_digits) = round_digits {
                    observed_rate = format!(
                        "{observed_rate:.round_digits$}",
                        round_digits = round_digits
                    )
                    .parse()
                    .unwrap();
                }

                rate = Some(observed_rate);

                {
                    let mut dts = nalgebra::DVector::zeros(num - 1);
                    let mut dts_sorted = Vec::new(); // with_capacity(num - 1);
                    dts_sorted.resize(num - 1, 0.0);
                    for i in 0..(num - 1) {
                        let dt = topic_data[i + 1].log_time - topic_data[i].log_time;
                        dts[i] = dt;
                        dts_sorted[i] = dt;
                    }
                    let sort_indices = get_sorted_indices(&dts_sorted);
                    // dts_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

                    let bins = get_bins(&dts_sorted, &sort_indices, 7);

                    /*
                    let half_num = num / 2;
                    let median;
                    if num % 2 == 0 {
                        median = (dts_sorted[half_num - 1] + dts_sorted[half_num]) / 2.0;
                    } else {
                        median = dts_sorted[half_num];
                    }*/

                    let longest_gap_ind = *sort_indices.last().unwrap();
                    let longest_gap = dts_sorted[longest_gap_ind];

                    // TODO(lucasw) instead of printing, put all the stats into a struct for outputting
                    // into a toml file
                    log::info!("{topic_name}");
                    println!("    {observed_rate:.2}Hz {num}");
                    println!("    rx time gap mean: {:.3}, std dev {:0.3}, min {:0.3}, max {longest_gap:0.1}",
                        dts.mean(),
                        dts.variance().sqrt(),
                        dts_sorted[sort_indices[0]],
                    );
                    println!("    start gap: {gap_start:.3}, end gap: {gap_end:.3}");
                    println!(
                        "    relative time of longest gap ({longest_gap:.1}s): {:.3}s {:.1}%",
                        topic_data[longest_gap_ind].log_time - message_start_time,
                        100.0 * longest_gap_ind as f64 / num as f64,
                    );
                    println!("    gap bins seconds: {}", bins_text(&bins));
                } // gaps

                // message size
                {
                    let mut szs = nalgebra::DVector::zeros(num);
                    let mut szs_sorted = vec![0; num];
                    for i in 0..num {
                        let sz = topic_data[i].size;
                        szs[i] = sz;
                        szs_sorted[i] = sz;
                    }
                    let sort_indices = get_sorted_indices(&szs_sorted);
                    // szs_sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

                    let smallest_sz_ind = *sort_indices.first().unwrap();
                    let smallest_sz = szs_sorted[smallest_sz_ind];
                    let largest_sz_ind = *sort_indices.last().unwrap();
                    let largest_sz = szs_sorted[largest_sz_ind];

                    // TODO(lucasw) many messages are all the same size, this will only be
                    // interesting for compressed images and similar dynamic payloads
                    if smallest_sz != largest_sz {
                        // println!("    {smallest_sz}b - {largest_sz}b");
                        let bins = get_bins(&szs_sorted, &sort_indices, 7);
                        println!("    bytes bins: {}", bins_text(&bins));
                    } else {
                        println!("    {smallest_sz}b (all)");
                    }
                }

                // compare rates if both are available
                // if topic isn't in expected, note it here (already checked for
                // if the expected topics aren't in the current mcap above)
                if let Some(ref expected_stats) = expected_stats {
                    let expected_stat = expected_stats.get(topic_name);
                    if let Some(expected_stat) = expected_stat {
                        if rate.is_some() && expected_stat.rate.is_some() {
                            let observed_rate = rate.unwrap();
                            let expected_rate = expected_stat.rate.unwrap();
                            if observed_rate <= epsilon || expected_rate <= epsilon {
                                errors_observed = true;
                                log::error!("too small observed {observed_rate} or expected {expected_rate} < {epsilon}");
                            } else {
                                let ratio = observed_rate / expected_rate;
                                if ratio > (1.0 + threshold) {
                                    errors_observed = true;
                                    log::error!("ratio {ratio:.3} out of tolerance, observed {observed_rate} > {expected_rate} expected");
                                } else if ratio < (1.0 - threshold) {
                                    errors_observed = true;
                                    log::error!("ratio {ratio:.3} out of tolerance, observed {observed_rate} < {expected_rate} expected");
                                } else if ratio == 1.0 {
                                    log::warn!("ratio {ratio} is exactly 1.0 {observed_rate} == {expected_rate}");
                                } else {
                                    log::info!("ratio {ratio:.3} in tolerance ({threshold})");
                                }
                            }
                        } else {
                            errors_observed = true;
                            log::error!(
                                "rate mismatch observed: {:?} vs. expected: {:?}",
                                rate,
                                expected_stat.rate
                            );
                        }
                    } else {
                        errors_observed = true;
                        log::error!("unexpected observed topic '{}' {}", topic_name, topic_type);
                    }
                }
            }

            topic_stats_for_toml.push(TopicStats {
                topic: topic_name.to_string(),
                topic_type,
                rate,
            });
        } // loop through all topics

        let mut topic_stats_for_toml_wrapper = HashMap::new();
        topic_stats_for_toml_wrapper.insert("topic_stats", topic_stats_for_toml);
        let toml_text = toml::to_string(&topic_stats_for_toml_wrapper).unwrap();
        match output_toml {
            None => {
                /*
                println!("######{}#######", "#".repeat(mcap_name.len()));
                println!("##### {mcap_name} ######");
                println!("######{}#######", "#".repeat(mcap_name.len()));
                println!();
                println!("{toml_text}");
                */
            }
            Some(ref mut output_toml) => {
                // TODO(lucasw) I think this will append if there are multiple mcaps,
                // which isn't going to be correct if they have the same topics, need to commingle
                // the results
                log::info!("writiing to toml '{}'", output_toml_name.unwrap());
                output_toml.write_all(toml_text.as_bytes())?;
            }
        }
    }

    if errors_observed {
        return Err(anyhow::anyhow!("errors observed"));
    }
    Ok(())
}
