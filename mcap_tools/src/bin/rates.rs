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
use mcap_tools::misc;
// use ordered_float::NotNan;
use serde_derive::{Deserialize, Serialize};
use simple_logger::SimpleLogger;
use std::collections::HashMap;

fn get_sorted_indices<T: PartialOrd>(list: &Vec<T>) -> Vec<usize> {
    let mut indices = (0..list.len()).collect::<Vec<_>>();
    indices.sort_by(|&a, &b| list[a].partial_cmp(&list[b]).unwrap());
    indices
}

fn get_bins<T: Copy>(vals: &Vec<T>, sort_indices: &Vec<usize>, num_bins: usize) -> Vec<T> {
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

// for reading/writing tomls
#[derive(Deserialize, Serialize, Debug)]
struct TopicStats {
    name: String,
    rate: Option<f64>,
}

fn main() -> Result<(), anyhow::Error> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()?;

    let args = std::env::args();
    let matches = command!()
        .arg(
            arg!(
                <mcaps> ... "mcaps to load"
            )
            .trailing_var_arg(true),
        )
        .get_matches_from(args);
    let mcap_names: Vec<_> = matches.get_many::<String>("mcaps").unwrap().collect();
    let mcap_names: Vec<String> = mcap_names.iter().map(|s| (**s).clone()).collect();
    log::info!("mcaps: {mcap_names:?}");

    // TODO(lucasw) not yet sure how multiple mcaps are supposed to be handled-
    // commingle them all together?  Should duplicate messages be identified and deduplicated?
    for mcap_name in mcap_names {
        log::info!("analyzing '{mcap_name}'");
        let mapped_mcap = misc::map_mcap(&mcap_name)?;

        struct TopicData {
            log_time: f64,
            size: usize,
        }

        let mut topic_datas = HashMap::new();

        let summary = mcap::read::Summary::read(&mapped_mcap)?;
        // let summary = summary.ok_or(Err(anyhow::anyhow!("no summary")))?;
        let summary = summary.ok_or(anyhow::anyhow!("{mcap_name} no summary"))?;

        for channel in summary.channels.values() {
            // log::info!("{:?}", channel);

            topic_datas.insert(channel.topic.clone(), Vec::new());
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

        fn bins_text<T: std::fmt::Debug>(bins: &Vec<T>) -> String {
            let mut text = "".to_string();
            for bin in bins {
                text += &format!(" {bin:.3?}");
            }
            text
        }

        let mut topic_stats_for_toml = Vec::new();

        for topic_name in topic_names {
            let topic_data = topic_datas.get(topic_name).unwrap();
            let gap_start = topic_data.first().unwrap().log_time - message_start_time;
            let gap_end = message_end_time - topic_data.last().unwrap().log_time;

            let num = topic_data.len();
            if num < 2 {
                println!("{topic_name} {num} message");
                println!("    start gap: {gap_start:.3}, end gap: {gap_end:.3}");

                // if only a few messages then will only compare to new mcaps based
                // on presence of any messages at all, won't penalize them for the wrong rate
                topic_stats_for_toml.push(TopicStats {
                    name: topic_name.to_string(),
                    rate: None,
                });
            } else {
                // can get rate from the summary as well
                let rate = num as f64 / elapsed;
                // TODO(lucasw) if only one message maybe need to set a different
                // field, make that field and rate optional, if neither is present
                // there will ?
                topic_stats_for_toml.push(TopicStats {
                    name: topic_name.to_string(),
                    rate: Some(rate),
                });

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
                    println!("{topic_name}");
                    println!("    {rate:.2}Hz {num}");
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
            }
        } // loop through all topics

        let mut topic_stats_for_toml_wrapper = HashMap::new();
        topic_stats_for_toml_wrapper.insert("topic_stats", topic_stats_for_toml);
        let toml_text = toml::to_string(&topic_stats_for_toml_wrapper).unwrap();
        println!("######{}#######", "#".repeat(mcap_name.len()));
        println!("##### {mcap_name} ######");
        println!("######{}#######", "#".repeat(mcap_name.len()));
        println!();
        println!("{toml_text}");
    }

    Ok(())
}
