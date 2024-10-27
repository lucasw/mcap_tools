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
use simple_logger::SimpleLogger;
use std::collections::HashMap;

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

        fn get_sorted_indices(list: &[f64]) -> Vec<usize> {
            let mut indices = (0..list.len()).collect::<Vec<_>>();
            indices.sort_by(|&a, &b| list[a].partial_cmp(&list[b]).unwrap());
            indices
        }

        for topic_name in topic_names {
            let topic_data = topic_datas.get(topic_name).unwrap();
            let num = topic_data.len();
            if num < 2 {
                println!("{topic_name} {num} message");
            } else {
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

                let num_bins = 7;
                let mut bins = Vec::with_capacity(num_bins); // new().resize(bins, 0.0);
                for i in 0..num_bins {
                    let ind = num * i / num_bins;
                    bins.push(dts_sorted[sort_indices[ind]]);
                }
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

                let gap_start = topic_data.first().unwrap().log_time - message_start_time;
                let gap_end = message_end_time - topic_data.last().unwrap().log_time;

                // TODO(lucasw) can know these values much earlier from the summary, but want to
                // immediately go into each and histogram of gaps between messages
                let rate = num as f64 / elapsed;
                // TODO(lucasw) instead of printing, put all the stats into a struct for outputting
                // into a toml file
                println!("{topic_name}");
                println!("    {rate:.2}Hz {num}");
                println!("    rx time gap mean: {:.3}, std dev {:0.3}, min {:0.3}, max {longest_gap:0.1}\n    start gap: {gap_start:.3}, end gap: {gap_end:.3}",
                    dts.mean(),
                    dts.variance().sqrt(),
                    dts_sorted[sort_indices[0]],
                );
                println!(
                    "    relative time of longest gap ({longest_gap:.1}s): {:.3}s {:.1}%",
                    topic_data[longest_gap_ind].log_time - message_start_time,
                    100.0 * longest_gap_ind as f64 / num as f64,
                );
                print!("    bins: ");
                for bin in bins {
                    print!("{bin:.3} ");
                }
                println!();
            }
        }
    }

    Ok(())
}
