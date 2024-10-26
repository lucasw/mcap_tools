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

        let mut topics_log_times = HashMap::new();

        let summary = mcap::read::Summary::read(&mapped_mcap)?;
        // let summary = summary.ok_or(Err(anyhow::anyhow!("no summary")))?;
        let summary = summary.ok_or(anyhow::anyhow!("{mcap_name} no summary"))?;

        for channel in summary.channels.values() {
            log::info!("{:?}", channel);

            topics_log_times.insert(channel.topic.clone(), Vec::new());
        }

        let stats = summary
            .stats
            .ok_or(anyhow::anyhow!("{mcap_name} no stats"))?;

        let message_start_time = stats.message_start_time as f64 / 1e9;
        let message_end_time = stats.message_end_time as f64 / 1e9;
        let elapsed = message_end_time - message_start_time;

        for message in (mcap::MessageStream::new(&mapped_mcap)?).flatten() {
            topics_log_times
                .get_mut(&message.channel.topic)
                .unwrap()
                .push(message.log_time as f64 / 1e9);
        }

        for (topic_name, topic_log_times) in topics_log_times {
            // TODO(lucasw) can know these values much earlier from the summary, but want to
            // immediately go into each and histogram of gaps between messages
            let rate = topic_log_times.len() as f64 / elapsed;
            println!("{topic_name} {rate:.2}Hz");
        }
    }

    Ok(())
}
