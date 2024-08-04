/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away

mod misc;
use misc::get_params;
use misc::get_master_client;
use std::collections::HashMap;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let tracing_sub = tracing_subscriber::fmt().finish();
    tracing::subscriber::set_global_default(tracing_sub)?;

    let node_name = "/topic_list";

    let master_client = get_master_client(&node_name).await?;

    let mut params = HashMap::<String, String>::new();
    let (ns, full_node_name, unused_args) = get_params(&mut params);

    // tokio::spawn(async mov {
    let topics = master_client.get_published_topics(ns).await?;
    for (topic_name, topic_type) in topics {
        println!("{topic_name} - {topic_type}");
    }

    Ok(())
}
