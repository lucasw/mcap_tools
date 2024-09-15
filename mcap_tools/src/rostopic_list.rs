/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away
use mcap_tools::misc;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let tracing_sub = tracing_subscriber::fmt().finish();
    tracing::subscriber::set_global_default(tracing_sub)?;

    let node_name = "/topic_list";

    let master_client = misc::get_master_client(node_name).await?;

    let mut params = HashMap::<String, String>::new();
    let mut _remaps = HashMap::<String, String>::new();
    let (ns, _full_node_name, _unused_args) = misc::get_params_remaps(&mut params, &mut _remaps);

    let topics = master_client.get_published_topics(ns).await?;
    for (topic_name, topic_type) in topics {
        println!("{topic_name} - {topic_type}");
    }

    Ok(())
}
