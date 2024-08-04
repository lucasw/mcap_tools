/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away

mod misc;
use misc::get_params;
use misc::get_master_client;
use std::collections::HashMap;
use std::collections::HashSet;
// use tracing_subscriber;

roslibrust_codegen_macro::find_and_generate_ros_messages!();

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // let tracing_sub = tracing_subscriber::fmt().finish();
    // tracing::subscriber::set_global_default(tracing_sub)?;

    let mut params = HashMap::<String, String>::new();
    let (ns, full_node_name, _unused_args) = get_params(&mut params);
    let master_client = get_master_client(&full_node_name).await?;

    // Setup a task to kill this process when ctrl_c comes in:
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        std::process::exit(0);
    });

    let mut old_topics = HashSet::<(String, String)>::new();
    loop {
        // TODO(lucasw) optionally limit to namespace of this node (once this node can be launched into
        // a namespace)
        let topics = master_client.get_published_topics(ns.clone()).await?;
        let mut cur_topics = HashSet::<(String, String)>::new();
        for (topic_name, topic_type) in &topics {
            cur_topics.insert(((*topic_name).clone(), (*topic_type).clone()));
            // println!("{topic_name} - {topic_type}");
        }

        for topic_and_type in &old_topics {
            if !cur_topics.contains(topic_and_type) {
                println!("removed {topic_and_type:?}");
            }
        }
        for topic_and_type in &cur_topics {
            if !old_topics.contains(topic_and_type) {
                println!("added {topic_and_type:?}");
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        old_topics = cur_topics;
    }

    // Ok(())
}
