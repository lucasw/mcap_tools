/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away

mod misc;
use misc::get_params;
use misc::get_master_client;
use std::collections::HashMap;
use std::collections::HashSet;

roslibrust_codegen_macro::find_and_generate_ros_messages!();

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // view log messages from roslibrust in stdout
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
        .without_timestamps() // required for running wsl2
        .init()
        .unwrap();

    let mut params = HashMap::<String, String>::new();
    params.insert("_name".to_string(), "mcap_record".to_string());
    let (ns, full_node_name, _unused_args) = get_params(&mut params);
    let master_client = get_master_client(&full_node_name).await?;
    let nh = {
        let master_uri = std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());
        roslibrust::ros1::NodeHandle::new(&master_uri, &full_node_name).await?
    };

    // Setup a task to kill this process when ctrl_c comes in:
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        std::process::exit(0);
    });

    let mut subscribers = HashMap::new();

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
                let (topic, topic_type) = topic_and_type;
                let topic_copy = topic.clone();
                let mut subscriber = nh.subscribe_any::<std_msgs::ByteMultiArray>(topic, topic_type, 10).await?;

                let rv = tokio::spawn(async move {
                    /*
                    loop {
                        let rv = subscriber.next_raw().await;
                        println!("{rv:?}");
                    }
                    */
                    while let Some(data) = subscriber.next_raw().await {
                        if let Ok(data) = data {
                            println!("Got raw message data: {} bytes, {:?}", data.len(), data);
                        } else {
                            println!("{data:?}");
                        }
                    }
                });

                subscribers.insert(topic.clone(), rv);
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

        old_topics = cur_topics;
    }

    // Ok(())
}
