/// get a list of ros topics from the master, optionally loop and show new topics that appear
/// or note old topics that have gone away

use roslibrust::ros1::MasterClient;
use roslibrust::ros1::NodeServerHandle;
use roslibrust::ros1::XmlRpcServer;
use roslibrust::ros1::determine_addr;
use tokio::sync::mpsc;
use tracing_subscriber;

roslibrust_codegen_macro::find_and_generate_ros_messages!();

async fn get_master_client(node_name: &str) -> Result<MasterClient, anyhow::Error> {
    // TODO(lucasw) copied this out of roslibrust actor.rs Node::new()
    let master_uri = std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());

    let (node_sender, _node_receiver) = mpsc::unbounded_channel();
    let xml_server_handle = NodeServerHandle {
        node_server_sender: node_sender.clone(),
        // None here because this handle should not keep task alive
        _node_task: None,
    };

    let (addr, hostname) = determine_addr().await?;

    // Create our xmlrpc server and bind our socket so we know our port and can determine our local URI
    let xmlrpc_server = XmlRpcServer::new(addr, xml_server_handle)?;
    let client_uri = format!("http://{hostname}:{}", xmlrpc_server.port());

    let master_client = MasterClient::new(master_uri.clone(), client_uri.clone(), node_name.to_string()).await?;

    tracing::info!("{node_name} connected to roscore at {master_uri} from {client_uri}");

    Ok(master_client)
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let tracing_sub = tracing_subscriber::fmt().finish();
    tracing::subscriber::set_global_default(tracing_sub)?;

    let node_name = "/topic_list";

    let master_client = get_master_client(&node_name).await?;

    // TODO(lucasw) optionally limit to namespace of this node (once this node can be launched into
    // a namespace)
    let topics = master_client.get_published_topics("").await?;
    for (topic_name, topic_type) in topics {
        println!("{topic_name} - {topic_type}");
    }

    Ok(())
}
