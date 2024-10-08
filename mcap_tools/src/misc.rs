use anyhow::{Context, Result};
use camino::Utf8Path;
use memmap::Mmap;
use roslibrust::ros1::{determine_addr, MasterClient, NodeServerHandle, XmlRpcServer};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing;

roslibrust_codegen_macro::find_and_generate_ros_messages!();

pub async fn get_master_client(node_name: &str) -> Result<MasterClient, anyhow::Error> {
    // copied this out of roslibrust actor.rs Node::new(), seemed like bare minimum
    // to make a valid master client
    let master_uri =
        std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());

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

    let master_client = MasterClient::new(
        master_uri.clone(),
        client_uri.clone(),
        node_name.to_string(),
    )
    .await?;

    tracing::info!("{node_name} connected to roscore at {master_uri} from {client_uri}");

    Ok(master_client)
}

/// pass in empty hashmap or set it up with params to look for
/// let mut params = HashMap::<String, String>::new();
/// params.insert("update_rate".to_string(), "5.0".to_string());
///
/// returns full path node name, the namespace, and a vector of unused args
pub fn get_params_remaps(
    params: &mut HashMap<String, String>,
    remaps: &mut HashMap<String, String>,
) -> (String, String, Vec<String>) {
    // TODO(lucasw) generate a unique node name
    // let _ = params.try_insert("_name".to_string(), "node_tbd".to_string());
    if !params.contains_key("_name") {
        params.insert("_name".to_string(), "node_tbd".to_string());
    }
    params.insert("_ns".to_string(), "".to_string());

    // TODO(lucasw) can an existing rust arg handling library handle the ':=' ros cli args?
    let args = std::env::args();
    let mut args2 = Vec::new();
    for arg in args {
        let key_val: Vec<&str> = arg.split(":=").collect();
        if key_val.len() != 2 {
            args2.push(arg);
            continue;
        }

        let (mut key, val) = (key_val[0].to_string(), key_val[1].to_string());
        if !key.starts_with('_') {
            remaps.insert(key, val);
            continue;
        }
        key.replace_range(0..1, "");

        if !params.contains_key(&key) {
            println!("unexpected param: '{key}' '{val}'");
            // continue;
        }
        params.insert(key, val);
    }
    println!("{args2:?}");

    let ns = params.remove("_ns").unwrap();
    let full_node_name = &format!("/{}/{}", &ns, &params["_name"],).replace("//", "/");

    let mut updated_remaps = HashMap::new();
    // if the topic is relative, add the namespace here and update the remaps
    for (topic_orig, topic_remap) in &mut *remaps {
        let topic_orig = topic_orig.clone();
        if !topic_remap.starts_with('/') {
            let updated_topic_remap = format!("{ns}/{}", topic_remap).to_string();
            updated_remaps.insert(topic_orig, updated_topic_remap);
        } else {
            updated_remaps.insert(topic_orig, topic_remap.to_string());
        }
        // TODO(lucasw) handle private topics with '~' leading?
    }
    *remaps = updated_remaps;
    /*
    for (topic_orig, topic_remap) in updated_remaps {
        remaps.insert(topic_orig, topic_remap);
    }
    */

    (ns.to_string(), full_node_name.to_string(), args2)
}

// TODO(lucasw) why is this needed?  https://docs.rs/mcap/latest/mcap/ doesn't explain it
pub fn map_mcap<P: AsRef<Utf8Path>>(p: P) -> Result<Mmap> {
    let fd = std::fs::File::open(p.as_ref()).context("Couldn't open MCAP file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
}

// TODO(lucasw) https://github.com/Carter12s/roslibrust/issues/158#issuecomment-2187839437
pub fn get_message_data_with_header(raw_message_data: std::borrow::Cow<'_, [u8]>) -> Vec<u8> {
    let len_header = raw_message_data.len() as u32;
    let mut msg_with_header = Vec::from(len_header.to_le_bytes());
    let mut message_data = Vec::from(raw_message_data);
    msg_with_header.append(&mut message_data);
    msg_with_header
}
