//! load mcaps and exports all compressed images to disk into directories derived from topics
//! and filenames using the total nanoseconds of the timestamp

use std::{env, fs, path::PathBuf};

use anyhow::{Context, Result};
use camino::Utf8Path;
use memmap::Mmap;

use roslibrust_codegen_macro::find_and_generate_ros_messages;

find_and_generate_ros_messages!();

/*
fn print_type_of<T>(_: &T) -> String {
    format!("{}", std::any::type_name::<T>())
}
*/

fn map_mcap<P: AsRef<Utf8Path>>(p: P) -> Result<Mmap> {
    let fd = fs::File::open(p.as_ref()).context("Couldn't open MCAP file")?;
    unsafe { Mmap::map(&fd) }.context("Couldn't map MCAP file")
}

// TODO(lucasw) https://github.com/Carter12s/roslibrust/issues/158#issuecomment-2187839437
fn get_message_data_with_header<'a>(raw_message_data: std::borrow::Cow<'a, [u8]>) -> Vec<u8> {
    let len_header = raw_message_data.len() as u32;
    let mut msg_with_header = Vec::from(len_header.to_le_bytes());
    let mut message_data = Vec::from(raw_message_data);
    msg_with_header.append(&mut message_data);
    msg_with_header
}

/*
fn ros_to_ns_string(ros_stamp: roslibrust_codegen::Time) -> f64 {
    ros_stamp.secs as f64 + ros_stamp.nsecs as f64 / 1e9
}
*/

fn mcap_extract(path: &PathBuf)
        -> Result<(), Box<dyn std::error::Error>> {
    dbg!(path);

    let mapped = map_mcap(path.display().to_string())?;

    // let mut count = 0;
    // let mut image_count = 0;

    for message_raw in mcap::MessageStream::new(&mapped)? {
        // println!("{:?}", print_type_of(&message));
        match message_raw {
            Ok(message) => {
                match message.channel.schema {
                    Some(schema) => {
                        if schema.name == "sensor_msgs/CompressedImage" {
                            let topic = message.channel.topic.replace("/", "__");
                            // println!("{}", schema.name);
                            let msg_with_header = get_message_data_with_header(message.data);
                            match serde_rosmsg::from_slice::<sensor_msgs::CompressedImage>(&msg_with_header) {
                                Ok(image_msg) => {
                                    println!("{:?} {:?} -> {:?}", image_msg.header.stamp, message.channel.topic, topic);
                                }
                            }
                        }
                    },
                    None => {},
                }
            }
            Err(e) => {
                println!("{:?}", e);
            },
        }  // message_raw
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let path = &args[1];

    // dbg!(args);
    dbg!(path);

    let mut paths: Vec<_> = std::fs::read_dir(path).unwrap()
                                                   .map(|r| r.unwrap())
                                                   .collect();
    paths.sort_by_key(|dir| dir.path());

    let mut ind = 0;
    for entry in paths {
        ind += 1;
        // let entry = entry?;
        match mcap_extract(&entry.path()) {
            Ok(()) => {
            },
            Err(e) => {
                println!("{:?}", e);
            },
        }
    }

    Ok(())
}
