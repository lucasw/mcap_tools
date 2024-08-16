//! load mcaps and exports all compressed images to disk into directories derived from topics
//! and filenames using the total nanoseconds of the timestamp

use std::{env, fs, path::PathBuf};

use anyhow::Result;
use mcap_tools::misc;

use roslibrust_codegen_macro::find_and_generate_ros_messages;

find_and_generate_ros_messages!();

/*
fn print_type_of<T>(_: &T) -> String {
    format!("{}", std::any::type_name::<T>())
}
*/

/*
fn ros_to_ns_string(ros_stamp: roslibrust_codegen::Time) -> f64 {
    ros_stamp.secs as f64 + ros_stamp.nsecs as f64 / 1e9
}
*/

fn mcap_extract(path: &PathBuf)
        -> Result<(), Box<dyn std::error::Error>> {
    dbg!(path);

    let mapped = misc::map_mcap(path.display().to_string())?;

    // let mut count = 0;
    // let mut image_count = 0;

    for message_raw in mcap::MessageStream::new(&mapped)? {
        // println!("{:?}", print_type_of(&message));
        match message_raw {
            Ok(message) => {
                match &message.channel.schema {
                    Some(schema) => {
                        if schema.name == "sensor_msgs/CompressedImage" {
                            let topic = message.channel.topic.replace("/", "__");
                            // println!("{}", schema.name);
                            let msg_with_header = misc::get_message_data_with_header(message.data);
                            match serde_rosmsg::from_slice::<sensor_msgs::CompressedImage>(&msg_with_header) {
                                Ok(image_msg) => {
                                    /*
                                    println!("{:?} {:?} -> {:?} {}",
                                        image_msg.header.stamp,
                                        message.channel.topic,
                                        topic,
                                        image_msg.data.len(),
                                    );
                                    */

                                    let image_name = format!("{topic}/{}{:0>9}.{}",
                                        image_msg.header.stamp.secs,
                                        image_msg.header.stamp.nsecs,
                                        image_msg.format,
                                        );
                                    println!("{image_name}");
                                    fs::create_dir_all(topic)?;
                                    fs::write(image_name, &image_msg.data)?;
                                },
                                Err(err) => {
                                    println!("{err:?}");
                                },
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

    // let mut ind = 0;
    for entry in paths {
        // ind += 1;
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
