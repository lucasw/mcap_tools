/*!
load mcaps with an Odometry topic and visualize in rerun.io

adapted from github.com/lucasw/ros_one2z/mcap_to_rerun
*/

use anyhow::Result;
use rerun::external::glam;
use roslibrust_util::{nav_msgs, sensor_msgs};
use std::env;
use tf_roslibrust::tf_util::stamp_to_f64;

/*
fn print_type_of<T>(_: &T) -> String {
    format!("{}", std::any::type_name::<T>())
}
*/

fn mcap_to_rerun(
    rec: &rerun::RecordingStream,
    mcap_file_name: &str,
    odom_topic: &String,
    image_topic: &String,
    ind: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    // dbg!(path);

    let mapped = roslibrust_util::map_mcap(mcap_file_name)?;

    let mut points = Vec::new();
    let mut count = 0;
    let mut image_count = 0;
    let mut timestamp = 0.0;

    for message_raw in mcap::MessageStream::new(&mapped)? {
        // println!("{:?}", print_type_of(&message));
        match message_raw {
            Ok(message) => {
                if message.channel.topic == *image_topic {
                    match mcap_tools::raw_message_to_ros::<sensor_msgs::CompressedImage>(
                        message.data,
                    ) {
                        Ok(image_msg) => {
                            if image_count % 120 == 0 {
                                rec.set_timestamp_secs_since_epoch(
                                    "sensor_time",
                                    stamp_to_f64(&image_msg.header.stamp),
                                );
                                let img = rerun::EncodedImage::from_file_contents(image_msg.data);
                                // rec.log(message.channel.topic.clone(), &img)?;
                                rec.log("image", &img)?;
                            }
                            image_count += 1;
                        }
                        Err(e) => {
                            println!("{:?}", e);
                        }
                    }
                } else if message.channel.topic == *odom_topic {
                    // message.channel.schema == "nav_msgs/Odometry"
                    // println!("{:?}", message.channel.schema);

                    // https://github.com/adnanademovic/serde_rosmsg/blob/master/src/lib.rs#L9

                    /*
                    {
                        let mut test_msg = marti_common_msgs::Float32Stamped::default();
                        test_msg.header.seq = 3;
                        test_msg.header.frame_id = "test".to_string();
                        test_msg.header.stamp.secs = 1;
                        test_msg.header.stamp.nsecs = 7;
                        test_msg.value = 1.0;
                        let test_data = serde_rosmsg::to_vec(&test_msg).unwrap();
                        println!("test {} - {:02x?}", test_data.len(), &test_data[..]);
                        println!("mcap {} - {:02x?}", msg_with_header_vec.len(), &msg_with_header_vec[..]);
                    }
                    match serde_rosmsg::from_slice::<marti_common_msgs::Float32Stamped>(&msg_with_header) {
                    */

                    match mcap_tools::raw_message_to_ros::<nav_msgs::Odometry>(message.data) {
                        Ok(odom_msg) => {
                            // println!("{:#?}", odom_msg);
                            let pos = &odom_msg.pose.pose.position;
                            timestamp = stamp_to_f64(&odom_msg.header.stamp);
                            // TODO(lucasw) get min and max of xyz
                            if count % 100 == 0 {
                                let point = glam::vec3(pos.x as f32, pos.y as f32, pos.z as f32);
                                points.push(point);
                            }

                            count += 1;
                        }
                        Err(e) => {
                            println!("{:?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                println!("{:?}", e);
            }
        } // message_raw
    }

    println!("{} -> {} points extracted", count, points.len());

    let points_vec = vec![points];

    rec.set_timestamp_secs_since_epoch("sensor_time", timestamp);
    /*
    // this will move with the timeline
    rec.log(
        format!("{odom_topic}/position"),
        &rerun::LineStrips3D::new(points_vec.clone())
            .with_colors([rerun::Color::from([128, 128, 128, 255])]),
    )?;
    */

    // this will be persistent
    // let filename = path.file_stem().unwrap().to_string_lossy();
    let name = mcap_file_name.replace("/", "_");
    rec.log(
        format!("{odom_topic}/position/{name}"),
        &rerun::LineStrips3D::new(points_vec).with_colors([rerun::Color::from([
            180,
            ((ind * 5) % 256) as u8,
            (ind % 256) as u8,
            255,
        ])]),
    )?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rec = rerun::RecordingStreamBuilder::new("bag_odom_to_rerun").spawn()?;

    let args: Vec<String> = env::args().collect();

    let path = &args[1];
    let odom_topic = &args[2];
    let image_topic = &args[3];

    // dbg!(args);
    dbg!(path);
    dbg!(odom_topic);
    dbg!(image_topic);

    // let entry = entry?;
    match mcap_to_rerun(&rec, path, odom_topic, image_topic, 0) {
        Ok(()) => {}
        Err(e) => {
            println!("{:?}", e);
        }
    }

    Ok(())
}
