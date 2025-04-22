/*!
Generate a bunch of test messages for an mcap_record test
*/

use roslibrust_util::sensor_msgs;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // view log messages from roslibrust in stdout
    simple_logger::SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        // .without_timestamps() // required for running wsl2
        .init()
        .unwrap();

    let (full_node_name, _unused_args, _remaps) = {
        let mut params = HashMap::<String, String>::new();
        params.insert(
            "_name".to_string(),
            "mcap_generate_test_messages".to_string(),
        );
        let mut remaps = HashMap::<String, String>::new();
        let (_ns, full_node_name, unused_args) =
            roslibrust_util::get_params_remaps(&mut params, &mut remaps);
        (full_node_name, unused_args, remaps)
    };

    let master_uri =
        std::env::var("ROS_MASTER_URI").unwrap_or("http://localhost:11311".to_string());

    let nh = roslibrust::ros1::NodeHandle::new(&master_uri, &full_node_name).await?;

    let image0_pub = nh
        .advertise::<sensor_msgs::Image>("/image0", 10, false)
        .await?;
    let image1_pub = nh
        .advertise::<sensor_msgs::Image>("/image1", 10, false)
        .await?;
    let image2_pub = nh
        .advertise::<sensor_msgs::Image>("/image2", 10, false)
        .await?;
    let image3_pub = nh
        .advertise::<sensor_msgs::Image>("/image3", 10, false)
        .await?;

    let mut handles = Vec::new();

    tokio::time::sleep(tokio::time::Duration::from_millis(2000)).await;

    for image_pub in [image0_pub, image1_pub, image2_pub, image3_pub] {
        let handle = tokio::spawn(async move {
            for i in 0..200 {
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
                let mut msg = sensor_msgs::Image::default();
                msg.header.stamp = tf_roslibrust::tf_util::stamp_now();
                msg.width = 24;
                msg.height = 16;
                msg.encoding = "bgr8".to_string();
                msg.step = msg.width * 3;
                msg.data = vec![(i * 4) as u8; (msg.step * msg.height).try_into().unwrap()];
                image_pub.publish(&msg).await.unwrap();
            }
        });

        handles.push(handle);
        // tokio::time::sleep(tokio::time:duration::from_millis(2)).await;
    }

    for handle in handles {
        handle.await?;
    }

    Ok(())
}
