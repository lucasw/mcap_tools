/*!
Lucas Walter

Visualize Image topic/s within an mcap file in a minifb window
*/

use anyhow::Result;
// use image::ImageReader;
// use image::EncodableLayout;
use minifb::{Scale, ScaleMode, Window, WindowOptions};
use opencv::core::Mat;
use opencv::prelude::{MatTraitConst, MatTraitConstManual};
use roslibrust_util::sensor_msgs::CompressedImage;
use simple_logger::SimpleLogger;
use std::collections::HashMap;
use std::env;
// use std::io::Cursor;

// image crate return rgb, but want bgr
/*
fn compressed_image_to_rgb8(data: &[u8]) -> Result<(Vec<u8>, usize, usize)> {
    let reader = ImageReader::new(Cursor::new(data))
        .with_guessed_format()
        .expect("Cursor io never fails");

    let img = reader.decode()?;

    let rgb8 = &img.to_rgba8();

    let width = usize::try_from(rgb8.width())?;
    let height = usize::try_from(rgb8.height())?;

    Ok((rgb8.as_bytes().to_vec(), width, height))
}
*/

fn compressed_image_to_bgr8(data: &[u8]) -> Result<(Vec<u8>, usize, usize)> {
    let buf = Mat::from_slice::<u8>(data)?;
    let cv_image_orig = opencv::imgcodecs::imdecode(
        &buf,
        // opencv::imgcodecs::ImreadModes::IMREAD_COLOR_BGR as i32,
        opencv::imgcodecs::ImreadModes::IMREAD_COLOR as i32,
    )?;
    let mut cv_image = Mat::default();
    opencv::imgproc::cvt_color(
        &cv_image_orig,
        &mut cv_image,
        opencv::imgproc::ColorConversionCodes::COLOR_BGR2BGRA as i32,
        4,
    )?;
    let size = cv_image.size()?;
    let width = size.width as usize;
    let height = size.height as usize;

    Ok((cv_image.data_bytes()?.to_vec(), width, height))
}

fn main() -> Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info)
        .init()?;

    let args: Vec<String> = env::args().collect();
    let mcap_name = &args[1];
    log::info!("viewing image from '{mcap_name}'");
    let mapped_mcap = roslibrust_util::map_mcap(mcap_name)?;

    let mut viewers_by_topic: HashMap<String, (Window, usize, usize)> = HashMap::new();

    log::info!("iterating through messages");
    loop {
        for message in mcap::MessageStream::new(&mapped_mcap)? {
            let message = message?;
            // let schema = message.channel.schema.ok_or(anyhow::anyhow!("bad schema"))?.clone(); //.as_ref();
            let schema_name = &message
                .channel
                .schema
                .clone()
                .ok_or(anyhow::anyhow!("bad schema"))?
                .name;
            if schema_name.ne("sensor_msgs/CompressedImage") {
                continue;
            }
            let topic = &message.channel.topic;

            let msg_with_header = roslibrust_util::get_message_data_with_header(message.data);
            let msg = serde_rosmsg::from_slice::<CompressedImage>(&msg_with_header).unwrap();
            let (rgb8, width, height) = compressed_image_to_bgr8(&msg.data)?;

            let (window, width0, height0) =
                viewers_by_topic.entry(topic.clone()).or_insert_with(|| {
                    log::info!(
                        "visualizing {topic} {width}x{height} ({} bytes)",
                        rgb8.len()
                    );
                    let window = Window::new(
                        topic,
                        width,
                        height,
                        WindowOptions {
                            resize: true,
                            scale: Scale::X1,
                            scale_mode: ScaleMode::AspectRatioStretch,
                            ..WindowOptions::default()
                        },
                    )
                    .unwrap();
                    (window, width, height)
                });
            if width != *width0 || height != *height0 {
                log::warn!("inconsistent image size topic width {width} was {width0}, height {height} was {height0}");
                // continue;
            }

            let rgba32 = {
                // TODO(lucasw) safe way to do this?
                unsafe {
                    let (prefix, rgba32, suffix) = rgb8.align_to::<u32>();
                    if !prefix.is_empty() || !prefix.is_empty() {
                        log::warn!(
                            "{} u8 -> {prefix:?} {} {suffix:?}",
                            rgb8.len(),
                            rgba32.len()
                        );
                    }
                    rgba32
                }
            };
            window.update_with_buffer(rgba32, width, height)?;
        }
    }

    // Ok(())
}
