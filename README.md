# mcap tools

[mcap](https://mcap.dev) tools with initial emphasis on ros1 and rust using [roslibrust](https://github.com/Carter12s/roslibrust)

Record all topics with sensors or odom in the full topic name to an mcap:
```
ROS_PACKAGE_PATH=`rospack find std_msgs`:`rospack find sensor_msgs`:`rospack find geometry_msgs` cargo build --release -- "(.*)sensors(.*)|(.*)odom(.*)"
```

```
ROS_PACKAGE_PATH=`rospack find std_msgs`:`rospack find sensor_msgs`:`rospack find geometry_msgs` cargo run --release --bin mcap_extract /path/to/some.mcap
```

```
# do a git commit before this to see changes
cargo fmt --all
# this only provides suggestions, doesn't change anything
ROS_PACKAGE_PATH=`rospack find std_msgs`:`rospack find sensor_msgs`:`rospack find geometry_msgs` cargo clippy
```

## existing tools and information

https://github.com/lucasw/ros_one2z/tree/main/mcap_to_rerun - play select message types out of ros1 mcap bags into [rerun](https://rerun.io/)

https://rerun.io/blog/rosbag

https://github.com/foxglove/mcap/tree/main/rust - official mcap rust examples

https://mcap.dev/guides/getting-started/ros-1 and https://foxglove.dev/blog/mcap-vs-ros1-bag-index-performance

https://github.com/transitiverobotics/mcap-play-ros - play mcap files into ros1 with python
https://github.com/lucasw/ros_one2z/blob/main/one2z/scripts/ros1_play_mcap.py - my own python mcap ros1 player with some different features

https://github.com/sensmore/kappe - only for ros2 messages?

https://github.com/eclipse-ecal/ecal-mcap-tools
