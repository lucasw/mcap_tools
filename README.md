# mcap tools

[mcap](https://mcap.dev) tools with initial emphasis on ros1 and rust using [roslibrust](https://github.com/Carter12s/roslibrust)


```
ROS_PACKAGE_PATH=`rospack find std_msgs` cargo run --release
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
