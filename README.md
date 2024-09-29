# mcap tools

[mcap](https://mcap.dev) tools with initial emphasis on ros1 and rust using [roslibrust](https://github.com/Carter12s/roslibrust)

## installation

Setup on Ubuntu 22.04 and Debian Trixie using Debian Science Team ROS1 apt packages:

```
apt install python3-rosmsg rospack-tools libsensor-msgs-dev libstd-msgs-dev libgeometry-msgs-dev ros-sensor-msgs ros-std-msgs ros-geometry-msgs cargo pkg-config librust-openssl-dev
ROS_PACKAGE_PATH=`rospack find actionlib_msgs`:`rospack find std_msgs`:`rospack find sensor_msgs`:`rospack find geometry_msgs`:`rospack find rosgraph_msgs`:`rospack find std_srvs`:`rospack find tf2_msgs`
cargo install --git https://github.com/lucasw/mcap_tools
export PATH=$PATH:$HOME/.cargo/bin
```

Ubuntu 24.04 is possible building ros from source (https://github.com/lucasw/ros_from_src/tree/ubuntu2404 - TBD clean this up with clearer instructions)

## usage

### example

```
roscore
# ... publish some messages
```

record any messages with either sensors or odom in name, but don't record anything with 'image' in name,
and prefix with 'data':

```
mcap_record --regex "(.*)sensors(.*)|(.*)odom(.*)" --exclude "(.*)image(.*)" --outputprefix "data_"
```

the output files will look like and will increment the final five digits to 00001 and so on when the size limit is reached (2048MB by default)

```
data_2024_08_26_09_15_08_-07_00_00000.mcap
data_2024_08_26_09_15_08_-07_00_00001.mcap
...
```

(TODO(lucasw) make the date optional)

## build

```
# use ROS_PACKAGE_PATH from above
cargo build --release
```

### build and run in one line (for development)

Record all topics with sensors or odom in the full topic name to an mcap:
```
# use ROS_PACKAGE_PATH from above
cargo run --release --bin mcap_record -- --regex "(.*)sensors(.*)|(.*)odom(.*)"
```

```
# use ROS_PACKAGE_PATH from above
cargo run --release --bin mcap_extract /path/to/some.mcap
```

```
# do a git commit before this to see changes
cargo fmt --all
# this only provides suggestions, doesn't change anything
# use ROS_PACKAGE_PATH from above
cargo clippy
```

# existing mcap tools and information

https://github.com/lucasw/ros_one2z/tree/main/mcap_to_rerun - play select message types out of ros1 mcap bags into [rerun](https://rerun.io/)

https://rerun.io/blog/rosbag

https://github.com/foxglove/mcap/tree/main/rust - official mcap rust examples

https://mcap.dev/guides/getting-started/ros-1 and https://foxglove.dev/blog/mcap-vs-ros1-bag-index-performance

https://github.com/transitiverobotics/mcap-play-ros - play mcap files into ros1 with python
https://github.com/lucasw/ros_one2z/blob/main/one2z/scripts/ros1_play_mcap.py - my own python mcap ros1 player with some different features

https://github.com/facontidavide/mcap_editor

https://github.com/sensmore/kappe - only for ros2 messages?

https://github.com/eclipse-ecal/ecal-mcap-tools
