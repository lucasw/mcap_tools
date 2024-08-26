#!/usr/bin/env python
# Launch a command line application that isn't installed in a package
import subprocess
import sys

import rospy


argv = sys.argv[1:]

# TODO(lucasw) if we don't want to send ros arguments to the application
strip_ros_args = False
if strip_ros_args:
    argv = rospy.myargv(argv=argv)

try:
    sys.exit(subprocess.call(argv))
except KeyboardInterrupt as ex:
    pass
