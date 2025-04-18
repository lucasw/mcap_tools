#!/usr/bin/env python
# Launch a command line application that isn't installed in a package
import subprocess
import signal
import sys

import rospy


argv = sys.argv[1:]

# TODO(lucasw) if we don't want to send ros arguments to the application
if len(argv) > 0 and argv[0] == "--strip-ros-args":
    print("stripping ros arguments")
    argv = argv[1:]
    argv = rospy.myargv(argv=argv)

print(f"ARGS {argv}")

# def signal_handler(sig, frame):
#     print("sigint")
#     # give the process time to quit
#     time.sleep(2.0)
#     print("exit")
#     sys.exit(0)

try:
    process = sys.exit(subprocess.call(argv))
except KeyboardInterrupt as ex:
    print("sigint keyboard interrupt")
    # process.send_signal(signal.SIGINT)
    # time.sleep(2.0)
