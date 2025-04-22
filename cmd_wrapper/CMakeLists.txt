cmake_minimum_required(VERSION 3.0.2)

project(cmd_wrapper)

find_package(catkin REQUIRED COMPONENTS
  # roslint
)

set(ROSLINT_PYTHON_OPTS "--ignore=W503" "1>&2")
# roslint_python()

catkin_package()

install(PROGRAMS
  scripts/cmd_wrapper.py
  DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION}
)
