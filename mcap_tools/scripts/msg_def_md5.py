#!/usr/bin/env python
"""
Copyright (c) Lucas Walter 2024

Get the md5sum from a message definition as stored in connection header
"""
import hashlib

from roslib.message import get_message_class


def message_definition_to_md5(msg_type: str, full_def: str, verbose=False):
    clean_def = ""
    for line in full_def.splitlines():
        if line.startswith("#"):  # or line.startswith("="):
            continue
        # print(line)
        line = line.split("#")[0].rstrip()
        # replace multiple whitespace with one space
        line = ' '.join(line.split())
        # oddly 'uint8 FOO=1' become 'uint8 FOO = 1', so restore it
        line = line.replace(" = ", "=")
        if line == "":
            continue
        clean_def += line + "\n"
    # strip final newline
    clean_def = clean_def.rstrip()  # [:-1]
    if verbose:
        print(clean_def)

    sections = clean_def.split("================================================================================\n")

    sub_messages = {}
    sub_messages[msg_type] = sections[0].rstrip()
    if verbose:
        print("definition without comments:")
    for section in sections[1:]:
        lines = section.splitlines()
        line0 = lines[0]
        if not line0.startswith("MSG: "):
            raise Exception(f"bad section {section} -> {line0} doesn't start with 'MSG: '")
        # TODO(lucasw) the full text definition doesn't always have the full message types
        # e.g. instead of geometry_msgs/Vector3 it just has Vector3, so if the same message were
        # to contain two different Vector3 types we wouldn't know which one to use
        section_type = line0.split(" ")[1]  # .split("/")[1]
        body = "\n".join(lines[1:]).rstrip()
        sub_messages[section_type] = body

        # print(repr(section))
        # print(section)
        # print("---")

    if verbose:
        print(f"\nsub messages: {sub_messages.keys()}\n")

    hashed = {}
    old_hashed_len = -1
    # expect to process at least one definition every pass
    while len(hashed.keys()) > old_hashed_len:
        old_hashed_len = len(hashed.keys())
        for name, msg in sub_messages.items():
            pkg_name = name.split("/")[0]
            if name in hashed:
                continue
            do_hash = True
            field_def = ""
            for line in msg.splitlines():
                raw_field_type, field_name = line.split()
                # TODO(lucasw) why are some field types retain the package name, and others don't?
                # Odometry has geometry_msgs/PoseWithCovariance and just Pose in places
                # leave array characters alone, could be [] [C] where C is a constant
                # -> the package name is left out when inside a message of the same package?
                # No that's not true, I see Header by itself
                field_type = raw_field_type.split("[")[0]
                # leave array characters alone, could be [] [C] where C is a constant

                base_types = ["bool",
                              "byte",
                              "int8", "int16", "int32", "int64",
                              "uint8", "uint16", "uint32", "uint64",
                              "float32", "float64",
                              "time", "duration",
                              "string",
                              ]
                if field_type not in base_types:
                    # TODO(lucasw) are there other special message types- or is it anything in std_msgs?
                    if field_type == "Header":
                        full_field_type = "std_msgs/Header"
                    elif "/" not in field_type:
                        full_field_type = pkg_name + "/" + field_type
                    else:
                        full_field_type = field_type
                    if full_field_type not in hashed:
                        if verbose:
                            print(f"can't find {full_field_type}, loop until no progress")
                        do_hash = False
                        break
                    line = line.replace(raw_field_type, hashed[full_field_type])
                field_def += line + "\n"
            if not do_hash:
                continue
            field_def = field_def.rstrip()
            if verbose:
                print(f"getting hash {name}: {repr(field_def)}")
            msg_hash = hashlib.md5()
            msg_hash.update(field_def.encode())
            hashed[name] = msg_hash.hexdigest()

    if verbose:
        print("<<<<<<<<<<<<\n")
        for k, v in hashed.items():
            print(f"{k}: {v}")
    return hashed[msg_type]


def test_messages():
    """
    run pytest msg_def_md5.py -rP
    """
    msgs = ["std_msgs/Header",
            "rosgraph_msgs/Log",
            "nav_msgs/Odometry",
            "sensor_msgs/CameraInfo",
            "tf2_msgs/TFMessage",
            "vision_msgs/Detection3DArray",
            ]
    for msg_type in msgs:
        msg_class = get_message_class(msg_type)
        assert (msg_class._type == msg_type)
        print(f"source message type: '{msg_type}'\n{msg_class._md5sum} <- stored md5")
        computed_md5 = message_definition_to_md5(msg_type, msg_class._full_text, verbose=False)
        print(f"{computed_md5} <- computed md5")
        assert (computed_md5 == msg_class._md5sum)

    if True:
        msg_hash = hashlib.md5()
        # rosgraph_msgs/Log
        text = "byte DEBUG=1\nbyte INFO=2\nbyte WARN=4\nbyte ERROR=8\nbyte FATAL=16\n"
        text += "2176decaecbce78abc3b96ef049fabed header\n"
        text += "byte level\nstring name\nstring msg\nstring file\nstring function\nuint32 line\nstring[] topics"
        msg_hash.update(text.encode())
        assert (msg_hash.hexdigest() == "acffd30cd6b6de30f120938c17c593fb")


def main():
    import sys

    msg_type = sys.argv[1]
    msg_class = get_message_class(msg_type)

    full_def = msg_class._full_text
    # print(full_def)
    print(f"source message type: '{msg_type}'\n{msg_class._md5sum} <- stored md5")
    md5 = message_definition_to_md5(msg_type, full_def, verbose=False)
    print(f"{md5} <- computed md5")
    assert (md5 == msg_class._md5sum)


if __name__ == "__main__":
    main()
