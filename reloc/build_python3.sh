#!/bin/bash -e

TARGET=build/release/scylla-python3.tar.gz

if [ -f "$TARGET" ]; then
    rm "$TARGET"
fi

PACKAGES="python3-PyYAML python3-urwid python3-pyparsing python3-requests python3-pyudev python3-setuptools"
./scripts/create-relocatable-python.py --output $TARGET $PACKAGES
