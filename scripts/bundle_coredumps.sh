#!/bin/bash

TARGET=coredumps
COREDUMP_DIR=/var/lib/systemd/coredump

set -e

mkdir -p "$TARGET"

# get information from gdb
info=$(echo "info sharedlibrary" | coredumpctl gdb)

executable=$(echo "$info" | grep Executable | sed -e 's!^[^/]\+!!')

cp "$executable" "$TARGET"
cp ${COREDUMP_DIR}/* "$TARGET"

# copy libraries extracted from gdb info
echo "$info" | grep '^0x' | sed -e 's!^[^/]\+!!' | xargs -ILIB cp "LIB" "$TARGET"

