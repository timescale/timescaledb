#!/bin/bash

SCRIPTDIR=$(cd "$(dirname $0)" || exit; pwd)
BASEDIR=$(dirname $SCRIPTDIR)

find $BASEDIR -name CMakeLists.txt  -exec cmake-format -i {} +
find $BASEDIR/src $BASEDIR/test $BASEDIR/tsl -name '*.cmake' -exec cmake-format -i {} +
