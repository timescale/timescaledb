#!/bin/bash

SCRIPTDIR=$(cd "$(dirname $0)" || exit; pwd)
BASEDIR=$(dirname $SCRIPTDIR)

CMAKE_FORMAT=${CMAKE_FORMAT:-cmake-format}

find $BASEDIR -name CMakeLists.txt -print0 \
    | xargs -0 -n 4 -P 0 $CMAKE_FORMAT -i
find $BASEDIR/src $BASEDIR/test $BASEDIR/tsl -name '*.cmake' -print0 \
    | xargs -0 -n 4 -P 0 $CMAKE_FORMAT -i
