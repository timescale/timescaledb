#!/bin/bash

SCRIPTDIR=$(cd "$(dirname $0)" || exit; pwd)
BASEDIR=$(dirname $SCRIPTDIR)

PERLTIDY=${PERLTIDY:-perltidy}
PERLTIDY_CONFIG=${PERLTIDY_CONFIG:-$BASEDIR/.perltidyrc}

if [ ! -f "$PERLTIDY_CONFIG" ]; then
   echo "perltidy config '$PERLTIDY_CONFIG' not found"
   exit 1
fi

find "$BASEDIR"/test -name '*.p[lm]' -exec "$PERLTIDY" --profile="$PERLTIDY_CONFIG" -b -bext=/ {} +
find "$BASEDIR"/tsl/test -name '*.p[lm]' -exec "$PERLTIDY" --profile="$PERLTIDY_CONFIG" -b -bext=/ {} +

