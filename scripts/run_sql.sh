 #!/bin/bash

# To avoid pw writing, add localhost:5432:*:postgres:test to ~/.pgpass
set -u
set -e
set -x

PWD=`pwd`
DIR=`dirname $0`

export PGUSER=${PGUSER:-postgres}
export PGHOST=${PGHOST:-localhost}
export PGDATABASE=${PGDATABASE:-iobeam}

psql -v ON_ERROR_STOP=1 -f $DIR/sql/$1
