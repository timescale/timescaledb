#!/usr/bin/env bash
#
# Wrapper for the PostgreSQL isolationtest runner. It replaces
# the chunks IDs in the output of the tests by _X_X_. So, even
# if the IDs change, the tests will not fail.
##############################################################

set -e
set -u

ISOLATIONTEST=$1
shift

$ISOLATIONTEST "$@" | \
   sed -e 's!_[0-9]\{1,\}_[0-9]\{1,\}_chunk!_X_X_chunk!g'
 
