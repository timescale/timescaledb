#!/usr/bin/env bash
#
# Starting with version 0.5.0, TimescaleDB had support for most constraints,
# including foreign keys from the hypertable to another table. In order to test
# this, we updated the testing files to include a few of these constraints.
# However, since support in earlier versions was incomplete, we don't run to
# try upgrading from pre-0.5.0 since it is not guaranteed to work. Hence we
# use a different update test script.

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)

TAGS="0.5.0 0.6.0 0.6.1 0.7.0-pg9.6 0.7.1-pg9.6 0.8.0-pg9.6 0.9.0-pg9.6 0.9.1-pg9.6 0.9.2-pg9.6 0.10.0-pg9.6 0.10.1-pg9.6 0.11.0-pg9.6 0.12.0-pg9.6 1.0.0-pg9.6 1.0.1-pg9.6 1.1.0-pg9.6 1.1.1-pg9.6 1.2.0-pg9.6 1.2.1-pg9.6 1.2.2-pg9.6"
TEST_VERSION="v2"

. ${SCRIPT_DIR}/test_updates.sh
