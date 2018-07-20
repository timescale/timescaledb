#!/bin/bash
#
# Starting with version 0.5.0, TimescaleDB had support for most constraints,
# including foreign keys from the hypertable to another table. In order to test
# this, we updated the testing files to include a few of these constraints.
# However, since support in earlier versions was incomplete, we don't run to
# try upgrading from pre-0.5.0 since it is not guaranteed to work. Hence we
# use a different update test script.

set -e
set -o pipefail

TAGS="0.5.0 0.6.0 0.6.1 0.7.0-pg9.6 0.7.1-pg9.6 0.8.0-pg9.6 0.9.0-pg9.6 0.9.1-pg9.6 0.9.2-pg9.6 0.10.0-pg9.6 0.10.1-pg9.6"

for tag in ${TAGS};
do
    UPDATE_FROM_TAG=${tag} TEST_VERSION="v2" $(dirname $0)/test_update_from_tag.sh
done
