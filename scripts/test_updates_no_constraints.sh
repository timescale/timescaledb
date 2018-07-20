#!/bin/bash
#
# Prior to version 0.5.0, TimescaleDB did not fully support most constraints
# so we don't include update tests that test constraint support in pre-0.5.0
# versions.

set -e
set -o pipefail

TAGS="0.1.0 0.2.0 0.3.0 0.4.0 0.4.1 0.4.2"

for tag in ${TAGS};
do
    UPDATE_FROM_TAG=${tag} TEST_VERSION="v1" $(dirname $0)/test_update_from_tag.sh
done
