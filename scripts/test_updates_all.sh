#!/bin/bash

set -e
set -o pipefail

V1_TAGS="0.1.0 0.2.0 0.3.0 0.4.0 0.4.1 0.4.2"
V2_TAGS="0.5.0 0.6.0 0.6.1 0.7.0-pg9.6 0.7.1-pg9.6 0.8.0-pg9.6"

for tag in ${V1_TAGS};
do
    UPDATE_FROM_TAG=${tag} TEST_VERSION="v1" $(dirname $0)/test_update_from_tag.sh
done

for tag in ${V2_TAGS};
do
    UPDATE_FROM_TAG=${tag} TEST_VERSION="v2" $(dirname $0)/test_update_from_tag.sh
done
