#!/bin/bash

set -e

SCRIPT_DIR=$(dirname $0)
PG_MAJOR_VERSION=$(pg_config --version | awk '{print $2}' | awk -F. '{print $1}')

PG_EXTENSION_DIR=$(pg_config --sharedir)/extension
if [ "${CI:-false}" == true ]; then
  GIT_REF=${GIT_REF:-$(git rev-parse HEAD)}
else
  GIT_REF=$(git branch --show-current)
fi


BUILD_DIR="build_update_pg${PG_MAJOR_VERSION}"

CURRENT_VERSION=$(grep '^version ' version.config | awk '{ print $3 }')
PREV_VERSION=$(grep '^downgrade_to_version ' version.config | awk '{ print $3 }')

if [ ! -d "${BUILD_DIR}" ]; then
  echo "Initializing build directory"
	BUILD_DIR="${BUILD_DIR}" ./bootstrap -DCMAKE_BUILD_TYPE=Release -DWARNINGS_AS_ERRORS=OFF -DASSERTIONS=ON -DLINTER=ON -DGENERATE_DOWNGRADE_SCRIPT=ON -DREGRESS_CHECKS=OFF -DTAP_CHECKS=OFF
fi

if [ ! -f "${PG_EXTENSION_DIR}/timescaledb--${PREV_VERSION}.sql" ]; then
  echo "Building ${PREV_VERSION}"
  git checkout ${PREV_VERSION}
  make -C "${BUILD_DIR}" -j4 > /dev/null
  sudo make -C "${BUILD_DIR}" install > /dev/null
  git checkout ${GIT_REF}
fi

# We want to use the latest loader for all the tests so we build it last
make -C "${BUILD_DIR}" -j4
sudo make -C "${BUILD_DIR}" install > /dev/null

set +e

FROM_VERSION=${CURRENT_VERSION} TO_VERSION=${PREV_VERSION} TEST_REPAIR=false "${SCRIPT_DIR}/test_update_from_version.sh"
return_code=$?
if [ $return_code -ne 0 ]; then
  echo -e "\nFailed downgrade from ${CURRENT_VERSION} to ${PREV_VERSION}\n"
  exit 1
fi

