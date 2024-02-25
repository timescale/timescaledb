#!/bin/bash

set -eu

SCRIPT_DIR=$(readlink -f "$(dirname $0)")
PG_MAJOR_VERSION=$(pg_config --version | awk '{print $2}' | awk -F. '{print $1}')

PG_EXTENSION_DIR=$(pg_config --sharedir)/extension
if [ "${CI:-false}" == true ]; then
  GIT_REF=${GIT_REF:-$(git rev-parse HEAD)}
else
  GIT_REF=$(git branch --show-current)
fi

BUILD_DIR="build_update_pg${PG_MAJOR_VERSION}"

VERSIONS=""
FAILED_VERSIONS=""

ALL_VERSIONS=$(git tag --sort=taggerdate | grep -P '^[2]\.[0-9]+\.[0-9]+$')
MAX_VERSION=$(grep '^downgrade_to_version ' version.config | awk '{ print $3 }')

# major version is always 2 atm
max_minor_version=$(echo "${MAX_VERSION}" | awk -F. '{print $2}')
max_patch_version=$(echo "${MAX_VERSION}" | awk -F. '{print $3}')

# Filter versions depending on the current postgres version
# Minimum version for valid update paths are as follows:
# PG 13 v7 2.1+
# PG 14 v8 2.5+
# PG 15 v8 2.9+
# PG 16 v8 2.13+
for version in ${ALL_VERSIONS}; do
  minor_version=$(echo "${version}" | awk -F. '{print $2}')
  patch_version=$(echo "${version}" | awk -F. '{print $3}')

  # skip versions that are newer than the max version
  # We might have a tag for a newer version defined already but the post release
  # adjustment have not been merged yet. So we want to skip those versions.
  if [ "${minor_version}" -gt "${max_minor_version}" ]; then
    continue
  elif [ "${minor_version}" -eq "${max_minor_version}" ] && [ "${patch_version}" -gt "${max_patch_version}" ]; then
    continue
  fi

  if [ "${minor_version}" -eq 0 ]; then
    # not part of any valid update path
    continue
  elif [ "${minor_version}" -le 4 ]; then
    continue
    # on <= 2.4 we need to run v7 version of the update test
    if [ "${PG_MAJOR_VERSION}" -le 13 ]; then
        VERSIONS="${VERSIONS} ${version}"
    fi
  elif [ "${minor_version}" -le 8 ]; then
    if [ "${PG_MAJOR_VERSION}" -le 14 ]; then
        VERSIONS="${VERSIONS} ${version}"
    fi
  elif [ "${minor_version}" -le 12 ]; then
    if [ "${PG_MAJOR_VERSION}" -le 15 ]; then
        VERSIONS="${VERSIONS} ${version}"
    fi
  else
    VERSIONS="${VERSIONS} ${version}"
  fi
done

FAIL_COUNT=0

if [ ! -d "${BUILD_DIR}" ]; then
  echo "Initializing build directory"
  BUILD_DIR="${BUILD_DIR}" ./bootstrap -DCMAKE_BUILD_TYPE=Release -DWARNINGS_AS_ERRORS=OFF -DASSERTIONS=ON -DLINTER=OFF -DGENERATE_DOWNGRADE_SCRIPT=ON -DREGRESS_CHECKS=OFF -DTAP_CHECKS=OFF
fi

for version in ${VERSIONS}; do
  if [ ! -f "${PG_EXTENSION_DIR}/timescaledb--${version}.sql" ]; then
    echo "Building ${version}"
    git checkout ${version}
    make -C "${BUILD_DIR}" -j4 > /dev/null
    sudo make -C "${BUILD_DIR}" install > /dev/null
    git checkout ${GIT_REF}
  fi
done

# We want to use the latest loader for all the tests so we build it last
git checkout ${GIT_REF}
make -C "${BUILD_DIR}" -j4
sudo make -C "${BUILD_DIR}" install

set +e

if [ -n "${VERSIONS}" ]; then
  for version in ${VERSIONS}; do
    ts_minor_version=$(echo "${version}" | awk -F. '{print $2}')

    if [ "${ts_minor_version}" -le 4 ]; then
      TEST_VERSION=v7
    else
      TEST_VERSION=v8
    fi

    if [ "${ts_minor_version}" -ge 10 ]; then
      TEST_REPAIR=true
    else
      TEST_REPAIR=false
    fi

    export TEST_VERSION TEST_REPAIR

    FROM_VERSION=${version} "${SCRIPT_DIR}/test_update_from_version.sh"
    return_code=$?
    if [ $return_code -ne 0 ]; then
      FAIL_COUNT=$((FAIL_COUNT + 1))
      FAILED_VERSIONS="${FAILED_VERSIONS} ${version}"
    fi
  done
fi

echo -e "\nUpdate test finished for ${VERSIONS}\n"

if [ $FAIL_COUNT -gt 0 ]; then
  echo -e "Failed versions: ${FAILED_VERSIONS}\n"
else
  echo -e "All tests succeeded.\n"
fi

exit $FAIL_COUNT

