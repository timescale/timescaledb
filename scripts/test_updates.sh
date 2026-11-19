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
MAX_VERSION=$(grep '^previous_version ' version.config | awk '{ print $3 }')

# major version is always 2 atm
max_minor_version=$(echo "${MAX_VERSION}" | awk -F. '{print $2}')
max_patch_version=$(echo "${MAX_VERSION}" | awk -F. '{print $3}')

# Filter versions depending on the current postgres version
# Minimum version for valid update paths are as follows:
# PG 16 v8 2.13+
# PG 17 v8 2.17+
# PG 18 v8 2.23+
for version in ${ALL_VERSIONS}; do
  minor_version=$(echo "${version}" | awk -F. '{print $2}')
  patch_version=$(echo "${version}" | awk -F. '{print $3}')

  # 2.28.4 has broken update scripts and was replaced by 2.28.5
  if [ "${version}" = "2.28.4" ]; then
    continue
  fi

  # 2.31.1 has broken update scripts and was replaced by 2.31.2
  if [ "${version}" = "2.31.1" ]; then
    continue
  fi

  # skip versions that are newer than the max version
  # We might have a tag for a newer version defined already but the post release
  # adjustment have not been merged yet. So we want to skip those versions.
  if [ "${minor_version}" -gt "${max_minor_version}" ]; then
    continue
  elif [ "${minor_version}" -eq "${max_minor_version}" ] && [ "${patch_version}" -gt "${max_patch_version}" ]; then
    continue
  fi

  if [ "${minor_version}" -le 12 ]; then
    # not part of any valid update path
    continue
  elif [ "${minor_version}" -le 16 ]; then
    if [ "${PG_MAJOR_VERSION}" -le 16 ]; then
        VERSIONS="${VERSIONS} ${version}"
    fi
  elif [ "${minor_version}" -le 22 ]; then
    if [ "${PG_MAJOR_VERSION}" -le 17 ]; then
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

# Released packages are much faster to get than building every old version from
# source. The packages for the current Ubuntu release don't go back far enough,
# so we take them from an older release that has all versions. Only the
# versioned library and SQL files are copied, which only need libc and libssl,
# so this works on newer distributions too. It is off by default outside CI
# because a local Postgres may be built differently from the packaged one.
FETCH_PACKAGES=${FETCH_PACKAGES:-${CI:-false}}
FETCH_PACKAGES_REQUESTED=${FETCH_PACKAGES}
SOURCE_BUILDS=""
PACKAGE_DISTRO=${PACKAGE_DISTRO:-jammy}
PACKAGE_REPO=https://packagecloud.io/timescale/timescaledb/ubuntu
PG_PKGLIB_DIR=$(pg_config --pkglibdir)

case "$(uname -m)" in
  x86_64) PACKAGE_ARCH=amd64 ;;
  aarch64) PACKAGE_ARCH=arm64 ;;
  *) FETCH_PACKAGES=false ;;
esac

if [ "${FETCH_PACKAGES}" == true ]; then
  PACKAGE_TMP=$(mktemp -d)
  trap 'rm -rf "${PACKAGE_TMP}"' EXIT
  if ! curl -sfL --retry 5 "${PACKAGE_REPO}/dists/${PACKAGE_DISTRO}/main/binary-${PACKAGE_ARCH}/Packages.gz" \
      -o "${PACKAGE_TMP}/Packages.gz" || ! gunzip "${PACKAGE_TMP}/Packages.gz"; then
    echo "Could not fetch package index, building from source instead"
    FETCH_PACKAGES=false
  fi
fi

fetch_package() {
  local version=$1
  local name="timescaledb-2-${version}-postgresql-${PG_MAJOR_VERSION}"
  local dir="${PACKAGE_TMP}/${version}"
  local entry file sha

  # The index can list a package more than once, take the last entry
  entry=$(awk -v RS= -v FS='\n' -v line="Package: ${name}" '{ for (i = 1; i <= NF; i++) if ($i == line) e = $0 } END { print e }' "${PACKAGE_TMP}/Packages")
  file=$(awk '/^Filename:/ { print $2 }' <<<"${entry}")
  sha=$(awk '/^SHA256:/ { print $2 }' <<<"${entry}")
  if [ -z "${file}" ] || [ -z "${sha}" ]; then
    return 1
  fi

  mkdir -p "${dir}"
  curl -sfL --retry 5 "${PACKAGE_REPO}/${file}" -o "${dir}/package.deb" || return 1
  echo "${sha}  ${dir}/package.deb" | sha256sum -c --quiet || return 1
  (cd "${dir}" && ar x package.deb && tar -xf data.tar.*) || return 1

  # Leave out the loader and control file, those come from the current build
  local lib="${dir}/usr/lib/postgresql/${PG_MAJOR_VERSION}/lib"
  local ext="${dir}/usr/share/postgresql/${PG_MAJOR_VERSION}/extension"

  # The package libraries are built on an older distribution, check that the
  # libraries they need are still around here
  if ldd "${lib}/timescaledb-${version}.so" "${lib}/timescaledb-tsl-${version}.so" 2>/dev/null | grep -q 'not found'; then
    echo "Missing libraries for the ${version} package"
    return 1
  fi

  sudo cp "${lib}/timescaledb-${version}.so" "${lib}/timescaledb-tsl-${version}.so" "${PG_PKGLIB_DIR}/" || return 1
  sudo cp "${ext}"/timescaledb--*.sql "${PG_EXTENSION_DIR}/" || return 1
  rm -rf "${dir}"
}

for version in ${VERSIONS}; do
  if [ ! -f "${PG_EXTENSION_DIR}/timescaledb--${version}.sql" ]; then
    if [ "${FETCH_PACKAGES}" == true ]; then
      echo "Fetching ${version} package"
      if fetch_package "${version}"; then
        continue
      fi
      echo "No usable package for ${version}"
    fi
    echo "Building ${version}"
    SOURCE_BUILDS="${SOURCE_BUILDS} ${version}"
    git checkout ${version}
    make -C "${BUILD_DIR}" -j "$(getconf _NPROCESSORS_ONLN)" > /dev/null
    sudo make -C "${BUILD_DIR}" install > /dev/null
    git checkout ${GIT_REF}
  fi
done

# Falling back to source builds still works but is slow, so make it visible
if [ "${FETCH_PACKAGES_REQUESTED}" == true ] && [ -n "${SOURCE_BUILDS}" ]; then
  echo "::warning::Built old versions from source instead of using packages:${SOURCE_BUILDS}"
fi

# We want to use the latest loader for all the tests so we build it last
git checkout ${GIT_REF}
make -C "${BUILD_DIR}" -j "$(getconf _NPROCESSORS_ONLN)"
sudo make -C "${BUILD_DIR}" install

set +e

test_version_for() {
  local minor
  minor=$(echo "$1" | awk -F. '{print $2}')

  if [ "${minor}" -ge 26 ]; then
    echo v12
  elif [ "${minor}" -ge 22 ]; then
    echo v11
  elif [ "${minor}" -ge 20 ]; then
    echo v10
  elif [ "${minor}" -ge 16 ]; then
    echo v9
  else
    echo v8
  fi
}

# Every update test runs its own cluster, so they can run in parallel. Each
# test gets a fixed port, since picking free ports while other tests start
# their clusters can hand out the same port twice. The output of each test
# goes to its own log file, which is only printed when the test fails.
UPDATE_TEST_JOBS=${UPDATE_TEST_JOBS:-$(getconf _NPROCESSORS_ONLN)}
UPDATE_TEST_BASE_PORT=${UPDATE_TEST_BASE_PORT:-55432}
# A hanging test would otherwise stall the run without any output
UPDATE_TEST_TIMEOUT=${UPDATE_TEST_TIMEOUT:-30m}
LOG_DIR=update_test/logs
mkdir -p "${LOG_DIR}"

log_file() {
  echo "${LOG_DIR}/${1}_${UPDATE_MODE:-direct}.log"
}

run_update_test() {
  local version=$1 port=$2
  local log start rc
  log=$(log_file "${version}")
  start=${SECONDS}

  FROM_VERSION=${version} TEST_VERSION=$(test_version_for "${version}") PGPORT=${port} \
    timeout --kill-after=1m "${UPDATE_TEST_TIMEOUT}" \
    "${SCRIPT_DIR}/test_update_from_version.sh" > "${log}" 2>&1
  rc=$?
  echo "${rc}" > "${log%.log}.rc"

  # The error lines show up as annotations on the GitHub run summary
  if [ "${rc}" -eq 0 ]; then
    echo "${version}: ok ($((SECONDS - start))s)"
  elif [ "${rc}" -eq 124 ] || [ "${rc}" -eq 137 ]; then
    echo "${version}: TIMED OUT ($((SECONDS - start))s)"
    echo "::error::Update test from ${version} timed out after ${UPDATE_TEST_TIMEOUT}"
  else
    echo "${version}: FAILED ($((SECONDS - start))s)"
    echo "::error::Update test from ${version} failed"
  fi
  grep '^Locks held' "${log}"
}

echo -e "\nRunning update tests with ${UPDATE_TEST_JOBS} jobs\n"

port=${UPDATE_TEST_BASE_PORT}
for version in ${VERSIONS}; do
  while [ "$(jobs -rp | wc -l)" -ge "${UPDATE_TEST_JOBS}" ]; do
    wait -n
  done
  log=$(log_file "${version}")
  rm -f "${log%.log}.rc"
  run_update_test "${version}" "${port}" &
  port=$((port + 1))
done
wait

# A missing result file means the test died before it could write one
for version in ${VERSIONS}; do
  log=$(log_file "${version}")
  if [ "$(cat "${log%.log}.rc" 2>/dev/null)" != 0 ]; then
    FAIL_COUNT=$((FAIL_COUNT + 1))
    FAILED_VERSIONS="${FAILED_VERSIONS} ${version}"
    echo -e "\n=== Log for ${version} ===\n"
    cat "${log}"
  fi
done

echo -e "\nUpdate test finished for ${VERSIONS}\n"

if [ $FAIL_COUNT -gt 0 ]; then
  echo -e "Failed versions: ${FAILED_VERSIONS}\n"
  echo -e "Postgres errors:\n"
  find update_test -name postgres.log -exec grep ERROR {} \;
else
  echo -e "All tests succeeded.\n"
fi

exit $FAIL_COUNT
