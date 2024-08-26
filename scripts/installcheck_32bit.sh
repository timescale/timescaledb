#!/usr/bin/env bash
set -xeu
set -o pipefail

# On 32-bit tests, the GitHub Action steps run inside a Docker container as
# root. Postgres can't run as root, so we switch to the postgres user using
# sudo, but this resets the ulimit, so we have to set the ulimit after that so
# that the core dumps can be collected. This is the purpose of this script.

ulimit -c unlimited
export LANG=C.UTF-8
whoami
make -k -C build installcheck IGNORES="${IGNORES}" \
  SKIPS="${SKIPS}" PSQL="${HOME}/${PG_INSTALL_DIR}/bin/psql" | tee installcheck.log
