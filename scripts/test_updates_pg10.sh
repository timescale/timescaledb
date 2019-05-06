#!/usr/bin/env bash

set -e
set -o pipefail

SCRIPT_DIR=$(dirname $0)

TAGS="0.7.0-pg10 0.7.1-pg10 0.8.0-pg10 0.9.0-pg10 0.9.1-pg10 0.9.2-pg10 0.10.0-pg10 0.10.1-pg10 0.11.0-pg10 0.12.0-pg10 1.0.0-pg10 1.0.1-pg10 1.1.0-pg10 1.1.1-pg10 1.2.0-pg10 1.2.1-pg10 1.2.2-pg10 1.3.0-pg10"
TEST_VERSION="v2"

. ${SCRIPT_DIR}/test_updates.sh
