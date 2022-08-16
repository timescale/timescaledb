#!/usr/bin/env python

#  This file and its contents are licensed under the Apache License 2.0.
#  Please see the included NOTICE for copyright information and
#  LICENSE-APACHE for a copy of the license.

# Python script to dynamically generate matrix for github action

# Since we want to run additional test configurations when triggered
# by a push to prerelease_test or by cron but github actions don't
# allow a dynamic matrix via yaml configuration, we generate the matrix
# with this python script. While we could always have the full matrix
# and have if checks on every step that would make the actual checks
# harder to browse because the workflow will have lots of entries and
# only by navigating into the individual jobs would it be visible
# if a job was actually run.

import json
import sys
from ci_settings import PG12_EARLIEST, PG12_LATEST, PG13_EARLIEST, PG13_LATEST, PG14_EARLIEST, PG14_LATEST

# github event type which is either push, pull_request or schedule
event_type = sys.argv[1]

m = {"include": [],}

# helper functions to generate matrix entries
# the release and apache config inherit from the
# debug config to reduce repetition
def build_debug_config(overrides):
  # llvm version and clang versions must match otherwise
  # there will be build errors this is true even when compiling
  # with gcc as clang is used to compile the llvm parts.
  #
  # Strictly speaking, WARNINGS_AS_ERRORS=ON is not needed here, but
  # we add it as a precaution. Intention is to have at least one
  # release and one debug build with WARNINGS_AS_ERRORS=ON so that we
  # capture warnings generated due to changes in the code base or the
  # compiler.
  base_config = dict({
    "name": "Debug",
    "build_type": "Debug",
    "pg_build_args": "--enable-debug --enable-cassert",
    "tsdb_build_args": "-DCODECOVERAGE=ON -DWARNINGS_AS_ERRORS=ON -DREQUIRE_ALL_TESTS=ON",
    "installcheck_args": "IGNORES='bgw_db_scheduler'",
    "coverage": True,
    "extra_packages": "clang-9 llvm-9 llvm-9-dev llvm-9-tools",
    "llvm_config": "llvm-config-9",
    "clang": "clang-9",
    "os": "ubuntu-20.04",
    "cc": "gcc",
    "cxx": "g++",
  })
  base_config.update(overrides)
  return base_config

# We build this release configuration with WARNINGS_AS_ERRORS=ON to
# make sure that we can build with -Werrors even for release
# builds. This will capture some cases where warnings are generated
# for release builds but not for debug builds.
def build_release_config(overrides):
  base_config = build_debug_config({})
  release_config = dict({
    "name": "Release",
    "build_type": "Release",
    "pg_build_args": "",
    "tsdb_build_args": "-DWARNINGS_AS_ERRORS=ON -DREQUIRE_ALL_TESTS=ON",
    "coverage": False,
  })
  base_config.update(release_config)
  base_config.update(overrides)
  return base_config

def build_without_telemetry(overrides):
  config = build_release_config({})
  config.update({
    'name': 'ReleaseWithoutTelemetry',
    "tsdb_build_args": config['tsdb_build_args'] + " -DUSE_TELEMETRY=OFF",
    "coverage": False,
  })
  config.update(overrides)
  return config

def build_apache_config(overrides):
  base_config = build_debug_config({})
  apache_config = dict({
    "name": "ApacheOnly",
    "build_type": "Release",
    "tsdb_build_args": "-DWARNINGS_AS_ERRORS=ON -DREQUIRE_ALL_TESTS=ON -DAPACHE_ONLY=1",
    "pg_build_args": "",
    "coverage": False,
  })
  base_config.update(apache_config)
  base_config.update(overrides)
  return base_config

def macos_config(overrides):
  base_config = dict({
    "pg": PG12_LATEST,
    "os": "macos-11",
    "cc": "clang",
    "cxx": "clang++",
    "clang": "clang",
    "pg_extra_args": "--with-libraries=/usr/local/opt/openssl/lib --with-includes=/usr/local/opt/openssl/include",
    "tsdb_build_args": "-DASSERTIONS=ON -DREQUIRE_ALL_TESTS=ON -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl",
    "llvm_config": "/usr/local/opt/llvm/bin/llvm-config",
    "coverage": False,
    "installcheck_args": "IGNORES='bgw_db_scheduler bgw_launcher pg_dump remote_connection compressed_collation'",
    "extra_packages": "",
  })
  base_config.update(overrides)
  return base_config

# always test debug build on latest of all supported pg versions
m["include"].append(build_debug_config({"pg":PG12_LATEST}))
m["include"].append(build_debug_config({"pg":PG13_LATEST}))
m["include"].append(build_debug_config({"pg":PG14_LATEST}))

m["include"].append(build_release_config(macos_config({})))

m["include"].append(build_without_telemetry({"pg":PG14_LATEST}))

# if this is not a pull request e.g. a scheduled run or a push
# to a specific branch like prerelease_test we add additional
# entries to the matrix
if event_type != "pull_request":

  # add debug test for first supported PG12 version
  # most of the IGNORES are the isolation tests because the output format has changed between versions
  # chunk_utils, telemetry and tablespace are skipped because of use after free bugs in postgres 12.0 which those tests hit
  pg12_debug_earliest = {
    "pg": PG12_EARLIEST,
    "installcheck_args": "SKIPS='chunk_utils tablespace telemetry' IGNORES='cluster-12 cagg_policy debug_notice dist_gapfill_pushdown-12'",
    "tsdb_build_args": "-DCODECOVERAGE=ON -DWARNINGS_AS_ERRORS=ON -DASSERTIONS=ON -DPG_ISOLATION_REGRESS=OFF",
  }
  m["include"].append(build_debug_config(pg12_debug_earliest))

  # add debug test for first supported PG13 version
  pg13_debug_earliest = {
    "pg": PG13_EARLIEST,
    "installcheck_args": "SKIPS='001_extension' IGNORES='dist_gapfill_pushdown-13'",
    "tsdb_build_args": "-DCODECOVERAGE=ON -DWARNINGS_AS_ERRORS=ON -DASSERTIONS=ON -DPG_ISOLATION_REGRESS=OFF",
  }
  m["include"].append(build_debug_config(pg13_debug_earliest))

  # add debug test for first supported PG14 version
  m["include"].append(build_debug_config({"pg": PG14_EARLIEST, "installcheck_args": "IGNORES='dist_gapfill_pushdown-14 memoize'"}))

  # add debug test for MacOS
  m["include"].append(build_debug_config(macos_config({})))

  # add release test for latest pg 12 and 13
  m["include"].append(build_release_config({"pg":PG12_LATEST}))
  m["include"].append(build_release_config({"pg":PG13_LATEST}))
  m["include"].append(build_release_config({"pg":PG14_LATEST}))

  # add apache only test for latest pg
  m["include"].append(build_apache_config({"pg":PG12_LATEST}))
  m["include"].append(build_apache_config({"pg":PG13_LATEST}))
  m["include"].append(build_apache_config({"pg":PG14_LATEST}))

  # to discover issues with upcoming releases we run CI against
  # the stable branches of supported PG releases
  m["include"].append(build_debug_config({"pg":12,"snapshot":"snapshot"}))
  m["include"].append(build_debug_config({"pg":13,"snapshot":"snapshot"}))
  m["include"].append(build_debug_config({"pg":14,"snapshot":"snapshot", "installcheck_args": "IGNORES='dist_gapfill_pushdown-14 memoize'"}))

# generate command to set github action variable
print(str.format("::set-output name=matrix::{0}",json.dumps(m)))

