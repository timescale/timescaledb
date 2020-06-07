# Copyright (c) 2016-2020  Timescale, Inc. All Rights Reserved.
#
# This file is licensed under the Apache License, see LICENSE-APACHE
# at the top level directory of the timescaledb distribution.

# Python script to dynamically generate matrix for github action

# Since we want to run additional test configurations when triggered
# by a push to prerelease_test or by cron but github actions don't
# allow a dynamic matrix via yaml configuration we generate the matrix
# with a python script. While we could always have the full matrix
# and have if checks on every step that would make the actual checks
# harder to browse because the workflow will have lots of entries and
# only by navigating into the individual jobs would it be visible
# if a job was actually run.
# Additionally we set fail-fast to true for pull requests and false
# for other event types.

import json
import sys

# github event type which is either push, pull_request or schedule
event_type = sys.argv[1]

m = {"include": [],}

# helper functions to generate matrix entries
# the release and apache config inherit from the
# debug config to reduce repetition
def build_debug_config(overrides):
  # llvm version and clang versions must match otherwise
  # there will be build errors this is true even when compiling
  # with gcc as clang is used to compile the llvm parts
  base_config = dict({
    "name": "Debug",
    "build_type": "Debug",
    "pg_build_args": "--enable-debug --enable-cassert",
    "tsdb_build_args": "-DCODECOVERAGE=ON",
    "coverage": True,
    "llvm_config": "/usr/bin/llvm-config-9",
    "clang": "clang-9",
    "os": "ubuntu-18.04",
    "cc": "gcc",
  })
  base_config.update(overrides)
  return base_config

def build_release_config(overrides):
  base_config = build_debug_config({})
  release_config = dict({
    "name": "Release",
    "build_type": "Release",
    "pg_build_args": "",
    "tsdb_build_args": "",
    "coverage": False,
  })
  base_config.update(release_config)
  base_config.update(overrides)
  return base_config

def build_apache_config(overrides):
  base_config = build_debug_config({})
  apache_config = dict({
    "name": "ApacheOnly",
    "build_type": "Release",
    "tsdb_build_args": "-DAPACHE_ONLY=1",
    "pg_build_args": "",
    "coverage": False,
  })
  base_config.update(apache_config)
  base_config.update(overrides)
  return base_config

# always test debug build on latest pg11 and latest pg12
m["include"].append(build_debug_config({"pg":"11.8"}))
m["include"].append(build_debug_config({"pg":"12.3"}))

# if this is not a pull request e.g. a scheduled run or a push
# to a specific branch like prerelease_test we add additional
# entries to the matrix
if event_type != "pull_request":

  # add debug test for first supported PG11 version
  # there is a problem when building PG 11.0 on ubuntu
  # with llvm-9 so we use llvm-8 instead
  pg11_debug_earliest = {
    "pg": "11.0",
    "llvm_config": "/usr/bin/llvm-config-8",
    "clang": "clang-8",
    "extra_packages": "llvm-8 llvm-8-dev llvm-8-tools",
  }
  m["include"].append(build_debug_config(pg11_debug_earliest))

  # add debug test for first supported PG12 version
  m["include"].append(build_debug_config({"pg":"12.0"}))

  # add release test for latest pg11 and latest pg12
  m["include"].append(build_release_config({"pg":"11.8"}))
  m["include"].append(build_release_config({"pg":"12.3"}))

  # add apache only test for latest pg11 and latest pg 12
  m["include"].append(build_apache_config({"pg":"11.8"}))
  m["include"].append(build_apache_config({"pg":"12.3"}))


# generate command to set github action variable
print(str.format("::set-output name=matrix::{0}",json.dumps(m)))
print(str.format("::set-output name=fail-fast::{0}",json.dumps(event_type == "pull_request")))

