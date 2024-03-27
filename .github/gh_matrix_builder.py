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

# We hash the .github directory to understand whether our Postgres build cache
# can still be used, and the __pycache__ files interfere with that, so don't
# create them.
import sys

sys.dont_write_bytecode = True

import json
import os
import subprocess
from ci_settings import (
    PG13_EARLIEST,
    PG13_LATEST,
    PG14_EARLIEST,
    PG14_LATEST,
    PG15_EARLIEST,
    PG15_LATEST,
    PG16_EARLIEST,
    PG16_LATEST,
    PG_LATEST,
)

# github event type which is either push, pull_request or schedule
event_type = sys.argv[1]
pull_request = event_type == "pull_request"

m = {
    "include": [],
}


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
    base_config = dict(
        {
            "build_type": "Debug",
            "cc": "gcc",
            "clang": "clang-14",
            "coverage": True,
            "cxx": "g++",
            "extra_packages": "clang-14 llvm-14 llvm-14-dev llvm-14-tools",
            "ignored_tests": {
                "bgw_db_scheduler",
                "bgw_db_scheduler_fixed",
                "telemetry",
            },
            "name": "Debug",
            "os": "ubuntu-22.04",
            "pg_extra_args": "--enable-debug --enable-cassert --with-llvm LLVM_CONFIG=llvm-config-14",
            "pginstallcheck": True,
            "tsdb_build_args": "-DWARNINGS_AS_ERRORS=ON -DREQUIRE_ALL_TESTS=ON",
        }
    )
    base_config.update(overrides)
    return base_config


# We build this release configuration with WARNINGS_AS_ERRORS=ON to
# make sure that we can build with -Werrors even for release
# builds. This will capture some cases where warnings are generated
# for release builds but not for debug builds.
def build_release_config(overrides):
    base_config = build_debug_config({})
    release_config = dict(
        {
            "name": "Release",
            "build_type": "RelWithDebInfo",
            "tsdb_build_args": "-DWARNINGS_AS_ERRORS=ON -DREQUIRE_ALL_TESTS=ON",
            "coverage": False,
        }
    )
    base_config.update(release_config)
    base_config.update(overrides)
    return base_config


def build_without_telemetry(overrides):
    config = build_release_config({})
    config.update(
        {
            "name": "ReleaseWithoutTelemetry",
            "tsdb_build_args": config["tsdb_build_args"] + " -DUSE_TELEMETRY=OFF",
            "coverage": False,
        }
    )
    config.update(overrides)
    return config


def build_apache_config(overrides):
    base_config = build_debug_config({})
    apache_config = dict(
        {
            "name": "ApacheOnly",
            "build_type": "RelWithDebInfo",
            "tsdb_build_args": "-DWARNINGS_AS_ERRORS=ON -DREQUIRE_ALL_TESTS=ON -DAPACHE_ONLY=1",
            "coverage": False,
        }
    )
    base_config.update(apache_config)
    base_config.update(overrides)
    return base_config


def macos_config(overrides):
    base_config = dict(
        {
            "cc": "clang",
            "clang": "clang",
            "coverage": False,
            "cxx": "clang++",
            "extra_packages": "",
            "ignored_tests": {
                "bgw_db_scheduler",
                "bgw_db_scheduler_fixed",
                "bgw_launcher",
                "pg_dump",
                "remote_connection",
                "compressed_collation",
            },
            "os": "macos-13",
            "pg_extra_args": "--with-libraries=/usr/local/opt/openssl@3/lib --with-includes=/usr/local/opt/openssl@3/include --without-icu",
            "pginstallcheck": True,
            "tsdb_build_args": "-DASSERTIONS=ON -DREQUIRE_ALL_TESTS=ON -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl@3",
        }
    )
    base_config.update(overrides)
    return base_config


# common ignored tests for all scheduled pg15 tests
ignored_tests = {}

# common ignored tests for all non-scheduled pg15 tests (e.g. PRs)
if pull_request:
    ignored_tests = {
        "telemetry",
    }

# always test debug build on latest of all supported pg versions
m["include"].append(
    build_debug_config({"pg": PG13_LATEST, "ignored_tests": ignored_tests})
)

m["include"].append(
    build_debug_config({"pg": PG14_LATEST, "ignored_tests": ignored_tests})
)

m["include"].append(
    build_debug_config({"pg": PG15_LATEST, "ignored_tests": ignored_tests})
)

m["include"].append(
    build_debug_config({"pg": PG16_LATEST, "ignored_tests": ignored_tests})
)

# test timescaledb with release config on latest postgres release in MacOS
m["include"].append(
    build_release_config(
        macos_config({"pg": PG16_LATEST, "ignored_tests": ignored_tests})
    )
)

# Test latest postgres release without telemetry. Also run clang-tidy on it
# because it's the fastest one.
m["include"].append(
    build_without_telemetry(
        {
            "pg": PG16_LATEST,
            "cc": "clang-14",
            "cxx": "clang++-14",
            "ignored_tests": ignored_tests,
            "tsdb_build_args": "-DLINTER=ON -DWARNINGS_AS_ERRORS=ON",
        }
    )
)

# if this is not a pull request e.g. a scheduled run or a push
# to a specific branch like prerelease_test we add additional
# entries to the matrix
if not pull_request:
    # add debug test for first supported PG13 version
    pg13_debug_earliest = {
        "pg": PG13_EARLIEST,
        # The early releases don't build with llvm 14.
        "pg_extra_args": "--enable-debug --enable-cassert --without-llvm",
        "skipped_tests": {"001_extension"},
        "ignored_tests": {
            "transparent_decompress_chunk-13",
        },
        "tsdb_build_args": "-DWARNINGS_AS_ERRORS=ON -DASSERTIONS=ON -DPG_ISOLATION_REGRESS=OFF",
    }
    m["include"].append(build_debug_config(pg13_debug_earliest))

    # add debug test for first supported PG14 version
    m["include"].append(
        build_debug_config(
            {
                "pg": PG14_EARLIEST,
                # The early releases don't build with llvm 14.
                "pg_extra_args": "--enable-debug --enable-cassert --without-llvm",
                "ignored_tests": {"memoize"},
            }
        )
    )

    # add debug test for first supported PG15 version
    m["include"].append(
        build_debug_config({"pg": PG15_EARLIEST, "ignored_tests": ignored_tests})
    )

    # add debug test for first supported PG16 version
    m["include"].append(
        build_debug_config({"pg": PG16_EARLIEST, "ignored_tests": ignored_tests})
    )

    # add debug tests for timescaledb on latest postgres release in MacOS
    m["include"].append(
        build_debug_config(
            macos_config({"pg": PG15_LATEST, "ignored_tests": ignored_tests})
        )
    )

    m["include"].append(
        build_debug_config(
            macos_config({"pg": PG16_LATEST, "ignored_tests": ignored_tests})
        )
    )

    # add release test for latest pg releases
    m["include"].append(build_release_config({"pg": PG13_LATEST}))
    m["include"].append(build_release_config({"pg": PG14_LATEST}))
    m["include"].append(
        build_release_config({"pg": PG15_LATEST, "ignored_tests": ignored_tests})
    )
    m["include"].append(
        build_release_config({"pg": PG16_LATEST, "ignored_tests": ignored_tests})
    )

    # add apache only test for latest pg versions
    for PG_LATEST_VER in PG_LATEST:
        m["include"].append(build_apache_config({"pg": PG_LATEST_VER}))

    # to discover issues with upcoming releases we run CI against
    # the stable branches of supported PG releases
    m["include"].append(build_debug_config({"pg": 13, "snapshot": "snapshot"}))
    m["include"].append(
        build_debug_config(
            {
                "ignored_tests": {"memoize"},
                "pg": 14,
                "snapshot": "snapshot",
            }
        )
    )
    m["include"].append(
        build_debug_config(
            {
                "pg": 15,
                "snapshot": "snapshot",
                "ignored_tests": ignored_tests,
            }
        )
    )
    m["include"].append(
        build_debug_config(
            {
                "pg": 16,
                "snapshot": "snapshot",
                "ignored_tests": ignored_tests,
            }
        )
    )
elif len(sys.argv) > 2:
    # Check if we need to check for the flaky tests. Determine which test files
    # have been changed in the PR. The sql files might include other files that
    # change independently, and might be .in templates, so it's easier to look
    # at the output files. They are also the same for the isolation tests.
    p = subprocess.Popen(
        f"git diff --name-only {sys.argv[2]} -- '**expected/*.out'",
        stdout=subprocess.PIPE,
        shell=True,
    )
    (output, err) = p.communicate()
    p_status = p.wait()
    if p_status != 0:
        print(
            f'git diff failed: code {p_status}, output "{output}", stderr "{err}"',
            file=sys.stderr,
        )
        sys.exit(1)
    tests = set()
    test_count = 1
    for f in output.decode().split("\n"):
        print(f)
        if not f:
            continue
        test_count += 1
        if test_count > 10:
            print(
                f"too many ({test_count}) changed tests, won't run the flaky check",
                file=sys.stderr,
            )
            print("full list:", file=sys.stderr)
            print(output, file=sys.stderr)
            tests = set()
            break
        basename = os.path.basename(f)
        split = basename.split(".")
        name = split[0]
        ext = split[-1]
        if ext == "out":
            # Account for the version number.
            tests.add(name)
        else:
            # Should've been filtered out above.
            print(
                f"unknown extension '{ext}' for test output file '{f}'", file=sys.stderr
            )
            sys.exit(1)

    if tests:
        m["include"].append(
            build_debug_config(
                {
                    "coverage": False,
                    "installcheck_args": f'TESTS="{" ".join(list(tests) * 20)}"',
                    "name": "Flaky Check Debug",
                    "pg": PG14_LATEST,
                    "pginstallcheck": False,
                }
            )
        )

# generate command to set github action variable
with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
    print(str.format("matrix={0}", json.dumps(m, default=list)), file=output)
