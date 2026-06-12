#!/usr/bin/env bash

# This oracle runs the reproducer script with no additional checks. The later
# workflow checks will detect the program logic errors: segfault, assertion
# failure, address sanitizer failure, internal program error (SQLSTATE XX000)
# and so on. The admissible bug must manifest as this class of error.
set -xeu

psql -v ON_ERROR_STOP=0 -f ~/llm-fuzzer-repro.sql
