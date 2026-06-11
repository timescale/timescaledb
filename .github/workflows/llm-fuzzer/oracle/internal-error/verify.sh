# This oracle just runs the reproducer script. The later workflow checks will
# detect the program logic errors: segfault, assertion failure, address
# sanitizer failure, internal program error (SQLSTATE XX000) and so on.
# The reproducer script for this oracle must trigger this class of error.
set -xeu

psql -v ON_ERROR_STOP=0 -f ~/llm-fuzzer-repro.sql
