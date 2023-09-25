#!/usr/bin/env python3

import sys
import re


def remove_comments(lines):
    code_lines = (line for line in lines if not line.startswith("--"))
    return "\n".join(code_lines)


def unsafe_catalog_modification(s):
    """catalog tables reside in _timescaledb_catalog, _timescaledb_internal and _timescaledb_config.
    It is unsafe to simply ADD or DROP columns from catalog tables"""
    matches = re.search(
        r"ALTER\s*TABLE\s*(_timescaledb_catalog|_timescaledb_internal|_timescaledb_config)[\s\S]+(DROP|ADD)\s+COLUMN",
        s,
        flags=re.IGNORECASE,
    )
    if matches:
        return True
    return False


def uses_timescaledb_internal_schema(s):
    """Functions must not be created in the _timescaledb_internal schema anymore but in _timescaledb_functions instead"""
    # OR REPLACE is not needed because pgspot will complain, and the correct way is
    # to simply CREATE the function.
    matches = re.search(
        r"CREATE\s+(FUNCTION|PROCEDURE)\s+_timescaledb_internal",
        s,
        flags=re.IGNORECASE,
    )
    if matches:
        return True
    return False


def main():
    # Open latest-dev.sql
    latest_dev = sys.argv[1]
    unsafe_modification = False
    wrong_schema = False
    contents = ""

    with open(latest_dev, "r", encoding="utf-8") as ldev:
        # remove comments, we don't want to confuse them with actual commands
        lines = ldev.readlines()
        contents = remove_comments(lines)
        unsafe_modification = unsafe_catalog_modification(contents)
        wrong_schema = uses_timescaledb_internal_schema(contents)
    if unsafe_modification:
        print(
            """
ERROR: Attempting to alter timescaledb catalog tables without rebuilding the table.
Rebuilding catalog tables is required to ensure consistent attribute numbers across versions."""
        )
    if wrong_schema:
        print(
            """
ERROR: Attempting to create function in the _timescaledb_internal schema, which has been deprecated
for security reasons. Functions must be created in the _timescaledb_functions schema instead."""
        )
    if unsafe_modification or wrong_schema:
        sys.exit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
