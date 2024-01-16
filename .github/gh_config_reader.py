#!/usr/bin/env python

#  This file and its contents are licensed under the Apache License 2.0.
#  Please see the included NOTICE for copyright information and
#  LICENSE-APACHE for a copy of the license.

# We hash the .github directory to understand whether our Postgres build cache
# can still be used, and the __pycache__ files interfere with that, so don't
# create them.
import sys

sys.dont_write_bytecode = True

import ci_settings
import json
import os

# generate commands to set github action variables
for key in dir(ci_settings):
    if not key.startswith("__"):
        value = getattr(ci_settings, key)
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            print(str.format("{0}={1}", key, json.dumps(value)), file=output)
