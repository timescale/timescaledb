#!/usr/bin/env python

#  This file and its contents are licensed under the Apache License 2.0.
#  Please see the included NOTICE for copyright information and
#  LICENSE-APACHE for a copy of the license.

import ci_settings
import json
import os

# generate commands to set github action variables
for key in dir(ci_settings):
    if not key.startswith("__"):
        value = getattr(ci_settings, key)
        with open(os.environ["GITHUB_OUTPUT"], "a") as output:
            print(str.format("{0}={1}", key, json.dumps(value)), file=output)
