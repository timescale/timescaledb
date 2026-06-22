#!/usr/bin/env python

#  This file and its contents are licensed under the Apache License 2.0.
#  Please see the included NOTICE for copyright information and
#  LICENSE-APACHE for a copy of the license.

# Common settings for our CI jobs
#
# EARLIEST is the minimum postgres version required when building from source
# LATEST is the maximum postgres version that is supported
# ABI_MIN is the minimum postgres version required when the extension was build against LATEST
#

PG16_EARLIEST = "16.6"
PG16_LATEST = "16.14"
PG16_LATEST_WIN = "16.14"
PG16_ABI_MIN = "16.6"

PG17_EARLIEST = "17.2"
PG17_LATEST = "17.10"
PG17_LATEST_WIN = "17.10"
PG17_ABI_MIN = "17.2"

PG18_EARLIEST = "18.0"
PG18_LATEST = "18.4"
PG18_LATEST_WIN = "18.4"
PG18_ABI_MIN = "18.0"

PG_LATEST = [PG16_LATEST, PG17_LATEST, PG18_LATEST]
PG_LATEST_ONLY = [PG17_LATEST]
