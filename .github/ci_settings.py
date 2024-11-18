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

PG14_EARLIEST = "14.0"
PG14_LATEST = "14.13"
PG14_ABI_MIN = "14.0"

PG15_EARLIEST = "15.0"
PG15_LATEST = "15.8"
PG15_ABI_MIN = "15.0"

PG16_EARLIEST = "16.0"
PG16_LATEST = "16.4"
PG16_ABI_MIN = "16.0"

PG17_EARLIEST = "17.0"
PG17_LATEST = "17.0"
PG17_ABI_MIN = "17.0"

PG_LATEST = [PG14_LATEST, PG15_LATEST, PG16_LATEST, PG17_LATEST]
