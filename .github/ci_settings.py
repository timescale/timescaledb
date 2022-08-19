#!/usr/bin/env python

#  This file and its contents are licensed under the Apache License 2.0.
#  Please see the included NOTICE for copyright information and
#  LICENSE-APACHE for a copy of the license.

# Common settings for our CI jobs
#
# EARLIEST is the minimum postgres version required when building from source
# LATEST is the maximum postgres version that is supported
# ABI_MIN is the minimum postgres version required when the extension was build against LATEST

PG12_EARLIEST = "12.0"
PG12_LATEST = "12.12"
PG12_ABI_MIN = "12.8"

PG13_EARLIEST = "13.2"
PG13_LATEST = "13.8"
PG13_ABI_MIN = "13.5"

PG14_EARLIEST = "14.0"
PG14_LATEST = "14.5"
PG14_ABI_MIN = "14.0"

PG_LATEST = [PG12_LATEST, PG13_LATEST, PG14_LATEST]
