/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>

#include "compat/compat.h"
#include "telemetry/telemetry.h"
#include "uuid.h"

TS_FUNCTION_INFO_V1(ts_test_privacy);

Datum
ts_test_privacy(PG_FUNCTION_ARGS)
{
	/* This test should only run when timescaledb.telemetry_level=off */
	PG_RETURN_BOOL(ts_telemetry_main("", "", ""));
}
