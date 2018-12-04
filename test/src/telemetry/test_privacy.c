/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>

#include "compat.h"
#include "telemetry/telemetry.h"
#include "telemetry/uuid.h"

TS_FUNCTION_INFO_V1(ts_test_privacy);

Datum
ts_test_privacy(PG_FUNCTION_ARGS)
{
	/* This test should only run when timescaledb.telemetry_level=off */
	PG_RETURN_BOOL(ts_telemetry_main("", "", ""));
}
