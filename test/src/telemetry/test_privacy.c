#include <string.h>
#include <unistd.h>
#include <postgres.h>
#include <fmgr.h>
#include <miscadmin.h>

#include "compat.h"
#include "telemetry/telemetry.h"
#include "telemetry/uuid.h"

TS_FUNCTION_INFO_V1(test_privacy);

Datum
test_privacy(PG_FUNCTION_ARGS)
{
	/* This test should only run when timescaledb.telemetry_level=off */
	telemetry_main("", "", "");
	PG_RETURN_NULL();
}
