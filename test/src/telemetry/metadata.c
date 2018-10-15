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
#include <utils/builtins.h>

#include "export.h"
#include "telemetry/metadata.h"

TS_FUNCTION_INFO_V1(ts_test_uuid);
TS_FUNCTION_INFO_V1(ts_test_exported_uuid);
TS_FUNCTION_INFO_V1(ts_test_install_timestamp);

Datum
ts_test_uuid(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(metadata_get_uuid());
}

Datum
ts_test_exported_uuid(PG_FUNCTION_ARGS)

{
	PG_RETURN_DATUM(metadata_get_exported_uuid());
}

Datum
ts_test_install_timestamp(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(metadata_get_install_timestamp());
}
