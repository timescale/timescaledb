/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */

#ifndef TIMESCALEDB_GAPFILL_H
#define TIMESCALEDB_GAPFILL_H

#include <postgres.h>
#include <fmgr.h>

#define GAPFILL_FUNCTION "time_bucket_gapfill"
#define GAPFILL_LOCF_FUNCTION "locf"
#define GAPFILL_INTERPOLATE_FUNCTION "interpolate"

extern Datum gapfill_marker(PG_FUNCTION_ARGS);

#endif							/* TIMESCALEDB_GAPFILL_H */
