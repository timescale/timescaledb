/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_DATE_TRUNC_H
#define TIMESCALEDB_DATE_TRUNC_H

#include <postgres.h>
#include <fmgr.h>

#include "export.h"

extern TSDLLEXPORT Datum ts_time_bucket_ng(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_time_bucket_ng_timestamp(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_time_bucket_ng_timestamptz(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_DATE_TRUNC_H */
