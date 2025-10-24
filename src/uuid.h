/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>
#include <utils/uuid.h>

extern pg_uuid_t *ts_uuid_create(void);
extern pg_uuid_t *ts_create_uuid_v7_from_unixtime_us(int64 unixtime_us, bool boundary,
													 bool set_version);
extern TSDLLEXPORT pg_uuid_t *ts_create_uuid_v7_from_timestamptz(TimestampTz ts, bool boundary);
extern bool ts_uuid_v7_extract_unixtime(const pg_uuid_t *uuid, uint64 *unixtime_ms,
										uint16 *extra_us);
