/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "utils.h"
#include <catalog/pg_type.h>
#include <datatype/timestamp.h>
#include <pgtime.h>

#include "export.h"
#include <utils/timestamp.h>

/*
 * Represents a chunk time range with start and end boundaries.
 * Used for calendar-aligned chunking calculations.
 */
typedef struct ChunkRange
{
	Oid type;
	Datum range_start;
	Datum range_end;
} ChunkRange;

/*
 * Default origin used for calendar-based chunking when no user-specified
 * origin is provided. 2001-01-01 00:00:00 is a Monday, so 7-day intervals
 * align with the start of the week by default.
 */
#define DEFAULT_ORIGIN_YEAR 2001
#define DEFAULT_ORIGIN_MONTH 1
#define DEFAULT_ORIGIN_DAY 1

/*
 * Calculate the timezone-aligned chunk range for a given timestamp.
 *
 * The interval length can be expressed as pure months, pure days, or pure
 * time (not combined). The start of the interval is evenly aligned with
 * the specified origin, or defaults to 2001-01-01 00:00:00 if no origin
 * is specified.
 *
 * The origin parameter should be a Datum of the same type as the timestamp
 * (TIMESTAMPOID or TIMESTAMPTZOID), or 0 for the default origin.
 *
 * For example, with a timestamp of Feb 1 2024 and an interval of 2 months,
 * the range would be Jan 1 2024 to Mar 1 2024 (assuming default origin).
 */
extern ChunkRange ts_chunk_range_calculate(Datum timestamp, Oid type, const Interval *interval,
										   Datum origin);

/*
 * Calculate the chunk range for non-calendar-compatible (mixed) intervals.
 *
 * Uses iterative timestamp arithmetic to find the bucket containing the timestamp.
 * Optimized by estimating how many intervals to jump before iterating.
 *
 * The origin parameter should be a Datum of the same type as the timestamp.
 * If iterations is not NULL, it will be set to the number of iterations used.
 */
extern ChunkRange ts_chunk_range_calculate_general(Datum timestamp, Oid type,
												   const Interval *interval, Datum origin,
												   int64 *iterations);

static inline ChunkRange
ts_chunk_range_from_timestamp(Timestamp start, Timestamp end)
{
	return (ChunkRange){
		.type = TIMESTAMPOID,
		.range_start = TimestampGetDatum(start),
		.range_end = TimestampGetDatum(end),
	};
}

static inline ChunkRange
ts_chunk_range_from_timestamptz(TimestampTz start, TimestampTz end)
{
	return (ChunkRange){
		.type = TIMESTAMPTZOID,
		.range_start = TimestampTzGetDatum(start),
		.range_end = TimestampTzGetDatum(end),
	};
}

static inline int64
ts_chunk_range_get_start_internal(const ChunkRange *cr)
{
	return ts_time_value_to_internal(cr->range_start, cr->type);
}

static inline int64
ts_chunk_range_get_end_internal(const ChunkRange *cr)
{
	return ts_time_value_to_internal(cr->range_end, cr->type);
}

#ifdef TS_DEBUG
/*
 * Saturating timestamp + interval arithmetic for testing.
 * Returns true if operation succeeded, false if result was clamped to infinity.
 */
extern bool ts_test_timestamp_interval_add_saturating(TimestampTz timestamp,
													  const Interval *interval, pg_tz *attimezone,
													  TimestampTz *result);
#endif
