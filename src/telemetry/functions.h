/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TELEMETRY_FUNCTIONS_H
#define TIMESCALEDB_TELEMETRY_FUNCTIONS_H

#include <postgres.h>

typedef struct FnTelemetryEntry
{
	Oid fn;
	uint64 count;
} FnTelemetryEntry;

#define VEC_PREFIX fn_telemetry_entry
#define VEC_ELEMENT_TYPE FnTelemetryEntry
#define VEC_DECLARE 1
#define VEC_DEFINE 1
#define VEC_SCOPE static inline
#include <adts/vec.h>

extern void ts_telemetry_function_info_gather(Query *query);

extern fn_telemetry_entry_vec *ts_function_telemetry_read(const char **visible_extensions,
														  int num_visible_extensions);
extern void ts_function_telemetry_reset_counts(void);

#endif /* TIMESCALEDB_TELEMETRY_FUNCTIONS_H */
