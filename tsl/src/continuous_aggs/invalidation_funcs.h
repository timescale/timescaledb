/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>

#include <replication/logicalproto.h>

extern PGDLLEXPORT Datum ts_invalidation_read_record(PG_FUNCTION_ARGS);

extern PGDLLEXPORT Datum ts_invalidation_tuple_get_value(LogicalRepTupleData *tupleData,
														 TupleDesc tupdesc, int attnum,
														 bool *isnull);
extern PGDLLEXPORT LogicalRepRelId ts_invalidation_tuple_decode(LogicalRepTupleData *tuple,
																bytea *record);
