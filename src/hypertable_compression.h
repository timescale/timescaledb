/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_COMPRESSION_H
#define TIMESCALEDB_HYPERTABLE_COMPRESSION_H
#include <postgres.h>
#include <catalog/pg_type.h>

#include <catalog.h>
#include <chunk.h>

extern TSDLLEXPORT List *ts_hypertable_compression_get(int32 htid);
extern TSDLLEXPORT void
ts_hypertable_compression_fill_tuple_values(FormData_hypertable_compression *fd, Datum *values,
											bool *nulls);

extern TSDLLEXPORT bool ts_hypertable_compression_delete_by_hypertable_id(int32 htid);

#endif
