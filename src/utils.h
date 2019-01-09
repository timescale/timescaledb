/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_UTILS_H
#define TIMESCALEDB_UTILS_H

#include <postgres.h>
#include <fmgr.h>
#include <nodes/primnodes.h>
#include <catalog/pg_proc.h>
#include <utils/datetime.h>
#include <access/htup_details.h>

extern bool ts_type_is_int8_binary_compatible(Oid sourcetype);

/*
 * Convert a column value into the internal time representation.
 */
extern int64 ts_time_value_to_internal(Datum time_val, Oid type, bool failure_ok);

/*
 * Convert the difference of interval and current timestamp to internal representation
 */
extern int64 ts_interval_from_now_to_internal(Datum time_val, Oid type);

/*
 * Return the period in microseconds of the first argument to date_trunc.
 * This is approximate -- to be used for planning;
 */
extern int64 ts_date_trunc_interval_period_approx(text *units);

/*
 * Return the interval period in microseconds.
 * This is approximate -- to be used for planning;
 */
extern int64 ts_get_interval_period_approx(Interval *interval);

extern Oid	ts_inheritance_parent_relid(Oid relid);

extern bool ts_function_types_equal(Oid left[], Oid right[], int nargs);

extern Oid	get_function_oid(char *name, char *schema_name, int nargs, Oid arg_types[]);

extern void *ts_create_struct_from_tuple(HeapTuple tuple, MemoryContext mctx, size_t alloc_size, size_t copy_size);

#define STRUCT_FROM_TUPLE(tuple, mctx, to_type, form_type) \
      (to_type *) ts_create_struct_from_tuple(tuple, mctx, sizeof(to_type), sizeof(form_type));

/* note PG10 has_superclass but PG96 does not so use this */
#define is_inheritance_child(relid) \
	(ts_inheritance_parent_relid(relid) != InvalidOid)

#define is_inheritance_parent(relid) \
	(find_inheritance_children(table_relid, AccessShareLock) != NIL)

#define is_inheritance_table(relid) \
	(is_inheritance_child(relid) || is_inheritance_parent(relid))

#define DATUM_GET(values, attno) \
	values[attno-1]

#endif							/* TIMESCALEDB_UTILS_H */
