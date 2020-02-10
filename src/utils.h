/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_UTILS_H
#define TIMESCALEDB_UTILS_H

#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/pg_proc.h>
#include <utils/datetime.h>

#include "compat.h"

#if PG11_GE
#include <common/int.h>
#endif

/* Use a special pseudo-random field 4 value to avoid conflicting with user-advisory-locks */
#define TS_SET_LOCKTAG_ADVISORY(tag, id1, id2, id3)                                                \
	SET_LOCKTAG_ADVISORY((tag), (id1), (id2), (id3), 29749)

#define TS_EPOCH_DIFF_MICROSECONDS ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY)
#define TS_INTERNAL_TIMESTAMP_MIN ((int64) USECS_PER_DAY * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE))

/* find the length of a statically sized array */
#define TS_ARRAY_LEN(array) (sizeof(array) / sizeof(*array))

extern TSDLLEXPORT bool ts_type_is_int8_binary_compatible(Oid sourcetype);

typedef enum TimevalInfinity
{
	TimevalFinite = 0,
	TimevalNegInfinity = -1,
	TimevalPosInfinity = 1,
} TimevalInfinity;

typedef bool (*proc_filter)(Form_pg_proc form, void *arg);

/*
 * Convert a column value into the internal time representation.
 * cannot store a timestamp earlier than MIN_TIMESTAMP, or greater than
 *    END_TIMESTAMP - TS_EPOCH_DIFF_MICROSECONDS
 * nor dates that cannot be translated to timestamps
 * Will throw an error for that, or other conversion issues.
 */
extern TSDLLEXPORT int64 ts_time_value_to_internal(Datum time_val, Oid type);
extern int64 ts_time_value_to_internal_or_infinite(Datum time_val, Oid type_oid,
												   TimevalInfinity *is_infinite_out);
extern TSDLLEXPORT int64 ts_interval_value_to_internal(Datum time_val, Oid type_oid);

/*
 * Convert a column from the internal time representation into the specified type
 */
extern TSDLLEXPORT Datum ts_internal_to_time_value(int64 value, Oid type);
extern TSDLLEXPORT Datum ts_internal_to_interval_value(int64 value, Oid type);
extern TSDLLEXPORT char *ts_internal_to_time_string(int64 value, Oid type);

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

extern Oid ts_inheritance_parent_relid(Oid relid);

extern Oid ts_lookup_proc_filtered(const char *schema, const char *funcname, Oid *rettype,
								   proc_filter filter, void *filter_arg);
extern Oid ts_get_operator(const char *name, Oid namespace, Oid left, Oid right);

extern TSDLLEXPORT Oid ts_get_cast_func(Oid source, Oid target);

extern void *ts_create_struct_from_tuple(HeapTuple tuple, MemoryContext mctx, size_t alloc_size,
										 size_t copy_size);

extern TSDLLEXPORT AppendRelInfo *ts_get_appendrelinfo(PlannerInfo *root, Index rti,
													   bool missing_ok);

extern TSDLLEXPORT Expr *ts_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);

extern TSDLLEXPORT bool ts_has_row_security(Oid relid);

#define STRUCT_FROM_TUPLE(tuple, mctx, to_type, form_type)                                         \
	(to_type *) ts_create_struct_from_tuple(tuple, mctx, sizeof(to_type), sizeof(form_type));

/* note PG10 has_superclass but PG96 does not so use this */
#define is_inheritance_child(relid) (ts_inheritance_parent_relid(relid) != InvalidOid)

#define is_inheritance_parent(relid)                                                               \
	(find_inheritance_children(table_relid, AccessShareLock) != NIL)

#define is_inheritance_table(relid) (is_inheritance_child(relid) || is_inheritance_parent(relid))

#define DATUM_GET(values, attno) values[attno - 1]

static inline int64
int64_min(int64 a, int64 b)
{
	if (a <= b)
		return a;
	return b;
}

static inline int64
int64_saturating_add(int64 a, int64 b)
{
	int64 result;
	bool overflowed = pg_add_s64_overflow(a, b, &result);
	if (overflowed)
		result = a < 0 ? PG_INT64_MIN : PG_INT64_MAX;
	return result;
}

static inline int64
int64_saturating_sub(int64 a, int64 b)
{
	int64 result;
	bool overflowed = pg_sub_s64_overflow(a, b, &result);
	if (overflowed)
		result = b < 0 ? PG_INT64_MAX : PG_INT64_MIN;
	return result;
}

#endif /* TIMESCALEDB_UTILS_H */
