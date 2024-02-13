/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/genam.h>
#include <access/heapam.h>
#include <access/htup.h>
#include <access/htup_details.h>
#include <access/reloptions.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_am.h>
#include <catalog/pg_cast.h>
#include <catalog/pg_inherits.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_type.h>
#include <commands/event_trigger.h>
#include <commands/tablecmds.h>
#include <fmgr.h>
#include <funcapi.h>
#include <nodes/makefuncs.h>
#include <parser/parse_coerce.h>
#include <parser/parse_func.h>
#include <parser/scansup.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/catcache.h>
#include <utils/date.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "chunk.h"
#include "cross_module_fn.h"
#include "debug_point.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "jsonb_utils.h"
#include "time_utils.h"
#include "utils.h"

typedef struct
{
	const char *name;
	AclMode value;
} priv_map;

TS_FUNCTION_INFO_V1(ts_pg_timestamp_to_unix_microseconds);
TS_FUNCTION_INFO_V1(ts_makeaclitem);

/*
 * Convert a Postgres TIMESTAMP to BIGINT microseconds relative the UNIX epoch.
 */
Datum
ts_pg_timestamp_to_unix_microseconds(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);

	if (TIMESTAMP_IS_NOBEGIN(timestamp))
		PG_RETURN_INT64(TS_TIME_NOBEGIN);

	if (TIMESTAMP_IS_NOEND(timestamp))
		PG_RETURN_INT64(TS_TIME_NOEND);

	if (timestamp < TS_TIMESTAMP_MIN)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

	if (timestamp >= TS_TIMESTAMP_END)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

	PG_RETURN_INT64(timestamp + TS_EPOCH_DIFF_MICROSECONDS);
}

TS_FUNCTION_INFO_V1(ts_pg_unix_microseconds_to_timestamp);
TS_FUNCTION_INFO_V1(ts_pg_unix_microseconds_to_timestamp_without_timezone);
TS_FUNCTION_INFO_V1(ts_pg_unix_microseconds_to_date);

/*
 * Convert BIGINT microseconds relative the UNIX epoch to a Postgres TIMESTAMP.
 */
Datum
ts_pg_unix_microseconds_to_timestamp(PG_FUNCTION_ARGS)
{
	int64 microseconds = PG_GETARG_INT64(0);

	if (TS_TIME_IS_NOBEGIN(microseconds, TIMESTAMPTZOID))
		PG_RETURN_DATUM(ts_time_datum_get_nobegin(TIMESTAMPTZOID));

	if (TS_TIME_IS_NOEND(microseconds, TIMESTAMPTZOID))
		PG_RETURN_DATUM(ts_time_datum_get_noend(TIMESTAMPTZOID));

	/*
	 * Test that the UNIX us timestamp is within bounds. Note that an int64 at
	 * UNIX epoch and microsecond precision cannot represent the upper limit
	 * of the supported date range (Julian end date), so INT64_MAX-1 is the
	 * natural upper bound for this function.
	 */
	if (microseconds < TS_TIMESTAMP_INTERNAL_MIN)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE), errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMPTZ(microseconds - TS_EPOCH_DIFF_MICROSECONDS);
}

Datum
ts_pg_unix_microseconds_to_date(PG_FUNCTION_ARGS)
{
	int64 microseconds = PG_GETARG_INT64(0);
	Datum res;

	if (TS_TIME_IS_NOBEGIN(microseconds, DATEOID))
		PG_RETURN_DATUM(ts_time_datum_get_nobegin(DATEOID));

	if (TS_TIME_IS_NOEND(microseconds, DATEOID))
		PG_RETURN_DATUM(ts_time_datum_get_noend(DATEOID));

	res = DirectFunctionCall1(ts_pg_unix_microseconds_to_timestamp, Int64GetDatum(microseconds));
	res = DirectFunctionCall1(timestamp_date, res);
	PG_RETURN_DATUM(res);
}

static int64 ts_integer_to_internal(Datum time_val, Oid type_oid);

/* Convert valid timescale time column type to internal representation */
TSDLLEXPORT int64
ts_time_value_to_internal(Datum time_val, Oid type_oid)
{
	Datum res, tz;

	/* Handle custom time types. We currently only support binary coercible
	 * types */
	if (!IS_VALID_TIME_TYPE(type_oid))
	{
		if (ts_type_is_int8_binary_compatible(type_oid))
			return DatumGetInt64(time_val);

		elog(ERROR, "unknown time type \"%s\"", format_type_be(type_oid));
	}

	if (IS_INTEGER_TYPE(type_oid))
	{
		/* Integer time types have no distinction between min, max and
		 * infinity. We don't want min and max to be turned into infinity for
		 * these types so check for those values first. */
		if (TS_TIME_DATUM_IS_MIN(time_val, type_oid))
			return ts_time_get_min(type_oid);

		if (TS_TIME_DATUM_IS_MAX(time_val, type_oid))
			return ts_time_get_max(type_oid);
	}

	if (TS_TIME_DATUM_IS_NOBEGIN(time_val, type_oid))
		return ts_time_get_nobegin(type_oid);

	if (TS_TIME_DATUM_IS_NOEND(time_val, type_oid))
		return ts_time_get_noend(type_oid);

	switch (type_oid)
	{
		case INT8OID:
		case INT4OID:
		case INT2OID:
			return ts_integer_to_internal(time_val, type_oid);
		case TIMESTAMPOID:
			/*
			 * for timestamps, ignore timezones, make believe the timestamp is
			 * at UTC
			 */
			res = DirectFunctionCall1(ts_pg_timestamp_to_unix_microseconds, time_val);

			return DatumGetInt64(res);
		case TIMESTAMPTZOID:
			res = DirectFunctionCall1(ts_pg_timestamp_to_unix_microseconds, time_val);

			return DatumGetInt64(res);
		case DATEOID:
			tz = DirectFunctionCall1(date_timestamp, time_val);
			res = DirectFunctionCall1(ts_pg_timestamp_to_unix_microseconds, tz);

			return DatumGetInt64(res);
		default:
			elog(ERROR, "unknown time type \"%s\"", format_type_be(type_oid));
			return -1;
	}
}

TSDLLEXPORT int64
ts_interval_value_to_internal(Datum time_val, Oid type_oid)
{
	switch (type_oid)
	{
		case INT8OID:
		case INT4OID:
		case INT2OID:
			return ts_integer_to_internal(time_val, type_oid);
		case INTERVALOID:
		{
			Interval *interval = DatumGetIntervalP(time_val);
			if (interval->month != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("months and years not supported"),
						 errdetail("An interval must be defined as a fixed duration (such as "
								   "weeks, days, hours, minutes, seconds, etc.).")));
			return interval->time + (interval->day * USECS_PER_DAY);
		}
		default:
			elog(ERROR, "unknown interval type \"%s\"", format_type_be(type_oid));
			return -1;
	}
}

static int64
ts_integer_to_internal(Datum time_val, Oid type_oid)
{
	switch (type_oid)
	{
		case INT8OID:
			return DatumGetInt64(time_val);
		case INT4OID:
			return (int64) DatumGetInt32(time_val);
		case INT2OID:
			return (int64) DatumGetInt16(time_val);
		default:
			elog(ERROR, "unknown interval type \"%s\"", format_type_be(type_oid));
			return -1;
	}
}

int64
ts_time_value_to_internal_or_infinite(Datum time_val, Oid type_oid)
{
	switch (type_oid)
	{
		case TIMESTAMPOID:
		{
			Timestamp ts = DatumGetTimestamp(time_val);
			if (TIMESTAMP_NOT_FINITE(ts))
			{
				if (TIMESTAMP_IS_NOBEGIN(ts))
				{
					return PG_INT64_MIN;
				}
				else
				{
					return PG_INT64_MAX;
				}
			}

			return ts_time_value_to_internal(time_val, type_oid);
		}
		case TIMESTAMPTZOID:
		{
			TimestampTz ts = DatumGetTimestampTz(time_val);
			if (TIMESTAMP_NOT_FINITE(ts))
			{
				if (TIMESTAMP_IS_NOBEGIN(ts))
				{
					return PG_INT64_MIN;
				}
				else
				{
					return PG_INT64_MAX;
				}
			}

			return ts_time_value_to_internal(time_val, type_oid);
		}
		case DATEOID:
		{
			DateADT d = DatumGetDateADT(time_val);
			if (DATE_NOT_FINITE(d))
			{
				if (DATE_IS_NOBEGIN(d))
				{
					return PG_INT64_MIN;
				}
				else
				{
					return PG_INT64_MAX;
				}
			}

			return ts_time_value_to_internal(time_val, type_oid);
		}
	}

	return ts_time_value_to_internal(time_val, type_oid);
}

TS_FUNCTION_INFO_V1(ts_time_to_internal);
Datum
ts_time_to_internal(PG_FUNCTION_ARGS)
{
	Datum time = PG_GETARG_DATUM(0);
	Oid time_type = get_fn_expr_argtype(fcinfo->flinfo, 0);
	int64 res = ts_time_value_to_internal(time, time_type);
	PG_RETURN_INT64(res);
}

static Datum ts_integer_to_internal_value(int64 value, Oid type);

/*
 * convert int64 to Datum according to type
 * internally we store all times as int64 in the
 * same format postgres does
 */
TSDLLEXPORT Datum
ts_internal_to_time_value(int64 value, Oid type)
{
	if (TS_TIME_IS_NOBEGIN(value, type))
		return ts_time_datum_get_nobegin(type);

	if (TS_TIME_IS_NOEND(value, type))
		return ts_time_datum_get_noend(type);

	switch (type)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			return ts_integer_to_internal_value(value, type);
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			/* we continue ts_time_value_to_internal's incorrect handling of TIMESTAMPs for
			 * compatibility */
			return DirectFunctionCall1(ts_pg_unix_microseconds_to_timestamp, Int64GetDatum(value));
		case DATEOID:
			return DirectFunctionCall1(ts_pg_unix_microseconds_to_date, Int64GetDatum(value));
		default:
			if (ts_type_is_int8_binary_compatible(type))
				return Int64GetDatum(value);
			elog(ERROR,
				 "unknown time type \"%s\" in ts_internal_to_time_value",
				 format_type_be(type));
			pg_unreachable();
	}
}

TSDLLEXPORT int64
ts_internal_to_time_int64(int64 value, Oid type)
{
	if (TS_TIME_IS_NOBEGIN(value, type))
		return ts_time_datum_get_nobegin(type);

	if (TS_TIME_IS_NOEND(value, type))
		return ts_time_datum_get_noend(type);

	switch (type)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			return value;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			/* we continue ts_time_value_to_internal's incorrect handling of TIMESTAMPs for
			 * compatibility */
			return DatumGetInt64(
				DirectFunctionCall1(ts_pg_unix_microseconds_to_timestamp, Int64GetDatum(value)));
		case DATEOID:
			return DatumGetInt64(
				DirectFunctionCall1(ts_pg_unix_microseconds_to_date, Int64GetDatum(value)));
		default:
			elog(ERROR,
				 "unknown time type \"%s\" in ts_internal_to_time_value",
				 format_type_be(type));
			pg_unreachable();
	}
}

TSDLLEXPORT char *
ts_internal_to_time_string(int64 value, Oid type)
{
	Datum time_datum = ts_internal_to_time_value(value, type);
	Oid typoutputfunc;
	bool typIsVarlena;
	FmgrInfo typoutputinfo;

	getTypeOutputInfo(type, &typoutputfunc, &typIsVarlena);
	fmgr_info(typoutputfunc, &typoutputinfo);
	return OutputFunctionCall(&typoutputinfo, time_datum);
}

TS_FUNCTION_INFO_V1(ts_pg_unix_microseconds_to_interval);

Datum
ts_pg_unix_microseconds_to_interval(PG_FUNCTION_ARGS)
{
	int64 microseconds = PG_GETARG_INT64(0);
	Interval *interval = palloc0(sizeof(*interval));
	interval->day = microseconds / USECS_PER_DAY;
	interval->time = microseconds % USECS_PER_DAY;
	PG_RETURN_INTERVAL_P(interval);
}

TSDLLEXPORT Datum
ts_internal_to_interval_value(int64 value, Oid type)
{
	switch (type)
	{
		case INT2OID:
		case INT4OID:
		case INT8OID:
			return ts_integer_to_internal_value(value, type);
		case INTERVALOID:
			return DirectFunctionCall1(ts_pg_unix_microseconds_to_interval, Int64GetDatum(value));
		default:
			elog(ERROR,
				 "unknown time type \"%s\" in ts_internal_to_interval_value",
				 format_type_be(type));
			pg_unreachable();
	}
}

static Datum
ts_integer_to_internal_value(int64 value, Oid type)
{
	switch (type)
	{
		case INT2OID:
			return Int16GetDatum(value);
		case INT4OID:
			return Int32GetDatum(value);
		case INT8OID:
			return Int64GetDatum(value);
		default:
			elog(ERROR,
				 "unknown time type \"%s\" in ts_internal_to_time_value",
				 format_type_be(type));
			pg_unreachable();
	}
}

/* Returns approximate period in microseconds */
int64
ts_get_interval_period_approx(Interval *interval)
{
	return interval->time +
		   ((((int64) interval->month * DAYS_PER_MONTH) + interval->day) * USECS_PER_DAY);
}

#define DAYS_PER_WEEK 7
#define DAYS_PER_QUARTER 89
#define YEARS_PER_DECADE 10
#define YEARS_PER_CENTURY 100
#define YEARS_PER_MILLENNIUM 1000

/* Returns approximate period in microseconds */
int64
ts_date_trunc_interval_period_approx(text *units)
{
	int decode_type, val;
	char *lowunits =
		downcase_truncate_identifier(VARDATA_ANY(units), VARSIZE_ANY_EXHDR(units), false);

	decode_type = DecodeUnits(0, lowunits, &val);

	if (decode_type != UNITS)
		return -1;

	switch (val)
	{
		case DTK_WEEK:
			return DAYS_PER_WEEK * USECS_PER_DAY;
		case DTK_MILLENNIUM:
			return YEARS_PER_MILLENNIUM * DAYS_PER_YEAR * USECS_PER_DAY;
		case DTK_CENTURY:
			return YEARS_PER_CENTURY * DAYS_PER_YEAR * USECS_PER_DAY;
		case DTK_DECADE:
			return YEARS_PER_DECADE * DAYS_PER_YEAR * USECS_PER_DAY;
		case DTK_YEAR:
			return 1 * DAYS_PER_YEAR * USECS_PER_DAY;
		case DTK_QUARTER:
			return DAYS_PER_QUARTER * USECS_PER_DAY;
		case DTK_MONTH:
			return DAYS_PER_MONTH * USECS_PER_DAY;
		case DTK_DAY:
			return USECS_PER_DAY;
		case DTK_HOUR:
			return USECS_PER_HOUR;
		case DTK_MINUTE:
			return USECS_PER_MINUTE;
		case DTK_SECOND:
			return USECS_PER_SEC;
		case DTK_MILLISEC:
			return USECS_PER_SEC / 1000;
		case DTK_MICROSEC:
			return 1;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("timestamp units \"%s\" not supported", lowunits)));
	}
	return -1;
}

Oid
ts_inheritance_parent_relid(Oid relid)
{
	Relation catalog;
	SysScanDesc scan;
	ScanKeyData skey;
	Oid parent = InvalidOid;
	HeapTuple tuple;

	catalog = table_open(InheritsRelationId, AccessShareLock);
	ScanKeyInit(&skey,
				Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(relid));
	scan = systable_beginscan(catalog, InheritsRelidSeqnoIndexId, true, NULL, 1, &skey);
	tuple = systable_getnext(scan);

	if (HeapTupleIsValid(tuple))
		parent = ((Form_pg_inherits) GETSTRUCT(tuple))->inhparent;

	systable_endscan(scan);
	table_close(catalog, AccessShareLock);

	return parent;
}

TSDLLEXPORT bool
ts_type_is_int8_binary_compatible(Oid sourcetype)
{
	HeapTuple tuple;
	Form_pg_cast castForm;
	bool result;

	tuple =
		SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(sourcetype), ObjectIdGetDatum(INT8OID));
	if (!HeapTupleIsValid(tuple))
		return false; /* no cast */
	castForm = (Form_pg_cast) GETSTRUCT(tuple);
	result = castForm->castmethod == COERCION_METHOD_BINARY;
	ReleaseSysCache(tuple);
	return result;
}

/*
 * Create a fresh struct pointer that will contain copied contents of the tuple.
 * Note that this function uses GETSTRUCT, which will not work correctly for tuple types
 * that might have variable lengths.
 * Also note that the function assumes no NULLs in the tuple.
 */
static void *
ts_create_struct_from_tuple(HeapTuple tuple, MemoryContext mctx, size_t alloc_size,
							size_t copy_size)
{
	void *struct_ptr = MemoryContextAllocZero(mctx, alloc_size);

	/*
	 * Make sure the function is not used when the tuple contains NULLs.
	 * Also compare the aligned sizes in the assert.
	 */
	Assert(copy_size == MAXALIGN(tuple->t_len - tuple->t_data->t_hoff));
	memcpy(struct_ptr, GETSTRUCT(tuple), copy_size);

	return struct_ptr;
}

void *
ts_create_struct_from_slot(TupleTableSlot *slot, MemoryContext mctx, size_t alloc_size,
						   size_t copy_size)
{
	bool should_free;
	HeapTuple tuple = ExecFetchSlotHeapTuple(slot, false, &should_free);
	void *result = ts_create_struct_from_tuple(tuple, mctx, alloc_size, copy_size);

	if (should_free)
		heap_freetuple(tuple);

	return result;
}

bool
ts_function_types_equal(Oid left[], Oid right[], int nargs)
{
	int arg_index;

	for (arg_index = 0; arg_index < nargs; arg_index++)
	{
		if (left[arg_index] != right[arg_index])
			return false;
	}
	return true;
}

Oid
ts_get_function_oid(const char *funcname, const char *schema_name, int nargs, Oid arg_types[])
{
	List *qualified_funcname =
		list_make2(makeString(pstrdup(schema_name)), makeString(pstrdup(funcname)));
	FuncCandidateList func_candidates;

	func_candidates = FuncnameGetCandidates(qualified_funcname,
											nargs,
											NIL,
											false,
											false, /* include_out_arguments */
											false,
											false);
	while (func_candidates != NULL)
	{
		if (func_candidates->nargs == nargs &&
			ts_function_types_equal(func_candidates->args, arg_types, nargs))
			return func_candidates->oid;
		func_candidates = func_candidates->next;
	}

	elog(ERROR,
		 "failed to find function %s with %d args in schema \"%s\"",
		 funcname,
		 nargs,
		 schema_name);

	return InvalidOid;
}

/*
 * Find a partitioning function with a given schema and name.
 *
 * The caller can optionally pass a filter function and a type of the argument
 * that the partitioning function should take.
 */
Oid
ts_lookup_proc_filtered(const char *schema, const char *funcname, Oid *rettype, proc_filter filter,
						void *filter_arg)
{
	Oid namespace_oid = LookupExplicitNamespace(schema, false);
	regproc func = InvalidOid;
	CatCList *catlist;
	int i;

	/*
	 * We could use SearchSysCache3 to get by (name, args, namespace), but
	 * that would not allow us to check for functions that take either
	 * ANYELEMENTOID or a dimension-specific in the same search.
	 */
	catlist = SearchSysCacheList1(PROCNAMEARGSNSP, CStringGetDatum(funcname));

	for (i = 0; i < catlist->n_members; i++)
	{
		HeapTuple proctup = &catlist->members[i]->tuple;
		Form_pg_proc procform = (Form_pg_proc) GETSTRUCT(proctup);

		if (procform->pronamespace == namespace_oid &&
			(filter == NULL || filter(procform, filter_arg)))
		{
			if (rettype)
				*rettype = procform->prorettype;

			func = procform->oid;
			break;
		}
	}

	ReleaseSysCacheList(catlist);

	return func;
}

/*
 * ts_get_operator
 *
 *    finds an operator given an exact specification (name, namespace,
 *    left and right type IDs).
 */
Oid
ts_get_operator(const char *name, Oid namespace, Oid left, Oid right)
{
	HeapTuple tup;
	Oid opoid = InvalidOid;

	tup = SearchSysCache4(OPERNAMENSP,
						  PointerGetDatum(name),
						  ObjectIdGetDatum(left),
						  ObjectIdGetDatum(right),
						  ObjectIdGetDatum(namespace));
	if (HeapTupleIsValid(tup))
	{
		Form_pg_operator oprform = (Form_pg_operator) GETSTRUCT(tup);
		opoid = oprform->oid;
		ReleaseSysCache(tup);
	}

	return opoid;
}

/*
 * ts_get_cast_func
 *
 * returns Oid of functions that implements cast from source to target
 */
Oid
ts_get_cast_func(Oid source, Oid target)
{
	Oid result = InvalidOid;
	HeapTuple casttup;

	casttup = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(source), ObjectIdGetDatum(target));
	if (HeapTupleIsValid(casttup))
	{
		Form_pg_cast castform = (Form_pg_cast) GETSTRUCT(casttup);

		result = castform->castfunc;
		ReleaseSysCache(casttup);
	}

	return result;
}

AppendRelInfo *
ts_get_appendrelinfo(PlannerInfo *root, Index rti, bool missing_ok)
{
	ListCell *lc;
	/* use append_rel_array if it has been setup */
	if (root->append_rel_array)
	{
		if (root->append_rel_array[rti])
			return root->append_rel_array[rti];
		if (!missing_ok)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("no appendrelinfo found for index %d", rti)));
		return NULL;
	}

	foreach (lc, root->append_rel_list)
	{
		AppendRelInfo *appinfo = lfirst(lc);
		if (appinfo->child_relid == rti)
			return appinfo;
	}
	if (!missing_ok)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("no appendrelinfo found for index %d", rti)));
	return NULL;
}

#if PG15_GE
/*
 * Find an equivalence class member expression, all of whose Vars, come from
 * the indicated relation.
 *
 * This function has been copied from find_em_expr_for_rel in
 * contrib/postgres_fdw/postgres_fdw.c in postgres source.
 * This function was moved to postgres main in PG13 but was removed
 * again in PG15. So we use our own implementation for PG15+.
 */
Expr *
ts_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell *lc_em;

	foreach (lc_em, ec->ec_members)
	{
		EquivalenceMember *em = lfirst(lc_em);

		if (bms_is_subset(em->em_relids, rel->relids) && !bms_is_empty(em->em_relids))
		{
			/*
			 * If there is more than one equivalence member whose Vars are
			 * taken entirely from this relation, we'll be content to choose
			 * any one of those.
			 */
			return em->em_expr;
		}
	}

	/* We didn't find any suitable equivalence class expression */
	return NULL;
}
#endif

bool
ts_has_row_security(Oid relid)
{
	HeapTuple tuple;
	Form_pg_class classform;
	bool relrowsecurity;
	bool relforcerowsecurity;

	/* Fetch relation's relrowsecurity and relforcerowsecurity flags */
	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relid %u", relid);
	classform = (Form_pg_class) GETSTRUCT(tuple);
	relrowsecurity = classform->relrowsecurity;
	relforcerowsecurity = classform->relforcerowsecurity;
	ReleaseSysCache(tuple);
	return (relrowsecurity || relforcerowsecurity);
}

List *
ts_get_reloptions(Oid relid)
{
	HeapTuple tuple;
	Datum datum;
	bool isnull;
	List *options = NIL;

	Assert(OidIsValid(relid));

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	datum = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);

	if (!isnull && PointerIsValid(DatumGetPointer(datum)))
		options = untransformRelOptions(datum);

	ReleaseSysCache(tuple);

	return options;
}

/*
 * Get the integer_now function for a dimension
 */
Oid
ts_get_integer_now_func(const Dimension *open_dim, bool fail_if_not_found)
{
	Oid rettype;
	Oid now_func = InvalidOid;
	Oid argtypes[] = { 0 };

	rettype = ts_dimension_get_partition_type(open_dim);

	Assert(IS_INTEGER_TYPE(rettype));

	if (strlen(NameStr(open_dim->fd.integer_now_func)) == 0 &&
		strlen(NameStr(open_dim->fd.integer_now_func_schema)) == 0)
	{
		if (fail_if_not_found)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 (errmsg("integer_now function not set"))));
		else
			return now_func;
	}

	List *name = list_make2(makeString((char *) NameStr(open_dim->fd.integer_now_func_schema)),
							makeString((char *) NameStr(open_dim->fd.integer_now_func)));
	now_func = LookupFuncName(name, 0, argtypes, false);

	if (get_func_rettype(now_func) != rettype)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 (errmsg("invalid integer_now function"),
				  errhint("return type of function does not match dimension type"))));

	return now_func;
}

/* subtract passed in interval from the now.
 * Arguments:
 * now_func : function used to compute now.
 * interval : integer value
 * Returns:
 *  now_func() - interval
 */
int64
ts_sub_integer_from_now(int64 interval, Oid time_dim_type, Oid now_func)
{
	Datum now;
	int64 res;

	Assert(IS_INTEGER_TYPE(time_dim_type));

	now = OidFunctionCall0(now_func);
	switch (time_dim_type)
	{
		case INT2OID:
			res = DatumGetInt16(now) - interval;
			if (res < PG_INT16_MIN || res > PG_INT16_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
						 errmsg("integer time overflow")));
			return res;
		case INT4OID:
			res = DatumGetInt32(now) - interval;
			if (res < PG_INT32_MIN || res > PG_INT32_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
						 errmsg("integer time overflow")));
			return res;
		case INT8OID:
		{
			bool overflow = pg_sub_s64_overflow(DatumGetInt64(now), interval, &res);
			if (overflow)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
						 errmsg("integer time overflow")));
			}
			return res;
		}
		default:
			pg_unreachable();
	}
}

TS_FUNCTION_INFO_V1(ts_subtract_integer_from_now);
Datum
ts_subtract_integer_from_now(PG_FUNCTION_ARGS)
{
	Oid ht_relid = PG_GETARG_OID(0);
	int64 lag = PG_GETARG_INT64(1);
	Cache *hcache;
	Hypertable *ht = ts_hypertable_cache_get_cache_and_entry(ht_relid, CACHE_FLAG_NONE, &hcache);
	const Dimension *dim = hyperspace_get_open_dimension(ht->space, 0);

	if (!dim)
		elog(ERROR, "hypertable has no open partitioning dimension");

	Oid partitioning_type = ts_dimension_get_partition_type(dim);

	if (!IS_INTEGER_TYPE(partitioning_type))
		elog(ERROR, "hypertable has no integer partitioning dimension");

	Oid now_func = ts_get_integer_now_func(dim, true);
	if (!OidIsValid(now_func))
		elog(ERROR, "could not find valid integer_now function for hypertable");

	int64 res = ts_sub_integer_from_now(lag, partitioning_type, now_func);
	ts_cache_release(hcache);
	return Int64GetDatum(res);
}

TS_FUNCTION_INFO_V1(ts_relation_size);
Datum
ts_relation_size(PG_FUNCTION_ARGS)
{
	Oid relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	RelationSize relsize = { 0 };
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[4] = { 0 };
	bool nulls[4] = { false };

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	if (!OidIsValid(relid))
		PG_RETURN_NULL();

	relsize = ts_relation_size_impl(relid);

	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int64GetDatum(relsize.total_size);
	values[1] = Int64GetDatum(relsize.heap_size);
	values[2] = Int64GetDatum(relsize.index_size);
	values[3] = Int64GetDatum(relsize.toast_size);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

RelationSize
ts_relation_size_impl(Oid relid)
{
	RelationSize relsize = { 0 };
	Datum reloid = ObjectIdGetDatum(relid);
	Relation rel;

	DEBUG_WAITPOINT("relation_size_before_lock");
	/* Open relation earlier to keep a lock during all function calls */
	rel = try_relation_open(relid, AccessShareLock);

	if (!rel)
		return relsize;

	/* Get to total relation size to be our calculation base */
	relsize.total_size = DatumGetInt64(DirectFunctionCall1(pg_total_relation_size, reloid));

	/* Get the indexes size of the relation (don't consider TOAST indexes) */
	relsize.index_size = DatumGetInt64(DirectFunctionCall1(pg_indexes_size, reloid));

	/* If exists an associated TOAST calculate the total size (including indexes) */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
		relsize.toast_size =
			DatumGetInt64(DirectFunctionCall1(pg_total_relation_size,
											  ObjectIdGetDatum(rel->rd_rel->reltoastrelid)));
	else
		relsize.toast_size = 0;

	relation_close(rel, AccessShareLock);

	/* Calculate the HEAP size based on the total size and indexes plus toast */
	relsize.heap_size = relsize.total_size - (relsize.index_size + relsize.toast_size);

	return relsize;
}

/*
 * Try to get cached size for a provided relation across all forks. The
 * size is returned in terms of number of blocks.
 *
 * The function calls the underlying smgrnblocks if there is no cached
 * data. That call populates the cache for subsequent invocations. This
 * cached data gets removed asynchronously by PG relcache invalidations
 * and then the refresh/cache cycle repeats till the next invalidation.
 */
static int64
ts_try_relation_cached_size(Relation rel, bool verbose)
{
	BlockNumber result = InvalidBlockNumber, nblocks = 0;
	ForkNumber forkNum;
	bool cached = true;

	if (!RELKIND_HAS_STORAGE(rel->rd_rel->relkind))
		return (int64) nblocks;

	/* Get heap size, including FSM and VM */
	for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
	{
		result = RelationGetSmgr(rel)->smgr_cached_nblocks[forkNum];

		if (result != InvalidBlockNumber)
		{
			nblocks += result;
		}
		else
		{
			if (smgrexists(RelationGetSmgr(rel), forkNum))
			{
				cached = false;
				nblocks += smgrnblocks(RelationGetSmgr(rel), forkNum);
			}
		}
	}

	if (verbose)
		ereport(DEBUG2,
				(errmsg("%s for %s",
						cached ? "Cached size used" : "Fetching actual size",
						RelationGetRelationName(rel))));

	/* convert the size into bytes and return */
	return (int64) nblocks * BLCKSZ;
}

static RelationSize
ts_relation_approximate_size_impl(Oid relid)
{
	RelationSize relsize = { 0 };
	Relation rel;

	DEBUG_WAITPOINT("relation_approximate_size_before_lock");
	/* Open relation earlier to keep a lock during all function calls */
	rel = try_relation_open(relid, AccessShareLock);

	if (!rel)
		return relsize;

	/* Get the main heap size */
	relsize.heap_size = ts_try_relation_cached_size(rel, false);

	/* Get the size of the relation's indexes */
	if (rel->rd_rel->relhasindex)
	{
		List *index_oids = RelationGetIndexList(rel);
		ListCell *cell;

		foreach (cell, index_oids)
		{
			Oid idxOid = lfirst_oid(cell);
			Relation idxRel;

			idxRel = relation_open(idxOid, AccessShareLock);
			relsize.index_size += ts_try_relation_cached_size(idxRel, false);
			relation_close(idxRel, AccessShareLock);
		}
	}

	/* If there's an associated TOAST table, calculate the total size (including its indexes) */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
	{
		Relation toastRel;
		List *index_oids;
		ListCell *cell;

		toastRel = relation_open(rel->rd_rel->reltoastrelid, AccessShareLock);
		relsize.toast_size = ts_try_relation_cached_size(toastRel, false);

		/* Get the indexes size of the TOAST relation */
		index_oids = RelationGetIndexList(toastRel);
		foreach (cell, index_oids)
		{
			Oid idxOid = lfirst_oid(cell);
			Relation idxRel;

			idxRel = relation_open(idxOid, AccessShareLock);
			relsize.toast_size += ts_try_relation_cached_size(idxRel, false);
			relation_close(idxRel, AccessShareLock);
		}

		relation_close(toastRel, AccessShareLock);
	}

	relation_close(rel, AccessShareLock);

	/* Add up the total size based on the heap size, indexes and toast */
	relsize.total_size = relsize.heap_size + relsize.index_size + relsize.toast_size;

	return relsize;
}

TS_FUNCTION_INFO_V1(ts_relation_approximate_size);
Datum
ts_relation_approximate_size(PG_FUNCTION_ARGS)
{
	Oid relid = PG_GETARG_OID(0);
	RelationSize relsize = { 0 };
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[4] = { 0 };
	bool nulls[4] = { false };

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	/* check if object exists, return NULL otherwise */
	if (get_rel_name(relid) == NULL)
		PG_RETURN_NULL();

	relsize = ts_relation_approximate_size_impl(relid);

	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int64GetDatum(relsize.total_size);
	values[1] = Int64GetDatum(relsize.heap_size);
	values[2] = Int64GetDatum(relsize.index_size);
	values[3] = Int64GetDatum(relsize.toast_size);

	tuple = heap_form_tuple(tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

static void
init_scan_by_hypertable_id(ScanIterator *iterator, int32 hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_HYPERTABLE_ID_INDEX);
	ts_scan_iterator_scan_key_init(iterator,
								   Anum_chunk_hypertable_id_idx_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(hypertable_id));
}

#define ADD_RELATIONSIZE(total, rel)                                                               \
	do                                                                                             \
	{                                                                                              \
		(total).heap_size += (rel).heap_size;                                                      \
		(total).toast_size += (rel).toast_size;                                                    \
		(total).index_size += (rel).index_size;                                                    \
		(total).total_size += (rel).total_size;                                                    \
	} while (0)

TS_FUNCTION_INFO_V1(ts_hypertable_approximate_size);
Datum
ts_hypertable_approximate_size(PG_FUNCTION_ARGS)
{
	Oid relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	RelationSize total_relsize = { 0 };
	TupleDesc tupdesc;
	HeapTuple tuple;
	Datum values[4] = { 0 };
	bool nulls[4] = { false };
	Cache *hcache;
	Hypertable *ht;
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, RowExclusiveLock, CurrentMemoryContext);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	if (!OidIsValid(relid))
		PG_RETURN_NULL();

	/* go ahead only if this is a hypertable or a CAgg */
	hcache = ts_hypertable_cache_pin();
	ht = ts_resolve_hypertable_from_table_or_cagg(hcache, relid, true);
	if (ht == NULL)
	{
		ts_cache_release(hcache);
		PG_RETURN_NULL();
	}

	/* get the main hypertable size */
	total_relsize = ts_relation_approximate_size_impl(relid);

	iterator = ts_scan_iterator_create(CHUNK, RowExclusiveLock, CurrentMemoryContext);
	init_scan_by_hypertable_id(&iterator, ht->fd.id);
	ts_scanner_foreach(&iterator)
	{
		bool isnull, dropped, is_osm_chunk;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		Datum id = slot_getattr(ti->slot, Anum_chunk_id, &isnull);
		Datum comp_id = DatumGetInt32(slot_getattr(ti->slot, Anum_chunk_id, &isnull));
		int32 chunk_id, compressed_chunk_id;
		Oid chunk_relid, compressed_chunk_relid;
		RelationSize chunk_relsize, compressed_chunk_relsize;

		if (isnull)
			continue;

		/* only consider chunks that are not dropped */
		dropped = DatumGetBool(slot_getattr(ti->slot, Anum_chunk_dropped, &isnull));
		Assert(!isnull);
		if (dropped)
			continue;

		chunk_id = DatumGetInt32(id);

		/* avoid if it's an OSM chunk */
		is_osm_chunk = slot_getattr(ti->slot, Anum_chunk_osm_chunk, &isnull);
		Assert(!isnull);
		if (is_osm_chunk)
			continue;

		chunk_relid = ts_chunk_get_relid(chunk_id, false);
		chunk_relsize = ts_relation_approximate_size_impl(chunk_relid);
		/* add this chunk's size to the total size */
		ADD_RELATIONSIZE(total_relsize, chunk_relsize);

		/* check if the chunk has a compressed counterpart and add if yes */
		comp_id = slot_getattr(ti->slot, Anum_chunk_compressed_chunk_id, &isnull);
		if (isnull)
			continue;

		compressed_chunk_id = DatumGetInt32(comp_id);
		compressed_chunk_relid = ts_chunk_get_relid(compressed_chunk_id, false);
		compressed_chunk_relsize = ts_relation_approximate_size_impl(compressed_chunk_relid);
		/* add this compressed chunk's size to the total size */
		ADD_RELATIONSIZE(total_relsize, compressed_chunk_relsize);
	}
	ts_scan_iterator_close(&iterator);

	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int64GetDatum(total_relsize.heap_size);
	values[1] = Int64GetDatum(total_relsize.index_size);
	values[2] = Int64GetDatum(total_relsize.toast_size);
	values[3] = Int64GetDatum(total_relsize.total_size);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	ts_cache_release(hcache);

	return HeapTupleGetDatum(tuple);
}

#define STR_VALUE(str) #str
#define NODE_CASE(name)                                                                            \
	case T_##name:                                                                                 \
		return STR_VALUE(name)

/*
 * Return a string with the name of the node.
 *
 */
const char *
ts_get_node_name(Node *node)
{
	/* tags are defined in nodes/nodes.h postgres source */
	switch (nodeTag(node))
	{
		/*
		 * primitive nodes (primnodes.h)
		 */
		NODE_CASE(Alias);
		NODE_CASE(RangeVar);
		NODE_CASE(TableFunc);
		NODE_CASE(IntoClause);
#if PG15_LT
		/* PG15 removed T_Expr nodetag because it's an abstract type. */
		NODE_CASE(Expr);
#endif
		NODE_CASE(Var);
		NODE_CASE(Const);
		NODE_CASE(Param);
		NODE_CASE(Aggref);
		NODE_CASE(GroupingFunc);
		NODE_CASE(WindowFunc);
		NODE_CASE(SubscriptingRef);
		NODE_CASE(FuncExpr);
		NODE_CASE(NamedArgExpr);
		NODE_CASE(OpExpr);
		NODE_CASE(DistinctExpr);
		NODE_CASE(NullIfExpr);
		NODE_CASE(ScalarArrayOpExpr);
		NODE_CASE(BoolExpr);
		NODE_CASE(SubLink);
		NODE_CASE(SubPlan);
		NODE_CASE(AlternativeSubPlan);
		NODE_CASE(FieldSelect);
		NODE_CASE(FieldStore);
		NODE_CASE(RelabelType);
		NODE_CASE(CoerceViaIO);
		NODE_CASE(ArrayCoerceExpr);
		NODE_CASE(ConvertRowtypeExpr);
		NODE_CASE(CollateExpr);
		NODE_CASE(CaseExpr);
		NODE_CASE(CaseWhen);
		NODE_CASE(CaseTestExpr);
		NODE_CASE(ArrayExpr);
		NODE_CASE(RowExpr);
		NODE_CASE(RowCompareExpr);
		NODE_CASE(CoalesceExpr);
		NODE_CASE(MinMaxExpr);
		NODE_CASE(SQLValueFunction);
		NODE_CASE(XmlExpr);
		NODE_CASE(NullTest);
		NODE_CASE(BooleanTest);
		NODE_CASE(CoerceToDomain);
		NODE_CASE(CoerceToDomainValue);
		NODE_CASE(SetToDefault);
		NODE_CASE(CurrentOfExpr);
		NODE_CASE(NextValueExpr);
		NODE_CASE(InferenceElem);
		NODE_CASE(TargetEntry);
		NODE_CASE(RangeTblRef);
		NODE_CASE(JoinExpr);
		NODE_CASE(FromExpr);
		NODE_CASE(OnConflictExpr);

		/*
		 * plan nodes (plannodes.h)
		 */
#if PG16_LT
		NODE_CASE(Plan);
		NODE_CASE(Scan);
		NODE_CASE(Join);
#endif
		NODE_CASE(Result);
		NODE_CASE(ProjectSet);
		NODE_CASE(ModifyTable);
		NODE_CASE(Append);
		NODE_CASE(MergeAppend);
		NODE_CASE(RecursiveUnion);
		NODE_CASE(BitmapAnd);
		NODE_CASE(BitmapOr);
		NODE_CASE(SeqScan);
		NODE_CASE(SampleScan);
		NODE_CASE(IndexScan);
		NODE_CASE(IndexOnlyScan);
		NODE_CASE(BitmapIndexScan);
		NODE_CASE(BitmapHeapScan);
		NODE_CASE(TidScan);
		NODE_CASE(SubqueryScan);
		NODE_CASE(FunctionScan);
		NODE_CASE(ValuesScan);
		NODE_CASE(TableFuncScan);
		NODE_CASE(CteScan);
		NODE_CASE(NamedTuplestoreScan);
		NODE_CASE(WorkTableScan);
		NODE_CASE(ForeignScan);
		NODE_CASE(CustomScan);
		NODE_CASE(NestLoop);
		NODE_CASE(MergeJoin);
		NODE_CASE(HashJoin);
		NODE_CASE(Material);
		NODE_CASE(Sort);
		NODE_CASE(Group);
		NODE_CASE(Agg);
		NODE_CASE(WindowAgg);
		NODE_CASE(Unique);
		NODE_CASE(Gather);
		NODE_CASE(GatherMerge);
		NODE_CASE(Hash);
		NODE_CASE(SetOp);
		NODE_CASE(LockRows);
		NODE_CASE(Limit);

		/*
		 * planner nodes (pathnodes.h)
		 */
		NODE_CASE(IndexPath);
		NODE_CASE(BitmapHeapPath);
		NODE_CASE(BitmapAndPath);
		NODE_CASE(BitmapOrPath);
		NODE_CASE(TidPath);
		NODE_CASE(SubqueryScanPath);
		NODE_CASE(ForeignPath);
		NODE_CASE(NestPath);
		NODE_CASE(MergePath);
		NODE_CASE(HashPath);
		NODE_CASE(AppendPath);
		NODE_CASE(MergeAppendPath);
		NODE_CASE(GroupResultPath);
		NODE_CASE(MaterialPath);
		NODE_CASE(UniquePath);
		NODE_CASE(GatherPath);
		NODE_CASE(GatherMergePath);
		NODE_CASE(ProjectionPath);
		NODE_CASE(ProjectSetPath);
		NODE_CASE(SortPath);
		NODE_CASE(GroupPath);
		NODE_CASE(UpperUniquePath);
		NODE_CASE(AggPath);
		NODE_CASE(GroupingSetsPath);
		NODE_CASE(MinMaxAggPath);
		NODE_CASE(WindowAggPath);
		NODE_CASE(SetOpPath);
		NODE_CASE(RecursiveUnionPath);
		NODE_CASE(LockRowsPath);
		NODE_CASE(ModifyTablePath);
		NODE_CASE(LimitPath);

		case T_Path:
			switch (castNode(Path, node)->pathtype)
			{
				NODE_CASE(SeqScan);
				NODE_CASE(SampleScan);
				NODE_CASE(SubqueryScan);
				NODE_CASE(FunctionScan);
				NODE_CASE(TableFuncScan);
				NODE_CASE(ValuesScan);
				NODE_CASE(CteScan);
				NODE_CASE(WorkTableScan);
				default:
					return psprintf("Path (%d)", castNode(Path, node)->pathtype);
			}

		case T_CustomPath:
			return psprintf("CustomPath (%s)", castNode(CustomPath, node)->methods->CustomName);

		default:
			return psprintf("Node (%d)", nodeTag(node));
	}
}

/*
 * Implementation marked unused in PostgreSQL lsyscache.c
 */
int
ts_get_relnatts(Oid relid)
{
	HeapTuple tp;
	Form_pg_class reltup;
	int result;

	tp = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tp))
		return InvalidAttrNumber;

	reltup = (Form_pg_class) GETSTRUCT(tp);
	result = reltup->relnatts;

	ReleaseSysCache(tp);
	return result;
}

/*
 * Wrap AlterTableInternal() for event trigger handling.
 *
 * AlterTableInternal can be called as a utility command, which is common in a
 * SQL function that alters a table in some form when called in the form
 * SELECT <cmd> INTO <table>. This is transformed into a process utility
 * command (CREATE TABLE AS), which expects an event trigger context to be
 * set up.
 *
 * The "cmd" parameter can be set to a higher-level command that caused the
 * alter table to occur. If "cmd" is set to NULL, the "cmds" list will be used
 * instead.
 */
void
ts_alter_table_with_event_trigger(Oid relid, Node *cmd, List *cmds, bool recurse)
{
	if (cmd == NULL)
		cmd = (Node *) cmds;

	EventTriggerAlterTableStart(cmd);
	AlterTableInternal(relid, cmds, recurse);
	EventTriggerAlterTableEnd();
}

void
ts_copy_relation_acl(const Oid source_relid, const Oid target_relid, const Oid owner_id)
{
	HeapTuple source_tuple;
	bool is_null;
	Datum acl_datum;
	Relation class_rel;

	/* We open it here since there is no point in trying to update the tuples
	 * if we cannot open the Relation catalog table */
	class_rel = table_open(RelationRelationId, RowExclusiveLock);

	source_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(source_relid));
	Assert(HeapTupleIsValid(source_tuple));

	/* We only bother about setting the ACL if the source relation ACL is
	 * non-null */
	acl_datum = SysCacheGetAttr(RELOID, source_tuple, Anum_pg_class_relacl, &is_null);

	if (!is_null)
	{
		HeapTuple target_tuple, newtuple;
		Datum new_val[Natts_pg_class] = { 0 };
		bool new_null[Natts_pg_class] = { false };
		bool new_repl[Natts_pg_class] = { false };
		Acl *acl = DatumGetAclP(acl_datum);

		new_repl[AttrNumberGetAttrOffset(Anum_pg_class_relacl)] = true;
		new_val[AttrNumberGetAttrOffset(Anum_pg_class_relacl)] = PointerGetDatum(acl);

		/* Find the tuple for the target in `pg_class` */
		target_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(target_relid));
		Assert(HeapTupleIsValid(target_tuple));

		/* Update the relacl for the target tuple to use the acl from the source */
		newtuple = heap_modify_tuple(target_tuple,
									 RelationGetDescr(class_rel),
									 new_val,
									 new_null,
									 new_repl);
		CatalogTupleUpdate(class_rel, &newtuple->t_self, newtuple);

		/* We need to update the shared dependencies as well to indicate that
		 * the target is dependent on any roles that the source is
		 * dependent on. */
		Oid *newmembers;
		int nnewmembers = aclmembers(acl, &newmembers);

		/* The list of old members is intentionally empty since we are using
		 * updateAclDependencies to set the ACL for the target. We can use NULL
		 * because getOidListDiff, which is called from updateAclDependencies,
		 * can handle that. */
		updateAclDependencies(RelationRelationId,
							  target_relid,
							  0,
							  owner_id,
							  0,
							  NULL,
							  nnewmembers,
							  newmembers);

		heap_freetuple(newtuple);
		ReleaseSysCache(target_tuple);
	}

	ReleaseSysCache(source_tuple);
	table_close(class_rel, RowExclusiveLock);
}

/*
 * Map attno from source relation to target relation by column name
 */
AttrNumber
ts_map_attno(Oid src_rel, Oid dst_rel, AttrNumber attno)
{
	char *attname = get_attname(src_rel, attno, false);
	AttrNumber dst_attno = get_attnum(dst_rel, attname);

	/*
	 * For any chunk mappings we do this should never happen.
	 */
	if (dst_attno == InvalidAttrNumber)
		elog(ERROR,
			 "could not map attribute number from relation \"%s\" to \"%s\" for column \"%s\"",
			 get_rel_name(src_rel),
			 get_rel_name(dst_rel),
			 attname);

	pfree(attname);
	return dst_attno;
}

bool
ts_relation_has_tuples(Relation rel)
{
	TableScanDesc scandesc = table_beginscan(rel, GetActiveSnapshot(), 0, NULL);
	TupleTableSlot *slot =
		MakeSingleTupleTableSlot(RelationGetDescr(rel), table_slot_callbacks(rel));
	bool hastuples = table_scan_getnextslot(scandesc, ForwardScanDirection, slot);

	table_endscan(scandesc);
	ExecDropSingleTupleTableSlot(slot);
	return hastuples;
}

bool
ts_table_has_tuples(Oid table_relid, LOCKMODE lockmode)
{
	Relation rel = table_open(table_relid, lockmode);
	bool hastuples = ts_relation_has_tuples(rel);

	table_close(rel, lockmode);
	return hastuples;
}

/*
 * This is copied from PostgreSQL 16.0 since versions before 16.0 does not
 * support lists for privileges.
 */
static AclMode
ts_convert_any_priv_string(text *priv_type_text, const priv_map *privileges)
{
	AclMode result = 0;
	char *priv_type = text_to_cstring(priv_type_text);
	char *chunk;
	char *next_chunk;

	/* We rely on priv_type being a private, modifiable string */
	for (chunk = priv_type; chunk; chunk = next_chunk)
	{
		int chunk_len;
		const priv_map *this_priv;

		/* Split string at commas */
		next_chunk = strchr(chunk, ',');
		if (next_chunk)
			*next_chunk++ = '\0';

		/* Drop leading/trailing whitespace in this chunk */
		while (*chunk && isspace((unsigned char) *chunk))
			chunk++;
		chunk_len = strlen(chunk);
		while (chunk_len > 0 && isspace((unsigned char) chunk[chunk_len - 1]))
			chunk_len--;
		chunk[chunk_len] = '\0';

		/* Match to the privileges list */
		for (this_priv = privileges; this_priv->name; this_priv++)
		{
			if (pg_strcasecmp(this_priv->name, chunk) == 0)
			{
				result |= this_priv->value;
				break;
			}
		}
		if (!this_priv->name)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unrecognized privilege type: \"%s\"", chunk)));
	}

	pfree(priv_type);
	return result;
}

/*
 * This is copied from PostgreSQL 16.0 since versions before 16.0 does not
 * support lists for privileges but we need that.
 */
Datum
ts_makeaclitem(PG_FUNCTION_ARGS)
{
	Oid grantee = PG_GETARG_OID(0);
	Oid grantor = PG_GETARG_OID(1);
	text *privtext = PG_GETARG_TEXT_PP(2);
	bool goption = PG_GETARG_BOOL(3);
	AclItem *result;
	AclMode priv;
	static const priv_map any_priv_map[] = {
		{ "SELECT", ACL_SELECT },
		{ "INSERT", ACL_INSERT },
		{ "UPDATE", ACL_UPDATE },
		{ "DELETE", ACL_DELETE },
		{ "TRUNCATE", ACL_TRUNCATE },
		{ "REFERENCES", ACL_REFERENCES },
		{ "TRIGGER", ACL_TRIGGER },
		{ "EXECUTE", ACL_EXECUTE },
		{ "USAGE", ACL_USAGE },
		{ "CREATE", ACL_CREATE },
		{ "TEMP", ACL_CREATE_TEMP },
		{ "TEMPORARY", ACL_CREATE_TEMP },
		{ "CONNECT", ACL_CONNECT },
#if PG16_GE
		{ "SET", ACL_SET },
		{ "ALTER SYSTEM", ACL_ALTER_SYSTEM },
#endif
#if PG17_GE
		{ "MAINTAIN", ACL_MAINTAIN },
#endif
		{ "RULE", 0 }, /* ignore old RULE privileges */
		{ NULL, 0 }
	};

	priv = ts_convert_any_priv_string(privtext, any_priv_map);

	result = (AclItem *) palloc(sizeof(AclItem));

	result->ai_grantee = grantee;
	result->ai_grantor = grantor;

	ACLITEM_SET_PRIVS_GOPTIONS(*result, priv, (goption ? priv : ACL_NO_RIGHTS));

	PG_RETURN_ACLITEM_P(result);
}

/*
 * heap_form_tuple using NullableDatum array instead of two arrays for
 * values and nulls
 */
HeapTuple
ts_heap_form_tuple(TupleDesc tupleDescriptor, NullableDatum *datums)
{
	int numElements = tupleDescriptor->natts;
	Datum *values = palloc0(sizeof(Datum) * numElements);
	bool *nulls = palloc0(sizeof(bool) * numElements);

	for (int i = 0; i < numElements; i++)
	{
		values[i] = datums[i].value;
		nulls[i] = datums[i].isnull;
	}

	return heap_form_tuple(tupleDescriptor, values, nulls);
}

/*
 * To not introduce shared object dependencies on functions in extension update
 * scripts we use this stub function as placeholder whenever we need to reference
 * c functions in the update scripts.
 */
TS_FUNCTION_INFO_V1(ts_update_placeholder);
Datum
ts_update_placeholder(PG_FUNCTION_ARGS)
{
	elog(ERROR, "this stub function is used only as placeholder during extension updates");
	PG_RETURN_NULL();
}

/*
 * Get relation information from the syscache in one call.
 *
 * Returns relkind and access method used. Both are non-optional.
 */
void
ts_get_rel_info(Oid relid, Oid *amoid, char *relkind)
{
	HeapTuple tuple;
	Form_pg_class cform;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u", relid);

	cform = (Form_pg_class) GETSTRUCT(tuple);
	*amoid = cform->relam;
	*relkind = cform->relkind;
	ReleaseSysCache(tuple);
}

/*
 * Get relation information from the syscache in one call.
 *
 * Returns relid, relkind and access method used. All are non-optional.
 */
void
ts_get_rel_info_by_name(const char *relnamespace, const char *relname, Oid *relid, Oid *amoid,
						char *relkind)
{
	HeapTuple tuple;
	Form_pg_class cform;
	Oid namespaceoid = get_namespace_oid(relnamespace, false);

	tuple = SearchSysCache2(RELNAMENSP, PointerGetDatum(relname), ObjectIdGetDatum(namespaceoid));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %s.%s", relnamespace, relname);

	cform = (Form_pg_class) GETSTRUCT(tuple);
	*relid = cform->oid;
	*amoid = cform->relam;
	*relkind = cform->relkind;
	ReleaseSysCache(tuple);
}

static Oid hypercore_amoid = InvalidOid;

bool
ts_is_hypercore_am(Oid amoid)
{
	if (!OidIsValid(hypercore_amoid))
		hypercore_amoid = get_table_am_oid("hyperstore", true);

	if (!OidIsValid(amoid) || !OidIsValid(hypercore_amoid))
		return false;

	return amoid == hypercore_amoid;
}

/* this function fills in a jsonb with the non-null fields of
 the error data and also includes the proc name and schema in the jsonb
 we include these here to avoid adding these fields to the table */
Jsonb *
ts_errdata_to_jsonb(ErrorData *edata, Name proc_schema, Name proc_name)
{
	JsonbParseState *parse_state = NULL;
	pushJsonbValue(&parse_state, WJB_BEGIN_OBJECT, NULL);
	if (edata->sqlerrcode)
		ts_jsonb_add_str(parse_state, "sqlerrcode", unpack_sql_state(edata->sqlerrcode));
	if (edata->message)
		ts_jsonb_add_str(parse_state, "message", edata->message);
	if (edata->detail)
		ts_jsonb_add_str(parse_state, "detail", edata->detail);
	if (edata->hint)
		ts_jsonb_add_str(parse_state, "hint", edata->hint);
	if (edata->filename)
		ts_jsonb_add_str(parse_state, "filename", edata->filename);
	if (edata->lineno)
		ts_jsonb_add_int32(parse_state, "lineno", edata->lineno);
	if (edata->funcname)
		ts_jsonb_add_str(parse_state, "funcname", edata->funcname);
	if (edata->domain)
		ts_jsonb_add_str(parse_state, "domain", edata->domain);
	if (edata->context_domain)
		ts_jsonb_add_str(parse_state, "context_domain", edata->context_domain);
	if (edata->context)
		ts_jsonb_add_str(parse_state, "context", edata->context);
	if (edata->schema_name)
		ts_jsonb_add_str(parse_state, "schema_name", edata->schema_name);
	if (edata->table_name)
		ts_jsonb_add_str(parse_state, "table_name", edata->table_name);
	if (edata->column_name)
		ts_jsonb_add_str(parse_state, "column_name", edata->column_name);
	if (edata->datatype_name)
		ts_jsonb_add_str(parse_state, "datatype_name", edata->datatype_name);
	if (edata->constraint_name)
		ts_jsonb_add_str(parse_state, "constraint_name", edata->constraint_name);
	if (edata->internalquery)
		ts_jsonb_add_str(parse_state, "internalquery", edata->internalquery);
	if (edata->detail_log)
		ts_jsonb_add_str(parse_state, "detail_log", edata->detail_log);
	if (strlen(NameStr(*proc_schema)) > 0)
		ts_jsonb_add_str(parse_state, "proc_schema", NameStr(*proc_schema));
	if (strlen(NameStr(*proc_name)) > 0)
		ts_jsonb_add_str(parse_state, "proc_name", NameStr(*proc_name));
	/* we add the schema qualified name here as well*/
	JsonbValue *result = pushJsonbValue(&parse_state, WJB_END_OBJECT, NULL);
	return JsonbValueToJsonb(result);
}
