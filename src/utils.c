/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/genam.h>
#include <access/heapam.h>
#include <access/htup_details.h>
#include <access/htup.h>
#include <access/reloptions.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_cast.h>
#include <catalog/pg_inherits.h>
#include <catalog/pg_operator.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <funcapi.h>
#include <nodes/makefuncs.h>
#include <parser/parse_coerce.h>
#include <parser/parse_func.h>
#include <parser/scansup.h>
#include <utils/builtins.h>
#include <utils/catcache.h>
#include <utils/date.h>
#include <utils/fmgroids.h>
#include <utils/fmgrprotos.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "chunk.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "utils.h"
#include "time_utils.h"

TS_FUNCTION_INFO_V1(ts_pg_timestamp_to_unix_microseconds);

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
ts_time_value_to_internal_or_infinite(Datum time_val, Oid type_oid,
									  TimevalInfinity *is_infinite_out)
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
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalNegInfinity;
					return PG_INT64_MIN;
				}
				else
				{
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalPosInfinity;
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
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalNegInfinity;
					return PG_INT64_MIN;
				}
				else
				{
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalPosInfinity;
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
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalNegInfinity;
					return PG_INT64_MIN;
				}
				else
				{
					if (is_infinite_out != NULL)
						*is_infinite_out = TimevalPosInfinity;
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
#if PG14_GE
											false, /* include_out_arguments */
#endif
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

#if PG12
/*
 * Find an equivalence class member expression, all of whose Vars, come from
 * the indicated relation.
 *
 * This function has been copied from find_em_expr_for_rel in
 * contrib/postgres_fdw/postgres_fdw.c in postgres source.
 * This function was moved to postgres main in PG13 so we only need this
 * backport for PG12 in later versions we will use the postgres implementation.
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
ts_get_integer_now_func(const Dimension *open_dim)
{
	Oid rettype;
	Oid now_func;
	Oid argtypes[] = { 0 };

	rettype = ts_dimension_get_partition_type(open_dim);

	Assert(IS_INTEGER_TYPE(rettype));

	if (strlen(NameStr(open_dim->fd.integer_now_func)) == 0 &&
		strlen(NameStr(open_dim->fd.integer_now_func_schema)) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION), (errmsg("integer_now function not set"))));

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

	AssertArg(IS_INTEGER_TYPE(time_dim_type));

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

	Oid now_func = ts_get_integer_now_func(dim);
	if (!OidIsValid(now_func))
		elog(ERROR, "could not find valid integer_now function for hypertable");

	int64 res = ts_sub_integer_from_now(lag, partitioning_type, now_func);
	ts_cache_release(hcache);
	return Int64GetDatum(res);
}

RelationSize
ts_relation_size(Oid relid)
{
	int64 tot_size;
	int i = 0;
	RelationSize ret;
	Datum oid = ObjectIdGetDatum(relid);
	static const char *filtyp[] = { "main", "init", "fsm", "vm" };

	/*
	 * For heap get size from fsm, vm, init and main as this is included in
	 * pg_table_size calculation
	 */
	ret.heap_size = 0;

	for (i = 0; i < lengthof(filtyp); i++)
	{
		ret.heap_size += DatumGetInt64(
			DirectFunctionCall2(pg_relation_size, oid, CStringGetTextDatum(filtyp[i])));
	}

	ret.index_size = DatumGetInt64(DirectFunctionCall1(pg_indexes_size, relid));
	tot_size = DatumGetInt64(DirectFunctionCall1(pg_table_size, relid));
	ret.toast_size = tot_size - ret.heap_size;

	return ret;
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
		 * plan nodes (plannodes.h)
		 */
		NODE_CASE(Plan);
		NODE_CASE(Result);
		NODE_CASE(ProjectSet);
		NODE_CASE(ModifyTable);
		NODE_CASE(Append);
		NODE_CASE(MergeAppend);
		NODE_CASE(RecursiveUnion);
		NODE_CASE(BitmapAnd);
		NODE_CASE(BitmapOr);
		NODE_CASE(Scan);
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
		NODE_CASE(Join);
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
