/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <funcapi.h>
#include <utils/typcache.h>
#include <parser/parse_type.h>
#include <access/htup_details.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <miscadmin.h>
#include <utils/elog.h>

#include "interval.h"
#include "hypertable.h"

#include "dimension.h"
#include "hypertable_cache.h"
#include "cache.h"
#include "utils.h"
#include "errors.h"
#include "compat.h"

#if !PG96
#include <utils/fmgrprotos.h>
#else
#include <utils/date.h>
#endif

/* This function deforms its input argument `ts_interval_datum` into `FormData_ts_interval`
 * assuming it was read from a postgres table and so the datum represents a TupleHeader
 */
FormData_ts_interval *
ts_interval_from_tuple(Datum interval)
{
	bool isnull[Natts_ts_interval];
	Datum values[Natts_ts_interval];
	HeapTupleHeader th;
	HeapTupleData tuple;
	FormData_ts_interval *invl;

	Oid rowType;
	int32 rowTypmod;
	TupleDesc rowdesc;

	th = DatumGetHeapTupleHeader(interval);
	rowType = HeapTupleHeaderGetTypeId(th);
	rowTypmod = HeapTupleHeaderGetTypMod(th);
	rowdesc = lookup_rowtype_tupdesc(rowType, rowTypmod);

	tuple.t_len = HeapTupleHeaderGetDatumLength(th);
	ItemPointerSetInvalid(&(tuple.t_self));
	tuple.t_tableOid = InvalidOid;
	tuple.t_data = th;

	heap_deform_tuple(&tuple, rowdesc, values, isnull);
	// lookup_rowtype_tupdesc gives a ref counted pointer
	DecrTupleDescRefCount(rowdesc);

	invl = palloc0(sizeof(FormData_ts_interval));

	Assert(!isnull[AttrNumberGetAttrOffset(Anum_is_time_interval)]);

	invl->is_time_interval = values[AttrNumberGetAttrOffset(Anum_is_time_interval)];
	if (invl->is_time_interval)
	{
		Assert(!isnull[AttrNumberGetAttrOffset(Anum_time_interval)]);
		Assert(isnull[AttrNumberGetAttrOffset(Anum_integer_interval)]);
		invl->time_interval =
			*DatumGetIntervalP(values[AttrNumberGetAttrOffset(Anum_time_interval)]);
	}
	else
	{
		Assert(isnull[AttrNumberGetAttrOffset(Anum_time_interval)]);
		Assert(!isnull[AttrNumberGetAttrOffset(Anum_integer_interval)]);
		invl->integer_interval =
			DatumGetInt64(values[AttrNumberGetAttrOffset(Anum_integer_interval)]);
	}

	return invl;
}

/* This function deforms its input `interval` argument into a FormData_ts_interval assuming interval
 * was given as a SQL function argument and represents data of type `interval_type` and should
 * represent an interval on hypertable with oid `relid`
 */
FormData_ts_interval *
ts_interval_from_sql_input(Oid relid, Datum interval, Oid interval_type, char *parameter_name,
						   char *caller_name)
{
	Hypertable *hypertable;
	Cache *hcache;
	FormData_ts_interval *invl = palloc0(sizeof(FormData_ts_interval));
	Oid partitioning_type;
	Dimension *open_dim;

	ts_hypertable_permissions_check(relid, GetUserId());

	hcache = ts_hypertable_cache_pin();
	hypertable = ts_hypertable_cache_get_entry(hcache, relid);
	/* First verify that the hypertable corresponds to a valid table */
	if (hypertable == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("could not add drop_chunks policy because \"%s\" is not a hypertable",
						get_rel_name(relid))));
	/* validate that the open dimension uses a time type */
	open_dim = hyperspace_get_open_dimension(hypertable->space, 0);

	if (NULL == open_dim)
		elog(ERROR, "internal error: no open dimension found while parsing interval");

	partitioning_type = ts_dimension_get_partition_type(open_dim);
	ts_cache_release(hcache);

	switch (interval_type)
	{
		case INTERVALOID:
			if (IS_INTEGER_TYPE(partitioning_type))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid parameter value for %s", parameter_name),
						 errhint("INTERVAL time duration cannot be used with hypertables with "
								 "integer-based time dimensions")));
			ts_dimension_open_typecheck(INTERVALOID, partitioning_type, caller_name);
			invl->is_time_interval = true;
			invl->time_interval = *DatumGetIntervalP(interval);
			break;
		case INT2OID:
		case INT4OID:
		case INT8OID:
			if (!IS_INTEGER_TYPE(partitioning_type))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid parameter value for %s", parameter_name),
						 errhint("integer-based time duration cannot be used with hypertables with "
								 "a timestamp-based time dimensions")));

			if (strlen(NameStr(open_dim->fd.integer_now_func)) == 0 ||
				strlen(NameStr(open_dim->fd.integer_now_func_schema)) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("integer_now_func not set on hypertable %s", get_rel_name(relid))));

			invl->is_time_interval = false;
			invl->integer_interval = ts_time_value_to_internal(interval, interval_type);

			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid type for parameter %s in %s", parameter_name, caller_name)));
	}

	return invl;
}

HeapTuple
ts_interval_form_heaptuple(FormData_ts_interval *invl)
{
	Oid typeid;
	TupleDesc olderthan_tupdesc;
	Datum values[Natts_ts_interval];
	bool nulls[Natts_ts_interval] = { false };

	values[AttrNumberGetAttrOffset(Anum_is_time_interval)] = BoolGetDatum(invl->is_time_interval);

	if (invl->is_time_interval)
	{
		nulls[AttrNumberGetAttrOffset(Anum_integer_interval)] = true;
		values[AttrNumberGetAttrOffset(Anum_time_interval)] =
			IntervalPGetDatum(&invl->time_interval);
	}
	else
	{
		nulls[AttrNumberGetAttrOffset(Anum_time_interval)] = true;
		values[AttrNumberGetAttrOffset(Anum_integer_interval)] =
			Int64GetDatum(invl->integer_interval);
	}

	typeid =
		typenameTypeId(NULL, typeStringToTypeName(CATALOG_SCHEMA_NAME "." TS_INTERVAL_TYPE_NAME));
	olderthan_tupdesc = lookup_type_cache(typeid, -1)->tupDesc;

	olderthan_tupdesc = BlessTupleDesc(olderthan_tupdesc);
	return heap_form_tuple(olderthan_tupdesc, values, nulls);
}

bool
ts_interval_equal(FormData_ts_interval *invl1, FormData_ts_interval *invl2)
{
	AssertArg(invl1 != NULL);
	AssertArg(invl2 != NULL);

	if (invl1->is_time_interval != invl2->is_time_interval)
		return false;

	if (invl1->is_time_interval &&
		!DatumGetBool(DirectFunctionCall2(interval_eq,
										  IntervalPGetDatum(&invl1->time_interval),
										  IntervalPGetDatum(&invl2->time_interval))))
		return false;

	if (!invl1->is_time_interval && invl1->integer_interval != invl2->integer_interval)
	{
		return false;
	}

	return true;
}
void
ts_interval_now_func_validate(Oid now_func_oid, Oid open_dim_type)
{
	HeapTuple tuple;
	Form_pg_proc now_func;

	/* this function should only be called for hypertabes with integer open time dimension */
	Assert(IS_INTEGER_TYPE(open_dim_type));

	if (!OidIsValid(now_func_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION), (errmsg("invalid integer_now function"))));

	tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(now_func_oid));
	if (!HeapTupleIsValid(tuple))
	{
		ReleaseSysCache(tuple);
		ereport(ERROR,
				(errcode(ERRCODE_NO_DATA_FOUND),
				 errmsg("cache lookup failed for function %u", now_func_oid)));
	}

	now_func = (Form_pg_proc) GETSTRUCT(tuple);

	if ((now_func->provolatile != PROVOLATILE_IMMUTABLE &&
		 now_func->provolatile != PROVOLATILE_STABLE) ||
		now_func->pronargs != 0)
	{
		ReleaseSysCache(tuple);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("integer_now_func must take no arguments and it must be STABLE")));
	}

	if (now_func->prorettype != open_dim_type)
	{
		ReleaseSysCache(tuple);
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("return type of integer_now_func must be the same as "
						"the type of the time partitioning column of the hypertable")));
	}
	ReleaseSysCache(tuple);
}

static Datum
ts_interval_from_now_func_get_datum(int64 interval, Oid time_dim_type, Oid now_func)
{
	Datum now;
	int64 res;

	AssertArg(IS_INTEGER_TYPE(time_dim_type));

	ts_interval_now_func_validate(now_func, time_dim_type);
	now = OidFunctionCall0(now_func);

	switch (time_dim_type)
	{
		case INT2OID:
			res = DatumGetInt16(now) - interval;
			if (res < PG_INT16_MIN || res > PG_INT16_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW), errmsg("ts_interval overflow")));
			return Int16GetDatum(res);
		case INT4OID:
			res = DatumGetInt32(now) - interval;
			if (res < PG_INT32_MIN || res > PG_INT32_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW), errmsg("ts_interval overflow")));
			return Int32GetDatum(res);
		case INT8OID:
			res = DatumGetInt64(now) - interval;
			if (res > DatumGetInt64(now))
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW), errmsg("ts_interval overflow")));
			return Int64GetDatum(res);
		default:
			pg_unreachable();
	}
}

static bool
noarg_integer_now_func_filter(Form_pg_proc form, void *arg)
{
	Oid *rettype = arg;

	return form->pronargs == 0 && form->prorettype == *rettype;
}

/* maybe this can be exported later if other parts of the code need
 * to access the integer_now_func
 */
static Oid
get_integer_now_func(Dimension *open_dim)
{
	Oid rettype;
	Oid now_func;

	rettype = ts_dimension_get_partition_type(open_dim);

	Assert(IS_INTEGER_TYPE(rettype));

	now_func = ts_lookup_proc_filtered(NameStr(open_dim->fd.integer_now_func_schema),
									   NameStr(open_dim->fd.integer_now_func),
									   NULL,
									   noarg_integer_now_func_filter,
									   &rettype);
	return now_func;
}

/*
 * Convert the difference of interval and current timestamp to internal representation
 * This function interprets the interval as distance in time dimension to the past.
 * Depending on the type of hypertable time column, the function applies the
 * necessary granularity to now() - interval and returns the resulting
 * datum (which incapsulates data of time column type)
 */
Datum
ts_interval_subtract_from_now(FormData_ts_interval *invl, Dimension *open_dim)
{
	Oid type_oid;
	AssertArg(invl != NULL);
	AssertArg(open_dim != NULL);

	type_oid = ts_dimension_get_partition_type(open_dim);

	if (invl->is_time_interval)
	{
		Datum res = TimestampTzGetDatum(GetCurrentTimestamp());

		switch (type_oid)
		{
			case TIMESTAMPOID:
				res = DirectFunctionCall1(timestamptz_timestamp, res);
				res = DirectFunctionCall2(timestamp_mi_interval,
										  res,
										  IntervalPGetDatum(&invl->time_interval));

				return res;
			case TIMESTAMPTZOID:
				res = DirectFunctionCall2(timestamptz_mi_interval,
										  res,
										  IntervalPGetDatum(&invl->time_interval));

				return res;
			case DATEOID:
				res = DirectFunctionCall1(timestamptz_timestamp, res);
				res = DirectFunctionCall2(timestamp_mi_interval,
										  res,
										  IntervalPGetDatum(&invl->time_interval));
				res = DirectFunctionCall1(timestamp_date, res);

				return res;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("unknown time type OID %d", type_oid)));
		}
	}
	else
	{
		Oid now_func = get_integer_now_func(open_dim);
		ts_interval_now_func_validate(now_func, type_oid);

		if (InvalidOid == now_func)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("integer_now function must be set")));

		return ts_interval_from_now_func_get_datum(invl->integer_interval, type_oid, now_func);
	}
	/* suppress compiler warnings on MSVC */
	pg_unreachable();
	return 0;
}

TS_FUNCTION_INFO_V1(ts_valid_ts_interval);
Datum
ts_valid_ts_interval(PG_FUNCTION_ARGS)
{
	/* this function does all the necessary validation and if successfull,
	returns the interval which is not necessary here */
	ts_interval_from_tuple(PG_GETARG_DATUM(0));
	PG_RETURN_BOOL(true);
}
