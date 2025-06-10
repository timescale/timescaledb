/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <commands/view.h>
#include <storage/lmgr.h>
#include <utils/acl.h>
#include <utils/regproc.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "extension.h"
#include "guc.h"
#include "time_bucket.h"
#include "utils.h"

enum
{
	Anum_cagg_validate_query_valid = 1,
	Anum_cagg_validate_query_error_level,
	Anum_cagg_validate_query_error_code,
	Anum_cagg_validate_query_error_message,
	Anum_cagg_validate_query_error_detail,
	Anum_cagg_validate_query_error_hint,
	_Anum_cagg_validate_query_max
};

#define Natts_cagg_validate_query (_Anum_cagg_validate_query_max - 1)
#define ORIGIN_PARAMETER_NAME "origin"

static Datum
create_cagg_validate_query_datum(TupleDesc tupdesc, const bool is_valid_query, ErrorData *edata)
{
	NullableDatum datums[Natts_cagg_validate_query] = { { 0 } };
	HeapTuple tuple;

	tupdesc = BlessTupleDesc(tupdesc);

	ts_datum_set_bool(Anum_cagg_validate_query_valid, datums, is_valid_query, false);
	ts_datum_set_text_from_cstring(Anum_cagg_validate_query_error_level,
								   datums,
								   edata->elevel > 0 ? error_severity(edata->elevel) : NULL);
	ts_datum_set_text_from_cstring(Anum_cagg_validate_query_error_code,
								   datums,
								   edata->sqlerrcode > 0 ? unpack_sql_state(edata->sqlerrcode) :
														   NULL);
	ts_datum_set_text_from_cstring(Anum_cagg_validate_query_error_message,
								   datums,
								   edata->message ? edata->message : NULL);
	ts_datum_set_text_from_cstring(Anum_cagg_validate_query_error_detail,
								   datums,
								   edata->detail ? edata->detail : NULL);
	ts_datum_set_text_from_cstring(Anum_cagg_validate_query_error_hint,
								   datums,
								   edata->hint ? edata->hint : NULL);

	Assert(tupdesc->natts == Natts_cagg_validate_query);
	tuple = ts_heap_form_tuple(tupdesc, datums);
	return HeapTupleGetDatum(tuple);
}

Datum
continuous_agg_validate_query(PG_FUNCTION_ARGS)
{
	text *query_text = PG_GETARG_TEXT_P(0);
	char *sql;
	volatile bool is_valid_query = false;
	Datum datum_sql;
	TupleDesc tupdesc;
	ErrorData *edata;
	MemoryContext oldcontext = CurrentMemoryContext;

	/* Change $1, $2 ... placeholders to NULL constant. This is necessary to make parser happy */
	sql = text_to_cstring(query_text);
	elog(DEBUG1, "sql: %s", sql);

	datum_sql = CStringGetTextDatum(sql);
	datum_sql = DirectFunctionCall4Coll(textregexreplace,
										C_COLLATION_OID,
										datum_sql,
										CStringGetTextDatum("\\$[0-9]+"),
										CStringGetTextDatum("NULL"),
										CStringGetTextDatum("g"));
	sql = text_to_cstring(DatumGetTextP(datum_sql));
	elog(DEBUG1, "sql: %s", sql);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "function returning record called in context that cannot accept type record");

	PG_TRY();
	{
		List *tree;
		Node *node;
		RawStmt *rawstmt;
		ParseState *pstate;
		Query *query;

		edata = (ErrorData *) palloc0(sizeof(ErrorData));
		edata->message = NULL;
		edata->detail = NULL;
		edata->hint = NULL;

		tree = pg_parse_query(sql);

		if (tree == NIL)
		{
			edata->elevel = ERROR;
			edata->sqlerrcode = ERRCODE_INTERNAL_ERROR;
			edata->message = "failed to parse query";
		}
		else if (list_length(tree) > 1)
		{
			edata->elevel = WARNING;
			edata->sqlerrcode = ERRCODE_FEATURE_NOT_SUPPORTED;
			edata->message = "multiple statements are not supported";
		}
		else
		{
			node = linitial(tree);
			rawstmt = (RawStmt *) node;
			pstate = make_parsestate(NULL);

			Assert(IsA(node, RawStmt));

			if (!IsA(rawstmt->stmt, SelectStmt))
			{
				edata->elevel = WARNING;
				edata->sqlerrcode = ERRCODE_FEATURE_NOT_SUPPORTED;
				edata->message = "only select statements are supported";
			}
			else
			{
				pstate->p_sourcetext = sql;
				query = transformTopLevelStmt(pstate, rawstmt);
				free_parsestate(pstate);

				(void) cagg_validate_query(query, true, "public", "cagg_validate", false);
				is_valid_query = true;
			}
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();
	}
	PG_END_TRY();

	PG_RETURN_DATUM(create_cagg_validate_query_datum(tupdesc, is_valid_query, edata));
}

typedef struct TimeBucketInfoContext
{
	/* The updated cagg definition */
	const ContinuousAgg *cagg;

	/* The Oid of the old bucket function that should be replaced */
	Oid function_to_replace;

	/* Was the defined origin added during the migration and needs
	 * to be added to the function parameters during rewrite? */
	bool origin_added_during_migration;

	/* Do we need to flip the timezone and the origin parameter during migration? */
	bool need_parameter_order_change;
} TimeBucketInfoContext;

/* Get the Oid of the direct view of the CAgg. We cannot use the TimescaleDB internal
 * functions such as ts_continuous_agg_find_by_mat_hypertable_id() at this point since this
 * function can be called during an extension upgrade and ts_catalog_get() does not work.
 */
static Oid
get_direct_view_oid(int32 mat_hypertable_id)
{
	RangeVar *ts_cagg = makeRangeVar(CATALOG_SCHEMA_NAME,
									 CONTINUOUS_AGG_TABLE_NAME,
									 -1 /* taken location unknown */);
	Relation cagg_rel = relation_openrv_extended(ts_cagg, AccessShareLock, /* missing ok */ true);

	RangeVar *ts_cagg_idx =
		makeRangeVar(CATALOG_SCHEMA_NAME, TS_CAGG_CATALOG_IDX, -1 /* taken location unknown */);
	Relation cagg_idx_rel =
		relation_openrv_extended(ts_cagg_idx, AccessShareLock, /* missing ok */ true);

	/* Prepare relation scan */
	TupleTableSlot *slot = table_slot_create(cagg_rel, NULL);
	ScanKeyData scankeys[1];
	ScanKeyEntryInitialize(&scankeys[0],
						   0,
						   1,
						   BTEqualStrategyNumber,
						   InvalidOid,
						   InvalidOid,
						   F_INT4EQ,
						   Int32GetDatum(mat_hypertable_id));

	/* Prepare index scan */
	PushActiveSnapshot(GetTransactionSnapshot());
	IndexScanDesc indexscan =
		index_beginscan_compat(cagg_rel, cagg_idx_rel, GetActiveSnapshot(), NULL, 1, 0);
	index_rescan(indexscan, scankeys, 1, NULL, 0);

	/* Read tuple from relation */
	bool got_next_slot = index_getnext_slot(indexscan, ForwardScanDirection, slot);
	if (!got_next_slot)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid materialized hypertable ID: %d", mat_hypertable_id)));
	bool is_null = false;

	/* We use the view_schema and view_name to get data from the system catalog. Even char pointers
	 * are passed to the catalog, it calls namehashfast() internally, which assumes that a char of
	 * NAMEDATALEN is allocated. */
	NameData view_schema_name;
	NameData view_name_name;

	/* We need to get the attribute names dynamically since this function can be called during an
	 * upgrade and the fixed attribution numbers (i.e., Anum_continuous_agg_direct_view_schema) can
	 * be different. */
	AttrNumber direct_view_schema_attr = get_attnum(cagg_rel->rd_id, "direct_view_schema");
	Ensure(direct_view_schema_attr != InvalidAttrNumber,
		   "unable to get attribute number for direct_view_schema");

	AttrNumber direct_view_name_attr = get_attnum(cagg_rel->rd_id, "direct_view_name");
	Ensure(direct_view_name_attr != InvalidAttrNumber,
		   "unable to get attribute number for direct_view_name");

	char *view_schema = DatumGetCString(slot_getattr(slot, direct_view_schema_attr, &is_null));
	Ensure(!is_null, "unable to get view schema for oid %d", mat_hypertable_id);
	Assert(view_schema != NULL);
	namestrcpy(&view_schema_name, view_schema);

	char *view_name = DatumGetCString(slot_getattr(slot, direct_view_name_attr, &is_null));
	Ensure(!is_null, "unable to get view name for oid %d", mat_hypertable_id);
	Assert(view_name != NULL);
	namestrcpy(&view_name_name, view_name);

	got_next_slot = index_getnext_slot(indexscan, ForwardScanDirection, slot);
	Ensure(!got_next_slot, "found duplicate definitions for CAgg mat_ht %d", mat_hypertable_id);

	/* End relation scan */
	index_endscan(indexscan);
	ExecDropSingleTupleTableSlot(slot);
	relation_close(cagg_rel, AccessShareLock);
	relation_close(cagg_idx_rel, AccessShareLock);
	PopActiveSnapshot();

	/* Get Oid of user view */
	Oid direct_view_oid =
		ts_get_relation_relid(NameStr(view_schema_name), NameStr(view_name_name), false);
	Assert(OidIsValid(direct_view_oid));
	return direct_view_oid;
}

enum
{
	Anum_cagg_bucket_function_oid = 1,
	Anum_cagg_bucket_function_width,
	Anum_cagg_bucket_function_origin,
	Anum_cagg_bucket_function_offset,
	Anum_cagg_bucket_function_timezone,
	Anum_cagg_bucket_function_fixed_width,
	_Anum_cagg_bucket_function_max
};

#define Natts_cagg_bucket_function (_Anum_cagg_bucket_function_max - 1)

static Datum
create_cagg_get_bucket_function_datum(TupleDesc tupdesc, ContinuousAggBucketFunction *bf)
{
	NullableDatum datums[Natts_cagg_bucket_function] = { { 0 } };
	HeapTuple tuple;

	char *bucket_origin = NULL;
	char *bucket_offset = NULL;
	char *bucket_width = NULL;

	if (IS_TIME_BUCKET_INFO_TIME_BASED(bf))
	{
		/* Bucketing on time */
		Assert(bf->bucket_time_width != NULL);
		bucket_width = DatumGetCString(
			DirectFunctionCall1(interval_out, IntervalPGetDatum(bf->bucket_time_width)));

		if (!TIMESTAMP_NOT_FINITE(bf->bucket_time_origin))
		{
			bucket_origin = DatumGetCString(
				DirectFunctionCall1(timestamptz_out, TimestampTzGetDatum(bf->bucket_time_origin)));
		}

		if (bf->bucket_time_offset != NULL)
		{
			bucket_offset = DatumGetCString(
				DirectFunctionCall1(interval_out, IntervalPGetDatum(bf->bucket_time_offset)));
		}
	}
	else
	{
		/* Bucketing on integers */
		bucket_width = palloc0((MAXINT8LEN + 1) * sizeof(char));
		pg_lltoa(bf->bucket_integer_width, bucket_width);

		/* Integer buckets with origin are not supported, so nothing to do. */
		Assert(bucket_origin == NULL);

		if (bf->bucket_integer_offset != 0)
		{
			bucket_offset = palloc0((MAXINT8LEN + 1) * sizeof(char));
			pg_lltoa(bf->bucket_integer_offset, bucket_offset);
		}
	}

	tupdesc = BlessTupleDesc(tupdesc);

	ts_datum_set_objectid(Anum_cagg_bucket_function_oid, datums, bf->bucket_function);
	ts_datum_set_text_from_cstring(Anum_cagg_bucket_function_width, datums, bucket_width);
	ts_datum_set_text_from_cstring(Anum_cagg_bucket_function_origin, datums, bucket_origin);
	ts_datum_set_text_from_cstring(Anum_cagg_bucket_function_offset, datums, bucket_offset);
	ts_datum_set_text_from_cstring(Anum_cagg_bucket_function_timezone,
								   datums,
								   bf->bucket_time_timezone);
	ts_datum_set_bool(Anum_cagg_bucket_function_fixed_width,
					  datums,
					  bf->bucket_fixed_interval,
					  false);

	Assert(tupdesc->natts == Natts_cagg_validate_query);
	tuple = ts_heap_form_tuple(tupdesc, datums);

	return HeapTupleGetDatum(tuple);
}

/*
 * Get the bucket function information for the given materialized hypertable id.
 *
 * When running `cagg_get_bucket_function_info` the function returns the following fields:
 * - oid: The Oid of the bucket function
 * - width: The width of the bucket function
 * - origin: The origin of the bucket function
 * - offset: The offset of the bucket function
 * - timezone: The timezone of the bucket function
 * - fixed_width: Is the bucket width fixed
 *
 * When running `cagg_get_bucket_function` the function returns the following fields:
 * - oid: The Oid of the bucket function
 */
static Datum
cagg_get_bucket_function_datum(int32 mat_hypertable_id, FunctionCallInfo fcinfo)
{
	Oid direct_view_oid = get_direct_view_oid(mat_hypertable_id);
	TupleDesc tupdesc;

	if (fcinfo != NULL && get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "function returning record called in context that cannot accept type record");

	ContinuousAggBucketFunction *bf = ts_cagg_get_bucket_function_info(direct_view_oid);

	if (!OidIsValid(bf->bucket_function))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("time_bucket function not found in CAgg definition for "
						"mat_ht_id: %d",
						mat_hypertable_id)));
		pg_unreachable();
	}

	if (fcinfo != NULL)
		return create_cagg_get_bucket_function_datum(tupdesc, bf);

	return ObjectIdGetDatum(bf->bucket_function);
}

/*
 * This function returns the `time_bucket` function Oid in the user view definition
 * of a given materialization hupertable.
 *
 * NOTE: this function is deprecated and should be removed in the future, use
 * `cagg_get_bucket_function_info` instead.
 */
Datum
continuous_agg_get_bucket_function(PG_FUNCTION_ARGS)
{
	/* Return the oid of the bucket function */
	PG_RETURN_DATUM(cagg_get_bucket_function_datum(PG_GETARG_INT32(0), NULL));
}

/*
 * This function returns all information about the `time_bucket` function of a given
 * materialization hypertable using the user view definition stored in Postgres catalog.
 */
Datum
continuous_agg_get_bucket_function_info(PG_FUNCTION_ARGS)
{
	/* Return all bucket function info */
	PG_RETURN_DATUM(cagg_get_bucket_function_datum(PG_GETARG_INT32(0), fcinfo));
}
