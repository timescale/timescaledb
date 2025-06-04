/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include "invalidation_funcs.h"

#include <access/tupdesc.h>
#include <funcapi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <replication/logicalproto.h>

#include "compat/compat.h"
#include "cache.h"
#include "hypertable.h"
#include "ts_catalog/catalog.h"

TS_FUNCTION_INFO_V1(ts_invalidation_read_record);

Datum
ts_invalidation_tuple_get_value(LogicalRepTupleData *tupleData, TupleDesc tupdesc, int attnum,
								bool *isnull)
{
	Form_pg_attribute att;
	Datum result;

	Assert(attnum > 0);

	att = TupleDescAttr(tupdesc, AttrNumberGetAttrOffset(attnum));

	if (att->attisdropped)
	{
		*isnull = true;
		result = (Datum) 0;
	}
	else
	{
		Assert(AttrNumberGetAttrOffset(attnum) < tupleData->ncols);
		Assert(tupleData->colstatus[AttrNumberGetAttrOffset(attnum)] == LOGICALREP_COLUMN_BINARY);

		StringInfo colvalue = &tupleData->colvalues[AttrNumberGetAttrOffset(attnum)];

		Oid typreceive, typioparam;
		TS_DEBUG_LOG("reading type %s from attnum %d", format_type_be(att->atttypid), att->attnum);
		getTypeBinaryInputInfo(att->atttypid, &typreceive, &typioparam);
		Assert(OidIsValid(typreceive));
		result = OidReceiveFunctionCall(typreceive, colvalue, typioparam, att->atttypmod);

		if (colvalue->cursor != colvalue->len)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					 errmsg("incorrect data format in invalidation record column %d", attnum)));

		*isnull = false;
	}
	return result;
}

/* Look up and replace the hypertable relid with the hypertable id */
static void
replace_hypertable_relid_with_hypertable_id(Datum *values)
{
	Cache *hcache;
	Oid hypertable_relid = DatumGetObjectId(values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_hypertable_invalidation_log_hypertable_id)]);
	Hypertable *ht =
		ts_hypertable_cache_get_cache_and_entry(hypertable_relid, CACHE_FLAG_NONE, &hcache);
	int32 hypertable_id = ht->fd.id;
	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_hypertable_invalidation_log_hypertable_id)] =
		Int32GetDatum(hypertable_id);
	ts_cache_release(&hcache);
}

/*
 * Create a heap tuple from the invalidation record.
 */
static HeapTuple
invalidation_tuple_get_heap_tuple(LogicalRepTupleData *tupleData, TupleDesc tupdesc)
{
	Datum values[_Anum_continuous_aggs_hypertable_invalidation_log_max];
	bool nulls[_Anum_continuous_aggs_hypertable_invalidation_log_max] = { 0 };

	for (int attnum = 1; attnum < _Anum_continuous_aggs_hypertable_invalidation_log_max; ++attnum)
		values[AttrNumberGetAttrOffset(attnum)] =
			ts_invalidation_tuple_get_value(tupleData,
											tupdesc,
											attnum,
											&nulls[AttrNumberGetAttrOffset(attnum)]);

	/* We store the relid in the invalidation record coming from the
	 * invalidation plugin so replace it with the hypertable id */
	replace_hypertable_relid_with_hypertable_id(values);

	return heap_form_tuple(tupdesc, values, nulls);
}

/*
 * Decode invalidation tuple in bytea format.
 */
LogicalRepRelId
ts_invalidation_tuple_decode(LogicalRepTupleData *tuple, bytea *record)
{
	StringInfoData info;
	initReadOnlyStringInfo(&info, VARDATA_ANY(record), VARSIZE_ANY_EXHDR(record));

	LogicalRepMsgType action = pq_getmsgbyte(&info);
	TS_DEBUG_LOG("action is %c", action);
	Ensure(action == LOGICAL_REP_MSG_INSERT, "expected an insert but got byte %02x", action);

	return logicalrep_read_insert(&info, tuple);
}

/*
 * Read an encoded invalidation record coming from the plugin.
 *
 * Records from the plugin is coming in logical replication format, so decode
 * it and produce a single composite value in the same format as the
 * invalidation table.
 */
Datum
ts_invalidation_read_record(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	bytea *raw_record = PG_GETARG_BYTEA_P(0);
	LogicalRepTupleData tupleData;

	ts_invalidation_tuple_decode(&tupleData, raw_record);

	HeapTuple htup = invalidation_tuple_get_heap_tuple(&tupleData, tupdesc);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
