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

static void
invalidation_record_set_value(LogicalRepTupleData *tupleData, TupleDesc tupdesc, int attnum,
							  Datum *values, bool *isnull)
{
	Form_pg_attribute att = TupleDescAttr(tupdesc, attnum - 1);

	Assert(attnum > 0);

	if (att->attisdropped)
	{
		values[attnum - 1] = (Datum) 0;
		isnull[attnum - 1] = true;
	}
	else
	{
		Assert(attnum - 1 < tupleData->ncols);
		Assert(tupleData->colstatus[attnum - 1] == LOGICALREP_COLUMN_BINARY);

		StringInfo colvalue = &tupleData->colvalues[attnum - 1];

		Oid typreceive, typioparam;
		getTypeBinaryInputInfo(att->atttypid, &typreceive, &typioparam);
		Assert(OidIsValid(typreceive));
		values[attnum - 1] =
			OidReceiveFunctionCall(typreceive, colvalue, typioparam, att->atttypmod);
		isnull[attnum - 1] = false;

		if (colvalue->cursor != colvalue->len)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION),
					 errmsg("incorrect data format in invalidation record column %d", attnum)));
	}
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
invalidation_record_get_heap_tuple(TupleDesc tupdesc, LogicalRepTupleData *tupleData)
{
	Datum values[_Anum_continuous_aggs_hypertable_invalidation_log_max];
	bool nulls[_Anum_continuous_aggs_hypertable_invalidation_log_max] = { 0 };

	for (int attnum = 1; attnum < _Anum_continuous_aggs_hypertable_invalidation_log_max; ++attnum)
		invalidation_record_set_value(tupleData, tupdesc, attnum, values, nulls);

	/* We store the relid in the invalidation record coming from the
	 * invalidation plugin so replace it with the hypertable id */
	replace_hypertable_relid_with_hypertable_id(values);

	return heap_form_tuple(tupdesc, values, nulls);
}

/*
 * Read an encoded invalidation record coming from the plugin.
 *
 * Records from the plugin is coming in logical replication format, so decode
 * it and produce a single composite value in the same format as the
 * invalidation table.
 */
TSDLLEXPORT Datum
ts_invalidation_read_record(PG_FUNCTION_ARGS)
{
	TupleDesc tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	StringInfoData info;
	bytea *raw_record = PG_GETARG_BYTEA_P(0);
	initReadOnlyStringInfo(&info, VARDATA_ANY(raw_record), VARSIZE_ANY_EXHDR(raw_record));

	LogicalRepMsgType action = pq_getmsgbyte(&info);
	Ensure(action == LOGICAL_REP_MSG_INSERT, "expected an insert but got byte %02x", action);

	LogicalRepTupleData tupleData;
	logicalrep_read_insert(&info, &tupleData);

	HeapTuple htup = invalidation_record_get_heap_tuple(tupdesc, &tupleData);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}
