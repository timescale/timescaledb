/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file handles commands on continuous aggs that should be allowed in
 * apache only mode. Right now this consists mostly of drop commands
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <catalog/dependency.h>
#include <catalog/namespace.h>
#include <catalog/pg_trigger.h>
#include <commands/trigger.h>
#include <fmgr.h>
#include <nodes/makefuncs.h>
#include <storage/lmgr.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/date.h>
#include <utils/lsyscache.h>
#include <utils/timestamp.h>

#include "compat/compat.h"

#include "bgw/job.h"
#include "compression_with_clause.h"
#include "cross_module_fn.h"
#include "errors.h"
#include "func_cache.h"
#include "hypercube.h"
#include "hypertable_cache.h"
#include "hypertable.h"
#include "scan_iterator.h"
#include "time_bucket.h"
#include "time_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/continuous_aggs_watermark.h"
#include "utils.h"

#define BUCKET_FUNCTION_SERIALIZE_VERSION 1
#define CHECK_NAME_MATCH(name1, name2) (namestrcmp(name1, name2) == 0)

static const WithClauseDefinition continuous_aggregate_with_clause_def[] = {
		[ContinuousEnabled] = {
			.arg_name = "continuous",
			.type_id = BOOLOID,
			.default_val = (Datum)false,
		},
		[ContinuousViewOptionCreateGroupIndex] = {
			.arg_name = "create_group_indexes",
			.type_id = BOOLOID,
			.default_val = (Datum)true,
		},
		[ContinuousViewOptionMaterializedOnly] = {
			.arg_name = "materialized_only",
			.type_id = BOOLOID,
			.default_val = (Datum)true,
		},
		[ContinuousViewOptionCompress] = {
			.arg_name = "compress",
			.type_id = BOOLOID,
		},
		[ContinuousViewOptionFinalized] = {
			.arg_name = "finalized",
			.type_id = BOOLOID,
			.default_val = (Datum)true,
		},
		[ContinuousViewOptionCompressSegmentBy] = {
			 .arg_name = "compress_segmentby",
			 .type_id = TEXTOID,
		},
		[ContinuousViewOptionCompressOrderBy] = {
			 .arg_name = "compress_orderby",
			 .type_id = TEXTOID,
		},
		[ContinuousViewOptionCompressChunkTimeInterval] = {
			 .arg_name = "compress_chunk_time_interval",
			 .type_id = INTERVALOID,
		},
};

WithClauseResult *
ts_continuous_agg_with_clause_parse(const List *defelems)
{
	return ts_with_clauses_parse(defelems,
								 continuous_aggregate_with_clause_def,
								 TS_ARRAY_LEN(continuous_aggregate_with_clause_def));
}

List *
ts_continuous_agg_get_compression_defelems(const WithClauseResult *with_clauses)
{
	List *ret = NIL;

	for (int i = 0; i < CompressOptionMax; i++)
	{
		int option_index = 0;
		switch (i)
		{
			case CompressEnabled:
				option_index = ContinuousViewOptionCompress;
				break;
			case CompressSegmentBy:
				option_index = ContinuousViewOptionCompressSegmentBy;
				break;
			case CompressOrderBy:
				option_index = ContinuousViewOptionCompressOrderBy;
				break;
			case CompressChunkTimeInterval:
				option_index = ContinuousViewOptionCompressChunkTimeInterval;
				break;
			default:
				elog(ERROR, "Unhandled compression option");
				break;
		}

		const WithClauseResult *input = &with_clauses[option_index];
		WithClauseDefinition def = continuous_aggregate_with_clause_def[option_index];

		if (!input->is_default)
		{
			Node *value = (Node *) makeString(ts_with_clause_result_deparse_value(input));
			DefElem *elem = makeDefElemExtended(EXTENSION_NAMESPACE,
												(char *) def.arg_name,
												value,
												DEFELEM_UNSPEC,
												-1);
			ret = lappend(ret, elem);
		}
	}
	return ret;
}

static void
init_scan_by_mat_hypertable_id(ScanIterator *iterator, const int32 mat_hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_PKEY);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_agg_pkey_mat_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(mat_hypertable_id));
}

static void
init_scan_cagg_bucket_function_by_mat_hypertable_id(ScanIterator *iterator,
													const int32 mat_hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_BUCKET_FUNCTION,
											CONTINUOUS_AGGS_BUCKET_FUNCTION_PKEY_IDX);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_aggs_bucket_function_pkey_mat_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(mat_hypertable_id));
}

static void
init_scan_by_raw_hypertable_id(ScanIterator *iterator, const int32 raw_hypertable_id)
{
	iterator->ctx.index =
		catalog_get_index(ts_catalog_get(), CONTINUOUS_AGG, CONTINUOUS_AGG_RAW_HYPERTABLE_ID_IDX);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_agg_raw_hypertable_id_idx_raw_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(raw_hypertable_id));
}

static void
init_invalidation_threshold_scan_by_hypertable_id(ScanIterator *iterator,
												  const int32 raw_hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_INVALIDATION_THRESHOLD,
											CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_PKEY);

	ts_scan_iterator_scan_key_init(iterator,
								   Anum_continuous_aggs_invalidation_threshold_pkey_hypertable_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(raw_hypertable_id));
}

static void
init_hypertable_invalidation_log_scan_by_hypertable_id(ScanIterator *iterator,
													   const int32 raw_hypertable_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG,
											CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG_IDX);

	ts_scan_iterator_scan_key_init(
		iterator,
		Anum_continuous_aggs_hypertable_invalidation_log_idx_hypertable_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(raw_hypertable_id));
}

static void
init_materialization_invalidation_log_scan_by_materialization_id(ScanIterator *iterator,
																 const int32 materialization_id)
{
	iterator->ctx.index = catalog_get_index(ts_catalog_get(),
											CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
											CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG_IDX);

	ts_scan_iterator_scan_key_init(
		iterator,
		Anum_continuous_aggs_materialization_invalidation_log_idx_materialization_id,
		BTEqualStrategyNumber,
		F_INT4EQ,
		Int32GetDatum(materialization_id));
}

static int32
number_of_continuous_aggs_attached(int32 raw_hypertable_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);
	int32 count = 0;

	init_scan_by_raw_hypertable_id(&iterator, raw_hypertable_id);
	ts_scanner_foreach(&iterator) { count++; }
	return count;
}

static void
invalidation_threshold_delete(int32 raw_hypertable_id)
{
	ScanIterator iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_INVALIDATION_THRESHOLD,
													RowExclusiveLock,
													CurrentMemoryContext);

	init_invalidation_threshold_scan_by_hypertable_id(&iterator, raw_hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
}

static void
cagg_bucket_function_delete(int32 mat_hypertable_id)
{
	ScanIterator iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_BUCKET_FUNCTION,
													RowExclusiveLock,
													CurrentMemoryContext);

	init_scan_cagg_bucket_function_by_mat_hypertable_id(&iterator, mat_hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
}

static void
hypertable_invalidation_log_delete(int32 raw_hypertable_id)
{
	ScanIterator iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG,
													RowExclusiveLock,
													CurrentMemoryContext);

	init_hypertable_invalidation_log_scan_by_hypertable_id(&iterator, raw_hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
}

void
ts_materialization_invalidation_log_delete_inner(int32 mat_hypertable_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGGS_MATERIALIZATION_INVALIDATION_LOG,
								RowExclusiveLock,
								CurrentMemoryContext);

	elog(DEBUG1, "materialization log delete for hypertable %d", mat_hypertable_id);
	init_materialization_invalidation_log_scan_by_materialization_id(&iterator, mat_hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
	}
}

static HeapTuple
continuous_agg_formdata_make_tuple(const FormData_continuous_agg *fd, TupleDesc desc)
{
	Datum values[Natts_continuous_agg];
	bool nulls[Natts_continuous_agg] = { false };

	memset(values, 0, sizeof(Datum) * Natts_continuous_agg);

	values[AttrNumberGetAttrOffset(Anum_continuous_agg_mat_hypertable_id)] =
		Int32GetDatum(fd->mat_hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_raw_hypertable_id)] =
		Int32GetDatum(fd->raw_hypertable_id);

	if (fd->parent_mat_hypertable_id == INVALID_HYPERTABLE_ID)
		nulls[AttrNumberGetAttrOffset(Anum_continuous_agg_parent_mat_hypertable_id)] = true;
	else
	{
		values[AttrNumberGetAttrOffset(Anum_continuous_agg_parent_mat_hypertable_id)] =
			Int32GetDatum(fd->parent_mat_hypertable_id);
	}

	values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_schema)] =
		NameGetDatum(&fd->user_view_schema);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_name)] =
		NameGetDatum(&fd->user_view_name);

	values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_schema)] =
		NameGetDatum(&fd->partial_view_schema);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_name)] =
		NameGetDatum(&fd->partial_view_name);

	values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_schema)] =
		NameGetDatum(&fd->direct_view_schema);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_name)] =
		NameGetDatum(&fd->direct_view_name);

	values[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)] =
		BoolGetDatum(fd->materialized_only);
	values[AttrNumberGetAttrOffset(Anum_continuous_agg_finalized)] = BoolGetDatum(fd->finalized);

	return heap_form_tuple(desc, values, nulls);
}

static void
continuous_agg_formdata_fill(FormData_continuous_agg *fd, const TupleInfo *ti)
{
	bool should_free;
	HeapTuple tuple;
	Datum values[Natts_continuous_agg];
	bool nulls[Natts_continuous_agg] = { false };

	tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
	heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, nulls);

	fd->mat_hypertable_id =
		DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_continuous_agg_mat_hypertable_id)]);
	fd->raw_hypertable_id =
		DatumGetInt32(values[AttrNumberGetAttrOffset(Anum_continuous_agg_raw_hypertable_id)]);

	if (nulls[AttrNumberGetAttrOffset(Anum_continuous_agg_parent_mat_hypertable_id)])
		fd->parent_mat_hypertable_id = INVALID_HYPERTABLE_ID;
	else
		fd->parent_mat_hypertable_id = DatumGetInt32(
			values[AttrNumberGetAttrOffset(Anum_continuous_agg_parent_mat_hypertable_id)]);

	namestrcpy(&fd->user_view_schema,
			   DatumGetCString(
				   values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_schema)]));
	namestrcpy(&fd->user_view_name,
			   DatumGetCString(
				   values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_name)]));

	namestrcpy(&fd->partial_view_schema,
			   DatumGetCString(
				   values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_schema)]));
	namestrcpy(&fd->partial_view_name,
			   DatumGetCString(
				   values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_name)]));

	namestrcpy(&fd->direct_view_schema,
			   DatumGetCString(
				   values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_schema)]));
	namestrcpy(&fd->direct_view_name,
			   DatumGetCString(
				   values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_name)]));

	fd->materialized_only =
		DatumGetBool(values[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)]);
	fd->finalized = DatumGetBool(values[AttrNumberGetAttrOffset(Anum_continuous_agg_finalized)]);

	if (should_free)
		heap_freetuple(tuple);
}

/*
 * Fill the fields of a integer based bucketing function
 */
static void
cagg_fill_bucket_function_integer_based(ContinuousAggsBucketFunction *bf, bool *isnull,
										Datum *values)
{
	/* Bucket width */
	Assert(!isnull[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_width)]);
	const char *bucket_width_str = TextDatumGetCString(
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_width)]);
	Assert(strlen(bucket_width_str) > 0);
	bf->bucket_integer_width = pg_strtoint64(bucket_width_str);

	/* Bucket origin cannot be used with integer based buckets */
	Assert(isnull[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_origin)] ==
		   true);

	/* Bucket offset */
	if (!isnull[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_offset)])
	{
		const char *offset_str = TextDatumGetCString(
			values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_offset)]);
		bf->bucket_integer_offset = pg_strtoint64(offset_str);
	}

	/* Timezones cannot be used with integer based buckets */
	Assert(isnull[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_timezone)] ==
		   true);
}

/*
 * Fill the fields of a time based bucketing function
 */
static void
cagg_fill_bucket_function_time_based(ContinuousAggsBucketFunction *bf, bool *isnull, Datum *values)
{
	/*
	 * bucket_width
	 *
	 * The value is stored as TEXT since we have to store the interval value of time
	 * buckets and also the number value of integer based buckets.
	 */
	Assert(!isnull[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_width)]);
	const char *bucket_width_str = TextDatumGetCString(
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_width)]);
	Assert(strlen(bucket_width_str) > 0);
	bf->bucket_time_width = DatumGetIntervalP(
		DirectFunctionCall3(interval_in, CStringGetDatum(bucket_width_str), InvalidOid, -1));

	/* Bucket origin */
	if (!isnull[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_origin)])
	{
		const char *origin_str = TextDatumGetCString(
			values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_origin)]);
		bf->bucket_time_origin = DatumGetTimestamp(DirectFunctionCall3(timestamptz_in,
																	   CStringGetDatum(origin_str),
																	   ObjectIdGetDatum(InvalidOid),
																	   Int32GetDatum(-1)));
	}
	else
	{
		TIMESTAMP_NOBEGIN(bf->bucket_time_origin);
	}

	/* Bucket offset */
	if (!isnull[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_offset)])
	{
		const char *offset_str = TextDatumGetCString(
			values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_offset)]);
		bf->bucket_time_offset = DatumGetIntervalP(
			DirectFunctionCall3(interval_in, CStringGetDatum(offset_str), InvalidOid, -1));
	}

	/* Bucket timezone */
	if (!isnull[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_timezone)])
	{
		bf->bucket_time_timezone = TextDatumGetCString(
			values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_bucket_timezone)]);
	}
}

static void
continuous_agg_fill_bucket_function(int32 mat_hypertable_id, ContinuousAggsBucketFunction *bf)
{
	ScanIterator iterator;
	int count = 0;

	iterator = ts_scan_iterator_create(CONTINUOUS_AGGS_BUCKET_FUNCTION,
									   AccessShareLock,
									   CurrentMemoryContext);
	init_scan_cagg_bucket_function_by_mat_hypertable_id(&iterator, mat_hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		Datum values[Natts_continuous_aggs_bucket_function];
		bool isnull[Natts_continuous_aggs_bucket_function];
		bool should_free;

		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);

		/*
		 * Our usual GETSTRUCT() approach doesn't work when TEXT fields are involved,
		 * thus a more robust approach with heap_deform_tuple() is used here.
		 */
		heap_deform_tuple(tuple, ts_scan_iterator_tupledesc(&iterator), values, isnull);

		/* Bucket function */
		Assert(!isnull[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_function)]);
		bf->bucket_function = DatumGetObjectId(
			values[AttrNumberGetAttrOffset(Anum_continuous_aggs_bucket_function_function)]);

		Assert(OidIsValid(bf->bucket_function));

		bf->bucket_time_based = ts_continuous_agg_bucket_on_interval(bf->bucket_function);

		if (bf->bucket_time_based)
		{
			cagg_fill_bucket_function_time_based(bf, isnull, values);
		}
		else
		{
			cagg_fill_bucket_function_integer_based(bf, isnull, values);
		}

		/* Bucket fixed width */
		Assert(!isnull[AttrNumberGetAttrOffset(
			Anum_continuous_aggs_bucket_function_bucket_fixed_width)]);
		bf->bucket_fixed_interval = DatumGetBool(values[AttrNumberGetAttrOffset(
			Anum_continuous_aggs_bucket_function_bucket_fixed_width)]);

		count++;

		if (should_free)
			heap_freetuple(tuple);
	}

	/*
	 * This function should never be called unless we know that the corresponding
	 * cagg exists and uses a variable-sized bucket. There should be exactly one
	 * entry in .continuous_aggs_bucket_function catalog table for such a cagg.
	 */
	if (count != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("invalid or missing information about the bucketing function for cagg"),
				 errdetail("%d", mat_hypertable_id)));
	}
}

static void
continuous_agg_init(ContinuousAgg *cagg, const Form_continuous_agg fd)
{
	Oid nspid = get_namespace_oid(NameStr(fd->user_view_schema), false);
	Hypertable *cagg_ht = ts_hypertable_get_by_id(fd->mat_hypertable_id);
	const Dimension *time_dim;

	Assert(NULL != cagg_ht);
	time_dim = hyperspace_get_open_dimension(cagg_ht->space, 0);
	Assert(NULL != time_dim);
	cagg->partition_type = ts_dimension_get_partition_type(time_dim);
	cagg->relid = get_relname_relid(NameStr(fd->user_view_name), nspid);
	memcpy(&cagg->data, fd, sizeof(cagg->data));

	Assert(OidIsValid(cagg->relid));
	Assert(OidIsValid(cagg->partition_type));

	cagg->bucket_function = palloc0(sizeof(ContinuousAggsBucketFunction));
	continuous_agg_fill_bucket_function(cagg->data.mat_hypertable_id, cagg->bucket_function);
}

TSDLLEXPORT CaggsInfo
ts_continuous_agg_get_all_caggs_info(int32 raw_hypertable_id)
{
	CaggsInfo all_caggs_info;

	List *caggs = ts_continuous_aggs_find_by_raw_table_id(raw_hypertable_id);
	ListCell *lc;

	all_caggs_info.mat_hypertable_ids = NIL;
	all_caggs_info.bucket_functions = NIL;

	Assert(list_length(caggs) > 0);

	foreach (lc, caggs)
	{
		ContinuousAgg *cagg = lfirst(lc);

		all_caggs_info.bucket_functions =
			lappend(all_caggs_info.bucket_functions, cagg->bucket_function);

		all_caggs_info.mat_hypertable_ids =
			lappend_int(all_caggs_info.mat_hypertable_ids, cagg->data.mat_hypertable_id);
	}
	return all_caggs_info;
}

TSDLLEXPORT ContinuousAggHypertableStatus
ts_continuous_agg_hypertable_status(int32 hypertable_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);
	ContinuousAggHypertableStatus status = HypertableIsNotContinuousAgg;

	ts_scanner_foreach(&iterator)
	{
		FormData_continuous_agg data;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);

		continuous_agg_formdata_fill(&data, ti);

		if (data.raw_hypertable_id == hypertable_id)
			status |= HypertableIsRawTable;
		if (data.mat_hypertable_id == hypertable_id)
			status |= HypertableIsMaterialization;

		if (status == HypertableIsMaterializationAndRaw)
		{
			ts_scan_iterator_close(&iterator);
			return status;
		}
	}

	return status;
}

TSDLLEXPORT bool
ts_continuous_agg_hypertable_all_finalized(int32 raw_hypertable_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);
	bool all_finalized = true;

	init_scan_by_raw_hypertable_id(&iterator, raw_hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		FormData_continuous_agg data;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);

		continuous_agg_formdata_fill(&data, ti);

		if (!data.finalized)
		{
			all_finalized = false;
			break;
		}
	}

	ts_scan_iterator_close(&iterator);

	return all_finalized;
}

TSDLLEXPORT List *
ts_continuous_aggs_find_by_raw_table_id(int32 raw_hypertable_id)
{
	List *continuous_aggs = NIL;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);

	init_scan_by_raw_hypertable_id(&iterator, raw_hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		ContinuousAgg *ca;
		FormData_continuous_agg data;
		MemoryContext oldmctx;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);

		continuous_agg_formdata_fill(&data, ti);

		oldmctx = MemoryContextSwitchTo(ts_scan_iterator_get_result_memory_context(&iterator));
		ca = palloc0(sizeof(*ca));
		continuous_agg_init(ca, &data);
		continuous_aggs = lappend(continuous_aggs, ca);
		MemoryContextSwitchTo(oldmctx);
	}

	return continuous_aggs;
}

/* Find a continuous aggregate by the materialized hypertable id */
ContinuousAgg *
ts_continuous_agg_find_by_mat_hypertable_id(int32 mat_hypertable_id, bool missing_ok)
{
	ContinuousAgg *ca = NULL;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);

	init_scan_by_mat_hypertable_id(&iterator, mat_hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		FormData_continuous_agg form;

		continuous_agg_formdata_fill(&form, ti);

		/* Note that this scan can only match at most once, so we assert on
		 * `ca` here. */
		Assert(ca == NULL);
		ca = ts_scan_iterator_alloc_result(&iterator, sizeof(*ca));
		continuous_agg_init(ca, &form);

		Assert(ca && ca->data.mat_hypertable_id == mat_hypertable_id);
	}
	ts_scan_iterator_close(&iterator);

	if (ca == NULL && !missing_ok)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid materialized hypertable ID: %d", mat_hypertable_id)));
	}

	return ca;
}

static bool
continuous_agg_find_by_name(const char *schema, const char *name, ContinuousAggViewType type,
							FormData_continuous_agg *fd)
{
	ScanIterator iterator;
	AttrNumber view_name_attrnum = 0;
	AttrNumber schema_name_attrnum = 0;
	int count = 0;

	Assert(schema);
	Assert(name);

	switch (type)
	{
		case ContinuousAggUserView:
			schema_name_attrnum = Anum_continuous_agg_user_view_schema;
			view_name_attrnum = Anum_continuous_agg_user_view_name;
			break;
		case ContinuousAggPartialView:
			schema_name_attrnum = Anum_continuous_agg_partial_view_schema;
			view_name_attrnum = Anum_continuous_agg_partial_view_name;
			break;
		case ContinuousAggDirectView:
			schema_name_attrnum = Anum_continuous_agg_direct_view_schema;
			view_name_attrnum = Anum_continuous_agg_direct_view_name;
			break;
		case ContinuousAggAnyView:
			break;
	}

	iterator = ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);

	if (type != ContinuousAggAnyView)
	{
		ts_scan_iterator_scan_key_init(&iterator,
									   schema_name_attrnum,
									   BTEqualStrategyNumber,
									   F_NAMEEQ,
									   CStringGetDatum(schema));
		ts_scan_iterator_scan_key_init(&iterator,
									   view_name_attrnum,
									   BTEqualStrategyNumber,
									   F_NAMEEQ,
									   CStringGetDatum(name));
	}

	ts_scanner_foreach(&iterator)
	{
		ContinuousAggViewType vtype = type;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		FormData_continuous_agg data;

		continuous_agg_formdata_fill(&data, ti);

		if (vtype == ContinuousAggAnyView)
			vtype = ts_continuous_agg_view_type(&data, schema, name);

		if (vtype != ContinuousAggAnyView)
		{
			memcpy(fd, &data, sizeof(*fd));
			count++;
		}
	}

	Assert(count <= 1);

	return count == 1;
}

ContinuousAgg *
ts_continuous_agg_find_by_view_name(const char *schema, const char *name,
									ContinuousAggViewType type)
{
	FormData_continuous_agg fd;
	ContinuousAgg *ca;

	if (!continuous_agg_find_by_name(schema, name, type, &fd))
		return NULL;

	ca = palloc0(sizeof(ContinuousAgg));
	continuous_agg_init(ca, &fd);

	return ca;
}

ContinuousAgg *
ts_continuous_agg_find_userview_name(const char *schema, const char *name)
{
	return ts_continuous_agg_find_by_view_name(schema, name, ContinuousAggUserView);
}

/*
 * Find a continuous agg object by the main relid.
 *
 * The relid is the user-facing object ID that represents the continuous
 * aggregate (i.e., the query view's ID).
 */
ContinuousAgg *
ts_continuous_agg_find_by_relid(Oid relid)
{
	const char *relname = get_rel_name(relid);
	const char *schemaname = get_namespace_name(get_rel_namespace(relid));

	if (NULL == relname || NULL == schemaname)
		return NULL;

	return ts_continuous_agg_find_userview_name(schemaname, relname);
}

/*
 * Find a continuous aggregate by range var.
 */
ContinuousAgg *
ts_continuous_agg_find_by_rv(const RangeVar *rv)
{
	Oid relid;
	if (rv == NULL)
		return NULL;
	relid = RangeVarGetRelid(rv, NoLock, true);
	if (!OidIsValid(relid))
		return NULL;
	return ts_continuous_agg_find_by_relid(relid);
}

static ObjectAddress
get_and_lock_rel_by_name(const Name schema, const Name name, LOCKMODE mode)
{
	ObjectAddress addr;
	Oid relid = InvalidOid;
	Oid nspid = get_namespace_oid(NameStr(*schema), true);
	if (OidIsValid(nspid))
	{
		relid = get_relname_relid(NameStr(*name), nspid);
		if (OidIsValid(relid))
			LockRelationOid(relid, mode);
	}
	ObjectAddressSet(addr, RelationRelationId, relid);
	return addr;
}

static ObjectAddress
get_and_lock_rel_by_hypertable_id(int32 hypertable_id, LOCKMODE mode)
{
	ObjectAddress addr;
	Oid relid = ts_hypertable_id_to_relid(hypertable_id, true);
	if (OidIsValid(relid))
		LockRelationOid(relid, mode);
	ObjectAddressSet(addr, RelationRelationId, relid);
	return addr;
}

/*
 * Drops continuous aggs and all related objects.
 *
 * This function is intended to be run by event trigger during CASCADE,
 * which implies that most of the dependent objects potentially could be
 * dropped including associated schema.
 *
 * These objects are:
 *
 * - user view itself
 * - continuous agg catalog entry
 * - partial view
 * - materialization hypertable
 * - trigger on the raw hypertable (hypertable specified in the user view)
 * - copy of the user view query (AKA the direct view)
 *
 * NOTE: The order in which the objects are dropped should be EXACTLY the
 * same as in materialize.c
 *
 * drop_user_view indicates whether to drop the user view.
 *                (should be false if called as part of the drop-user-view callback)
 */
static void
drop_continuous_agg(FormData_continuous_agg *cadata, bool drop_user_view)
{
	Catalog *catalog;
	ScanIterator iterator;
	ObjectAddress user_view = { 0 };
	ObjectAddress partial_view = { 0 };
	ObjectAddress direct_view = { 0 };
	ObjectAddress raw_hypertable_trig = { 0 };
	ObjectAddress raw_hypertable = { 0 };
	ObjectAddress mat_hypertable = { 0 };
	bool raw_hypertable_has_other_caggs;

	/* Delete the job before taking locks as it kills long-running jobs
	 * which we would otherwise wait on */
	List *jobs = ts_bgw_job_find_by_hypertable_id(cadata->mat_hypertable_id);
	ListCell *lc;

	foreach (lc, jobs)
	{
		BgwJob *job = lfirst(lc);
		ts_bgw_job_delete_by_id(job->fd.id);
	}

	/*
	 * Lock objects.
	 *
	 * Following objects might be already dropped in case of CASCADE
	 * drop including the associated schema object.
	 *
	 * NOTE: the lock order matters, see tsl/src/materialization.c.
	 * Perform all locking upfront.
	 *
	 * AccessExclusiveLock is needed to drop triggers and also prevent
	 * concurrent DML commands.
	 */
	if (drop_user_view)
		user_view = get_and_lock_rel_by_name(&cadata->user_view_schema,
											 &cadata->user_view_name,
											 AccessExclusiveLock);
	raw_hypertable =
		get_and_lock_rel_by_hypertable_id(cadata->raw_hypertable_id, AccessExclusiveLock);
	mat_hypertable =
		get_and_lock_rel_by_hypertable_id(cadata->mat_hypertable_id, AccessExclusiveLock);

	/* Lock catalogs */
	catalog = ts_catalog_get();
	LockRelationOid(catalog_get_table_id(catalog, BGW_JOB), RowExclusiveLock);
	LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGG), RowExclusiveLock);
	LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGGS_WATERMARK), RowExclusiveLock);

	raw_hypertable_has_other_caggs =
		OidIsValid(raw_hypertable.objectId) &&
		number_of_continuous_aggs_attached(cadata->raw_hypertable_id) > 1;

	if (!raw_hypertable_has_other_caggs)
	{
		LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG),
						RowExclusiveLock);
		LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
						RowExclusiveLock);

		/* The trigger will be dropped if the hypertable still exists and no other
		 * caggs attached. */
		if (OidIsValid(raw_hypertable.objectId))
		{
			ObjectAddressSet(raw_hypertable_trig,
							 TriggerRelationId,
							 get_trigger_oid(raw_hypertable.objectId,
											 CAGGINVAL_TRIGGER_NAME,
											 false));

			/* Raw hypertable is locked above */
			LockRelationOid(raw_hypertable_trig.objectId, AccessExclusiveLock);
		}
	}

	/*
	 * Following objects might be already dropped in case of CASCADE
	 * drop including the associated schema object.
	 */
	partial_view = get_and_lock_rel_by_name(&cadata->partial_view_schema,
											&cadata->partial_view_name,
											AccessExclusiveLock);

	direct_view = get_and_lock_rel_by_name(&cadata->direct_view_schema,
										   &cadata->direct_view_name,
										   AccessExclusiveLock);

	/* Delete catalog entry */
	iterator = ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);
	init_scan_by_mat_hypertable_id(&iterator, cadata->mat_hypertable_id);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		FormData_continuous_agg form;

		continuous_agg_formdata_fill(&form, ti);

		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));

		/* Delete all related rows */
		if (!raw_hypertable_has_other_caggs)
		{
			hypertable_invalidation_log_delete(form.raw_hypertable_id);
		}

		ts_materialization_invalidation_log_delete_inner(form.mat_hypertable_id);

		if (!raw_hypertable_has_other_caggs)
		{
			invalidation_threshold_delete(form.raw_hypertable_id);
		}

		/* Delete watermark */
		ts_cagg_watermark_delete_by_mat_hypertable_id(form.mat_hypertable_id);
	}

	cagg_bucket_function_delete(cadata->mat_hypertable_id);

	/* Perform actual deletions now */
	if (OidIsValid(user_view.objectId))
		performDeletion(&user_view, DROP_RESTRICT, 0);

	if (OidIsValid(raw_hypertable_trig.objectId))
	{
		ts_hypertable_drop_trigger(raw_hypertable.objectId, CAGGINVAL_TRIGGER_NAME);
	}

	if (OidIsValid(mat_hypertable.objectId))
	{
		performDeletion(&mat_hypertable, DROP_CASCADE, 0);
		ts_hypertable_delete_by_id(cadata->mat_hypertable_id);
	}

	if (OidIsValid(partial_view.objectId))
		performDeletion(&partial_view, DROP_RESTRICT, 0);

	if (OidIsValid(direct_view.objectId))
		performDeletion(&direct_view, DROP_RESTRICT, 0);
}

/*
 * This is a called when a hypertable gets dropped.
 *
 * If the hypertable is a raw hypertable for a continuous agg,
 * drop the continuous agg.
 *
 * If the hypertable is a materialization hypertable, error out
 * and force the user to drop the continuous agg instead.
 */
void
ts_continuous_agg_drop_hypertable_callback(int32 hypertable_id)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);

	ts_scanner_foreach(&iterator)
	{
		FormData_continuous_agg data;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);

		continuous_agg_formdata_fill(&data, ti);

		if (data.raw_hypertable_id == hypertable_id)
			drop_continuous_agg(&data, true);

		if (data.mat_hypertable_id == hypertable_id)
			ereport(ERROR,
					(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					 errmsg("cannot drop the materialized table because it is required by a "
							"continuous aggregate")));
	}
}

/* Block dropping the partial and direct view if the continuous aggregate still exists */
static void
drop_internal_view(const FormData_continuous_agg *fd)
{
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);
	int count = 0;
	init_scan_by_mat_hypertable_id(&iterator, fd->mat_hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		ts_catalog_delete_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti));
		count++;
	}
	if (count > 0)
		ereport(ERROR,
				(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
				 errmsg(
					 "cannot drop the partial/direct view because it is required by a continuous "
					 "aggregate")));
}

/* This gets called when a view gets dropped. */
static void
continuous_agg_drop_view_callback(FormData_continuous_agg *fd, const char *schema, const char *name)
{
	ContinuousAggViewType vtyp;
	vtyp = ts_continuous_agg_view_type(fd, schema, name);
	switch (vtyp)
	{
		case ContinuousAggUserView:
			drop_continuous_agg(fd, false /* The user view has already been dropped */);
			break;
		case ContinuousAggPartialView:
		case ContinuousAggDirectView:
			drop_internal_view(fd);
			break;
		default:
			elog(ERROR, "unknown continuous aggregate view type");
	}
}

bool
ts_continuous_agg_drop(const char *view_schema, const char *view_name)
{
	FormData_continuous_agg fd;
	bool found = continuous_agg_find_by_name(view_schema, view_name, ContinuousAggAnyView, &fd);

	if (found)
		continuous_agg_drop_view_callback(&fd, view_schema, view_name);

	return found;
}

static inline bool
ts_continuous_agg_is_user_view_schema(FormData_continuous_agg *data, const char *schema)
{
	return CHECK_NAME_MATCH(&data->user_view_schema, schema);
}

static inline bool
ts_continuous_agg_is_partial_view_schema(FormData_continuous_agg *data, const char *schema)
{
	return CHECK_NAME_MATCH(&data->partial_view_schema, schema);
}

static inline bool
ts_continuous_agg_is_direct_view_schema(FormData_continuous_agg *data, const char *schema)
{
	return CHECK_NAME_MATCH(&data->direct_view_schema, schema);
}

ContinuousAggViewType
ts_continuous_agg_view_type(FormData_continuous_agg *data, const char *schema, const char *name)
{
	if (CHECK_NAME_MATCH(&data->user_view_schema, schema) &&
		CHECK_NAME_MATCH(&data->user_view_name, name))
		return ContinuousAggUserView;
	else if (CHECK_NAME_MATCH(&data->partial_view_schema, schema) &&
			 CHECK_NAME_MATCH(&data->partial_view_name, name))
		return ContinuousAggPartialView;
	else if (CHECK_NAME_MATCH(&data->direct_view_schema, schema) &&
			 CHECK_NAME_MATCH(&data->direct_view_name, name))
		return ContinuousAggDirectView;
	else
		return ContinuousAggAnyView;
}

typedef struct CaggRenameCtx
{
	const char *old_schema;
	const char *old_name;
	const char *new_schema;
	const char *new_name;
	ObjectType *object_type;
	void (*process_rename)(FormData_continuous_agg *form, bool *do_update, void *data);
} CaggRenameCtx;

static void
continuous_agg_rename_process_rename_schema(FormData_continuous_agg *form, bool *do_update,
											void *data)
{
	CaggRenameCtx *ctx = (CaggRenameCtx *) data;

	if (ts_continuous_agg_is_user_view_schema(form, ctx->old_schema))
	{
		namestrcpy(&form->user_view_schema, ctx->new_schema);
		*do_update = true;
	}

	if (ts_continuous_agg_is_partial_view_schema(form, ctx->old_schema))
	{
		namestrcpy(&form->partial_view_schema, ctx->new_schema);
		*do_update = true;
	}

	if (ts_continuous_agg_is_direct_view_schema(form, ctx->old_schema))
	{
		namestrcpy(&form->direct_view_schema, ctx->new_schema);
		*do_update = true;
	}
}

static void
continuous_agg_rename_process_rename_view(FormData_continuous_agg *form, bool *do_update,
										  void *data)
{
	CaggRenameCtx *ctx = (CaggRenameCtx *) data;
	ContinuousAggViewType vtyp;

	vtyp = ts_continuous_agg_view_type(form, ctx->old_schema, ctx->old_name);

	switch (vtyp)
	{
		case ContinuousAggUserView:
		{
			if (*ctx->object_type == OBJECT_VIEW)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter continuous aggregate using ALTER VIEW"),
						 errhint("Use ALTER MATERIALIZED VIEW to alter a continuous aggregate.")));

			Assert(*ctx->object_type == OBJECT_MATVIEW);
			*ctx->object_type = OBJECT_VIEW;

			namestrcpy(&form->user_view_schema, ctx->new_schema);
			namestrcpy(&form->user_view_name, ctx->new_name);
			*do_update = true;
			break;
		}
		case ContinuousAggPartialView:
		{
			namestrcpy(&form->partial_view_schema, ctx->new_schema);
			namestrcpy(&form->partial_view_name, ctx->new_name);
			*do_update = true;
			break;
		}
		case ContinuousAggDirectView:
		{
			namestrcpy(&form->direct_view_schema, ctx->new_schema);
			namestrcpy(&form->direct_view_name, ctx->new_name);
			*do_update = true;
			break;
		}
		default:
			break;
	}
}

static ScanTupleResult
continuous_agg_rename(TupleInfo *ti, void *data)
{
	CaggRenameCtx *ctx = (CaggRenameCtx *) data;
	FormData_continuous_agg form;
	bool do_update = false;
	CatalogSecurityContext sec_ctx;

	continuous_agg_formdata_fill(&form, ti);

	ctx->process_rename(&form, &do_update, (void *) ctx);

	if (do_update)
	{
		HeapTuple new_tuple =
			continuous_agg_formdata_make_tuple(&form, ts_scanner_get_tupledesc(ti));
		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		ts_catalog_update_tid(ti->scanrel, ts_scanner_get_tuple_tid(ti), new_tuple);
		ts_catalog_restore_user(&sec_ctx);
		heap_freetuple(new_tuple);
	}

	return SCAN_CONTINUE;
}

void
ts_continuous_agg_rename_schema_name(const char *old_schema, const char *new_schema)
{
	CaggRenameCtx cagg_rename_ctx = {
		.old_schema = old_schema,
		.new_schema = new_schema,
		.process_rename = continuous_agg_rename_process_rename_schema,
	};

	Catalog *catalog = ts_catalog_get();

	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CONTINUOUS_AGG),
		.index = InvalidOid,
		.tuple_found = continuous_agg_rename,
		.data = &cagg_rename_ctx,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};

	ts_scanner_scan(&scanctx);
}

void
ts_continuous_agg_rename_view(const char *old_schema, const char *old_name, const char *new_schema,
							  const char *new_name, ObjectType *object_type)
{
	CaggRenameCtx cagg_rename_ctx = {
		.old_schema = old_schema,
		.old_name = old_name,
		.new_schema = new_schema,
		.new_name = new_name,
		.object_type = object_type,
		.process_rename = continuous_agg_rename_process_rename_view,
	};

	Catalog *catalog = ts_catalog_get();

	ScannerCtx scanctx = {
		.table = catalog_get_table_id(catalog, CONTINUOUS_AGG),
		.index = InvalidOid,
		.tuple_found = continuous_agg_rename,
		.data = &cagg_rename_ctx,
		.lockmode = RowExclusiveLock,
		.scandirection = ForwardScanDirection,
	};

	ts_scanner_scan(&scanctx);
}

static int32
find_raw_hypertable_for_materialization(int32 mat_hypertable_id)
{
	PG_USED_FOR_ASSERTS_ONLY short count = 0;
	int32 htid = INVALID_HYPERTABLE_ID;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, RowExclusiveLock, CurrentMemoryContext);

	init_scan_by_mat_hypertable_id(&iterator, mat_hypertable_id);
	ts_scanner_foreach(&iterator)
	{
		bool isnull;
		Datum datum = slot_getattr(ts_scan_iterator_slot(&iterator),
								   Anum_continuous_agg_raw_hypertable_id,
								   &isnull);

		Assert(!isnull);
		htid = DatumGetInt32(datum);
		count++;
	}
	Assert(count <= 1);
	ts_scan_iterator_close(&iterator);
	return htid;
}

/* Continuous aggregate materialization hypertables inherit integer_now func
 * from the raw hypertable (unless it was explicitly reset for cont. aggregate.
 * Walk the materialization hypertable ->raw hypertable tree till
 * we find a hypertable that has integer_now_func set.
 */
TSDLLEXPORT const Dimension *
ts_continuous_agg_find_integer_now_func_by_materialization_id(int32 mat_htid)
{
	int32 raw_htid = mat_htid;
	const Dimension *par_dim = NULL;
	while (raw_htid != INVALID_HYPERTABLE_ID)
	{
		Hypertable *raw_ht = ts_hypertable_get_by_id(raw_htid);
		const Dimension *open_dim = hyperspace_get_open_dimension(raw_ht->space, 0);
		if (strlen(NameStr(open_dim->fd.integer_now_func)) != 0 &&
			strlen(NameStr(open_dim->fd.integer_now_func_schema)) != 0)
		{
			par_dim = open_dim;
			break;
		}
		mat_htid = raw_htid;
		raw_htid = find_raw_hypertable_for_materialization(mat_htid);
	}
	return par_dim;
}

TSDLLEXPORT void
ts_continuous_agg_invalidate_chunk(Hypertable *ht, Chunk *chunk)
{
	int64 start = ts_chunk_primary_dimension_start(chunk);
	int64 end = ts_chunk_primary_dimension_end(chunk);

	Assert(hyperspace_get_open_dimension(ht->space, 0)->fd.id ==
		   chunk->cube->slices[0]->fd.dimension_id);
	ts_cm_functions->continuous_agg_invalidate_raw_ht(ht, start, end);
}

/* Determines if a bucket is using integer or an interval partitioning */
bool
ts_continuous_agg_bucket_on_interval(Oid bucket_function)
{
	Assert(OidIsValid(bucket_function));

	FuncInfo *func_info = ts_func_cache_get(bucket_function);
	Ensure(func_info != NULL, "unable to get function info for Oid %d", bucket_function);

	/* The function has to be a currently allowed function or one of the deprecated bucketing
	 * functions */
	Assert(func_info->allowed_in_cagg_definition || IS_DEPRECATED_TIME_BUCKET_NG_FUNC(func_info));

	Oid first_bucket_arg = func_info->arg_types[0];

	return first_bucket_arg == INTERVALOID;
}

/*
 * Calls the desired time bucket function depending on the arguments. If the experimental flag is
 * set on ContinuousAggsBucketFunction, one of time_bucket_ng() versions is used. This is a common
 * procedure used by ts_compute_* below.
 */
static Datum
generic_time_bucket(const ContinuousAggsBucketFunction *bf, Datum timestamp)
{
	FuncInfo *func_info = ts_func_cache_get_bucketing_func(bf->bucket_function);
	Ensure(func_info != NULL, "unable to get bucket function for Oid %d", bf->bucket_function);
	bool is_experimental = func_info->origin == ORIGIN_TIMESCALE_EXPERIMENTAL;

	if (!is_experimental)
	{
		if (bf->bucket_time_timezone != NULL)
		{
			if (TIMESTAMP_NOT_FINITE(bf->bucket_time_origin))
			{
				/* using default origin */
				return DirectFunctionCall3(ts_timestamptz_timezone_bucket,
										   IntervalPGetDatum(bf->bucket_time_width),
										   timestamp,
										   CStringGetTextDatum(bf->bucket_time_timezone));
			}
			else
			{
				/* custom origin specified */
				return DirectFunctionCall4(ts_timestamptz_timezone_bucket,
										   IntervalPGetDatum(bf->bucket_time_width),
										   timestamp,
										   CStringGetTextDatum(bf->bucket_time_timezone),
										   TimestampTzGetDatum(bf->bucket_time_origin));
			}
		}

		if (TIMESTAMP_NOT_FINITE(bf->bucket_time_origin))
		{
			/* using default origin */
			return DirectFunctionCall2(ts_timestamp_bucket,
									   IntervalPGetDatum(bf->bucket_time_width),
									   timestamp);
		}
		else
		{
			/* custom origin specified */
			return DirectFunctionCall3(ts_timestamp_bucket,
									   IntervalPGetDatum(bf->bucket_time_width),
									   timestamp,
									   TimestampTzGetDatum(bf->bucket_time_origin));
		}
	}
	else
	{
		if (bf->bucket_time_timezone != NULL)
		{
			if (TIMESTAMP_NOT_FINITE(bf->bucket_time_origin))
			{
				/* using default origin */
				return DirectFunctionCall3(ts_time_bucket_ng_timezone,
										   IntervalPGetDatum(bf->bucket_time_width),
										   timestamp,
										   CStringGetTextDatum(bf->bucket_time_timezone));
			}
			else
			{
				/* custom origin specified */
				return DirectFunctionCall4(ts_time_bucket_ng_timezone_origin,
										   IntervalPGetDatum(bf->bucket_time_width),
										   timestamp,
										   TimestampTzGetDatum(bf->bucket_time_origin),
										   CStringGetTextDatum(bf->bucket_time_timezone));
			}
		}

		if (TIMESTAMP_NOT_FINITE(bf->bucket_time_origin))
		{
			/* using default origin */
			return DirectFunctionCall2(ts_time_bucket_ng_timestamp,
									   IntervalPGetDatum(bf->bucket_time_width),
									   timestamp);
		}
		else
		{
			/* custom origin specified */
			return DirectFunctionCall3(ts_time_bucket_ng_timestamp,
									   IntervalPGetDatum(bf->bucket_time_width),
									   timestamp,
									   TimestampTzGetDatum(bf->bucket_time_origin));
		}
	}
}

/*
 * Adds one bf->bucket_size interval to the timestamp. This is a common
 * procedure used by ts_compute_* below.
 *
 * If bf->bucket_time_timezone is specified, the math happens in this timezone.
 * Otherwise, it happens in UTC.
 */
static Datum
generic_add_interval(const ContinuousAggsBucketFunction *bf, Datum timestamp)
{
	Datum tzname = 0;
	bool has_timezone = (bf->bucket_time_timezone != NULL);

	if (has_timezone)
	{
		/*
		 * Convert 'timestamp' to TIMESTAMP at given timezone.
		 * The code is equal to 'timestamptz AT TIME ZONE tzname'.
		 */
		tzname = CStringGetTextDatum(bf->bucket_time_timezone);
		timestamp = DirectFunctionCall2(timestamptz_zone, tzname, timestamp);
	}

	timestamp = DirectFunctionCall2(timestamp_pl_interval,
									timestamp,
									IntervalPGetDatum(bf->bucket_time_width));

	if (has_timezone)
	{
		Assert(tzname != 0);
		timestamp = DirectFunctionCall2(timestamp_zone, tzname, timestamp);
	}

	return timestamp;
}

/*
 * Computes inscribed refresh_window for variable-sized buckets.
 *
 * The algorithm is simple:
 *
 * end = time_bucket(bucket_size, end)
 *
 * if(start != time_bucket(bucket_size, start))
 *     start = time_bucket(bucket_size, start) + interval bucket_size
 *
 */
void
ts_compute_inscribed_bucketed_refresh_window_variable(int64 *start, int64 *end,
													  const ContinuousAggsBucketFunction *bf)
{
	Datum start_old, end_old, start_aligned, end_aliged;

	/*
	 * It's OK to use TIMESTAMPOID here. Variable-sized buckets can be used
	 * only for dates, timestamps and timestamptz's. For all these types our
	 * internal time representation is microseconds relative the UNIX epoch.
	 * So the results will be correct regardless of the actual type used in
	 * the CAGG. For more details see ts_internal_to_time_value() implementation.
	 */
	start_old = ts_internal_to_time_value(*start, TIMESTAMPOID);
	end_old = ts_internal_to_time_value(*end, TIMESTAMPOID);

	start_aligned = generic_time_bucket(bf, start_old);
	end_aliged = generic_time_bucket(bf, end_old);

	if (DatumGetTimestamp(start_aligned) != DatumGetTimestamp(start_old))
	{
		start_aligned = generic_add_interval(bf, start_aligned);
	}

	*start = ts_time_value_to_internal(start_aligned, TIMESTAMPOID);
	*end = ts_time_value_to_internal(end_aliged, TIMESTAMPOID);
}

/*
 * Computes circumscribed refresh_window for variable-sized buckets.
 *
 * The algorithm is simple:
 *
 * start = time_bucket(bucket_size, start)
 *
 * if(end != time_bucket(bucket_size, end))
 *     end = time_bucket(bucket_size, end) + interval bucket_size
 */
void
ts_compute_circumscribed_bucketed_refresh_window_variable(int64 *start, int64 *end,
														  const ContinuousAggsBucketFunction *bf)
{
	Datum start_old, end_old, start_new, end_new;

	/*
	 * It's OK to use TIMESTAMPOID here.
	 * See the comment in ts_compute_inscribed_bucketed_refresh_window_variable()
	 */
	start_old = ts_internal_to_time_value(*start, TIMESTAMPOID);
	end_old = ts_internal_to_time_value(*end, TIMESTAMPOID);
	start_new = generic_time_bucket(bf, start_old);
	end_new = generic_time_bucket(bf, end_old);

	if (DatumGetTimestamp(end_new) != DatumGetTimestamp(end_old))
	{
		end_new = generic_add_interval(bf, end_new);
	}

	*start = ts_time_value_to_internal(start_new, TIMESTAMPOID);
	*end = ts_time_value_to_internal(end_new, TIMESTAMPOID);
}

/*
 * Calculates the beginning of the next bucket.
 *
 * The algorithm is just:
 *
 * val = time_bucket(bucket_size, val) + interval bucket_size
 */
int64
ts_compute_beginning_of_the_next_bucket_variable(int64 timeval,
												 const ContinuousAggsBucketFunction *bf)
{
	Datum val_new;
	Datum val_old;

	/*
	 * It's OK to use TIMESTAMPOID here.
	 * See the comment in ts_compute_inscribed_bucketed_refresh_window_variable()
	 */
	val_old = ts_internal_to_time_value(timeval, TIMESTAMPOID);

	val_new = generic_time_bucket(bf, val_old);
	val_new = generic_add_interval(bf, val_new);
	return ts_time_value_to_internal(val_new, TIMESTAMPOID);
}

Oid
ts_cagg_permissions_check(Oid cagg_oid, Oid userid)
{
	Oid ownerid = ts_rel_get_owner(cagg_oid);

	if (!has_privs_of_role(userid, ownerid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of continuous aggregate \"%s\"", get_rel_name(cagg_oid))));

	return ownerid;
}

Query *
ts_continuous_agg_get_query(ContinuousAgg *cagg)
{
	Oid cagg_view_oid;
	Relation cagg_view_rel;
	RuleLock *cagg_view_rules;
	RewriteRule *rule;
	Query *cagg_view_query;

	/*
	 * Get the direct_view definition for the finalized version because
	 * the user view doesn't have the "GROUP BY" clause anymore.
	 */
	if (ContinuousAggIsFinalized(cagg))
		cagg_view_oid = ts_get_relation_relid(NameStr(cagg->data.direct_view_schema),
											  NameStr(cagg->data.direct_view_name),
											  false);
	else
		cagg_view_oid = ts_get_relation_relid(NameStr(cagg->data.user_view_schema),
											  NameStr(cagg->data.user_view_name),
											  false);

	cagg_view_rel = table_open(cagg_view_oid, AccessShareLock);
	cagg_view_rules = cagg_view_rel->rd_rules;
	Assert(cagg_view_rules && cagg_view_rules->numLocks == 1);

	rule = cagg_view_rules->rules[0];
	if (rule->event != CMD_SELECT)
		ereport(ERROR, (errcode(ERRCODE_TS_UNEXPECTED), errmsg("unexpected rule event for view")));

	cagg_view_query = (Query *) copyObject(linitial(rule->actions));
	table_close(cagg_view_rel, NoLock);

	return cagg_view_query;
}

/*
 * Get the width of a fixed size bucket
 */
int64
ts_continuous_agg_fixed_bucket_width(const ContinuousAggsBucketFunction *bucket_function)
{
	Assert(bucket_function->bucket_fixed_interval == true);

	if (bucket_function->bucket_time_based)
	{
		Interval *interval = bucket_function->bucket_time_width;
		Assert(interval->month == 0);
		return interval->time + (interval->day * USECS_PER_DAY);
	}
	else
	{
		return bucket_function->bucket_integer_width;
	}
}
