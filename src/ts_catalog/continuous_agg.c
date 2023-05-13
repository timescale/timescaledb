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
#include "ts_catalog/continuous_agg.h"
#include "ts_catalog/continuous_aggs_watermark.h"
#include "cross_module_fn.h"
#include "hypercube.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "scan_iterator.h"
#include "time_bucket.h"
#include "time_utils.h"
#include "ts_catalog/catalog.h"
#include "errors.h"
#include "compression_with_clause.h"
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
			.default_val = (Datum)false,
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
			DefElem *elem = makeDefElemExtended("timescaledb",
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

TS_FUNCTION_INFO_V1(ts_hypertable_invalidation_log_delete);
/**
 * Delete hypertable invalidation log entries for all the CAGGs that belong to the
 * distributed hypertable with hypertable ID 'raw_hypertable_id' in the Access Node.
 *
 * @param raw_hypertable_id - The hypertable ID of the original distributed hypertable in the
 *                            Access Node.
 */
Datum
ts_hypertable_invalidation_log_delete(PG_FUNCTION_ARGS)
{
	int32 raw_hypertable_id = PG_GETARG_INT32(0);

	elog(DEBUG1, "invalidation log delete for hypertable %d", raw_hypertable_id);
	hypertable_invalidation_log_delete(raw_hypertable_id);
	PG_RETURN_VOID();
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

TS_FUNCTION_INFO_V1(ts_materialization_invalidation_log_delete);
/**
 * Delete materialization invalidation log entries for the CAGG that belong to the
 * materialized hypertable with ID 'mat_hypertable_id' in the Access Node.
 *
 * @param mat_hypertable_id The hypertable ID of the CAGG materialized hypertable in the Access
 *                          Node.
 */
Datum
ts_materialization_invalidation_log_delete(PG_FUNCTION_ARGS)
{
	int32 mat_hypertable_id = PG_GETARG_INT32(0);
	ts_materialization_invalidation_log_delete_inner(mat_hypertable_id);
	PG_RETURN_VOID();
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

	values[AttrNumberGetAttrOffset(Anum_continuous_agg_bucket_width)] =
		Int64GetDatum(fd->bucket_width);

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

	memcpy(&fd->user_view_schema,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_schema)]),
		   NAMEDATALEN);
	memcpy(&fd->user_view_name,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_continuous_agg_user_view_name)]),
		   NAMEDATALEN);

	memcpy(&fd->partial_view_schema,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_schema)]),
		   NAMEDATALEN);
	memcpy(&fd->partial_view_name,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_continuous_agg_partial_view_name)]),
		   NAMEDATALEN);

	fd->bucket_width =
		DatumGetInt64(values[AttrNumberGetAttrOffset(Anum_continuous_agg_bucket_width)]);

	memcpy(&fd->direct_view_schema,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_schema)]),
		   NAMEDATALEN);
	memcpy(&fd->direct_view_name,
		   DatumGetName(values[AttrNumberGetAttrOffset(Anum_continuous_agg_direct_view_name)]),
		   NAMEDATALEN);

	fd->materialized_only =
		DatumGetBool(values[AttrNumberGetAttrOffset(Anum_continuous_agg_materialize_only)]);
	fd->finalized = DatumGetBool(values[AttrNumberGetAttrOffset(Anum_continuous_agg_finalized)]);

	if (should_free)
		heap_freetuple(tuple);
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
		const char *bucket_width_str;
		const char *origin_str;
		Datum values[Natts_continuous_aggs_bucket_function];
		bool isnull[Natts_continuous_aggs_bucket_function];

		bool should_free;
		HeapTuple tuple = ts_scan_iterator_fetch_heap_tuple(&iterator, false, &should_free);

		/*
		 * Our usual GETSTRUCT() approach doesn't work when TEXT fields are involved,
		 * thus a more robust approach with heap_deform_tuple() is used here.
		 */
		heap_deform_tuple(tuple, ts_scan_iterator_tupledesc(&iterator), values, isnull);

		Assert(!isnull[Anum_continuous_aggs_bucket_function_experimental - 1]);
		bf->experimental =
			DatumGetBool(values[Anum_continuous_aggs_bucket_function_experimental - 1]);

		Assert(!isnull[Anum_continuous_aggs_bucket_function_name - 1]);
		bf->name = TextDatumGetCString(values[Anum_continuous_aggs_bucket_function_name - 1]);

		/*
		 * So far bucket_width is stored as TEXT for flexibility, but it's type
		 * most likely is going to change to Interval when the variable-sized
		 * buckets feature will stabilize.
		 */
		Assert(!isnull[Anum_continuous_aggs_bucket_function_bucket_width - 1]);
		bucket_width_str =
			TextDatumGetCString(values[Anum_continuous_aggs_bucket_function_bucket_width - 1]);
		Assert(strlen(bucket_width_str) > 0);
		bf->bucket_width = DatumGetIntervalP(
			DirectFunctionCall3(interval_in, CStringGetDatum(bucket_width_str), InvalidOid, -1));

		Assert(!isnull[Anum_continuous_aggs_bucket_function_origin - 1]);
		origin_str = TextDatumGetCString(values[Anum_continuous_aggs_bucket_function_origin - 1]);
		if (strlen(origin_str) == 0)
			TIMESTAMP_NOBEGIN(bf->origin);
		else
			bf->origin = DatumGetTimestamp(DirectFunctionCall3(timestamp_in,
															   CStringGetDatum(origin_str),
															   ObjectIdGetDatum(InvalidOid),
															   Int32GetDatum(-1)));

		Assert(!isnull[Anum_continuous_aggs_bucket_function_timezone - 1]);
		bf->timezone =
			TextDatumGetCString(values[Anum_continuous_aggs_bucket_function_timezone - 1]);

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

	if (ts_continuous_agg_bucket_width_variable(cagg))
	{
		cagg->bucket_function = palloc0(sizeof(ContinuousAggsBucketFunction));
		continuous_agg_fill_bucket_function(cagg->data.mat_hypertable_id, cagg->bucket_function);
	}
}

TSDLLEXPORT CaggsInfo
ts_continuous_agg_get_all_caggs_info(int32 raw_hypertable_id)
{
	CaggsInfo all_caggs_info;

	List *caggs = ts_continuous_aggs_find_by_raw_table_id(raw_hypertable_id);
	ListCell *lc;
	Datum bucket_width;

	all_caggs_info.bucket_widths = NIL;
	all_caggs_info.mat_hypertable_ids = NIL;
	all_caggs_info.bucket_functions = NIL;

	Assert(list_length(caggs) > 0);

	foreach (lc, caggs)
	{
		ContinuousAgg *cagg = lfirst(lc);

		bucket_width = Int64GetDatum(ts_continuous_agg_bucket_width_variable(cagg) ?
										 BUCKET_WIDTH_VARIABLE :
										 ts_continuous_agg_bucket_width(cagg));
		all_caggs_info.bucket_widths =
			lappend(all_caggs_info.bucket_widths, DatumGetPointer(bucket_width));

		all_caggs_info.bucket_functions =
			lappend(all_caggs_info.bucket_functions, cagg->bucket_function);

		all_caggs_info.mat_hypertable_ids =
			lappend_int(all_caggs_info.mat_hypertable_ids, cagg->data.mat_hypertable_id);
	}
	return all_caggs_info;
}

/*
 * Serializes ContinuousAggsBucketFunction* into a string like:
 *
 *     ver;bucket_width;origin;timezone;
 *
 * ... where ver is a version of the serialization format. This particular format
 * was chosen because of it's simplicity and good performance. Future versions
 * of the procedure can use other format (like key=value or JSON) and/or add
 * extra fields. NULL pointer is serialized to an empty string.
 *
 * Note that the schema and the name of the function are not serialized. This
 * is intentional since this information is currently not used for anything.
 * We can serialize the name of the function as well when and if this would be
 * necessary.
 */
static const char *
bucket_function_serialize(const ContinuousAggsBucketFunction *bf)
{
	const char *bucket_width_str;
	const char *origin_str = "";
	StringInfo str;

	if (NULL == bf)
		return "";

	str = makeStringInfo();

	/* We are pretty sure that user can't place ';' character in this field */
	Assert(strstr(bf->timezone, ";") == NULL);

	bucket_width_str =
		DatumGetCString(DirectFunctionCall1(interval_out, IntervalPGetDatum(bf->bucket_width)));

	if (!TIMESTAMP_NOT_FINITE(bf->origin))
	{
		origin_str =
			DatumGetCString(DirectFunctionCall1(timestamp_out, TimestampGetDatum(bf->origin)));
	}

	appendStringInfo(str,
					 "%d;%s;%s;%s;",
					 BUCKET_FUNCTION_SERIALIZE_VERSION,
					 bucket_width_str,
					 origin_str,
					 bf->timezone);

	return str->data;
}

/*
 * Deserielizes a string into a palloc'ated ContinuousAggsBucketFunction*. Note
 * that NULL is also a valid return value.
 *
 * See bucket_function_serialize() for more details.
 */
static const ContinuousAggsBucketFunction *
bucket_function_deserialize(const char *str)
{
	char *begin, *end, *strings[4];
	ContinuousAggsBucketFunction *bf;

	/* empty string stands for serialized NULL */
	if (*str == '\0')
		return NULL;

	begin = pstrdup(str);
	for (size_t i = 0; i < lengthof(strings); i++)
	{
		end = strstr(begin, ";");
		if (end == NULL)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("failed to deserialize \"%s\" into a bucketing function", str),
					 errdetail("separator not found")));
		}

		*end = '\0';
		strings[i] = begin;
		begin = end + 1;
	}

	/* end of string was reached */
	Assert(*begin == '\0');

	if (atoi(strings[0]) != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("failed to deserialize \"%s\" into a bucketing function", str),
				 errdetail("unsupported format version")));
	}

	bf = palloc(sizeof(ContinuousAggsBucketFunction));
	bf->experimental = true;
	bf->name = "time_bucket_ng";
	Assert(strlen(strings[1]) > 0);
	bf->bucket_width = DatumGetIntervalP(
		DirectFunctionCall3(interval_in, CStringGetDatum(strings[1]), InvalidOid, -1));

	if (strlen(strings[2]) == 0)
		TIMESTAMP_NOBEGIN(bf->origin);
	else
		bf->origin = DatumGetTimestamp(DirectFunctionCall3(timestamp_in,
														   CStringGetDatum(strings[2]),
														   ObjectIdGetDatum(InvalidOid),
														   Int32GetDatum(-1)));

	bf->timezone = strings[3];
	return bf;
}

/*
 * Does not do deep copy of Datums For performance reasons. Make sure the arrays are not deallocated
 * before CaggsInfo.
 */
TSDLLEXPORT void
ts_populate_caggs_info_from_arrays(ArrayType *mat_hypertable_ids, ArrayType *bucket_widths,
								   ArrayType *bucket_functions, CaggsInfo *all_caggs)
{
	all_caggs->mat_hypertable_ids = NIL;
	all_caggs->bucket_widths = NIL;
	all_caggs->bucket_functions = NIL;

	Assert(ARR_NDIM(mat_hypertable_ids) > 0 && ARR_NDIM(bucket_widths) > 0 &&
		   ARR_NDIM(bucket_functions) > 0);
	Assert(ARR_NDIM(mat_hypertable_ids) == ARR_NDIM(bucket_widths) &&
		   ARR_NDIM(bucket_functions) == ARR_NDIM(bucket_widths));

	ArrayIterator it_htids, it_widths, it_bfs;
	Datum array_datum1, array_datum2, array_datum3;
	bool isnull1, isnull2, isnull3;

	it_htids = array_create_iterator(mat_hypertable_ids, 0, NULL);
	it_widths = array_create_iterator(bucket_widths, 0, NULL);
	it_bfs = array_create_iterator(bucket_functions, 0, NULL);
	while (array_iterate(it_htids, &array_datum1, &isnull1) &&
		   array_iterate(it_widths, &array_datum2, &isnull2) &&
		   array_iterate(it_bfs, &array_datum3, &isnull3))
	{
		Assert(!isnull1 && !isnull2 && !isnull3);
		int32 mat_hypertable_id = DatumGetInt32(array_datum1);
		all_caggs->mat_hypertable_ids =
			lappend_int(all_caggs->mat_hypertable_ids, mat_hypertable_id);

		Datum bucket_width;
		bucket_width = array_datum2;
		all_caggs->bucket_widths = lappend(all_caggs->bucket_widths, DatumGetPointer(bucket_width));

		const ContinuousAggsBucketFunction *bucket_function =
			bucket_function_deserialize(TextDatumGetCString(array_datum3));
		/* bucket_function is cast to non-const type to make Visual Studio happy */
		all_caggs->bucket_functions =
			lappend(all_caggs->bucket_functions, (ContinuousAggsBucketFunction *) bucket_function);
	}
	array_free_iterator(it_htids);
	array_free_iterator(it_widths);
	array_free_iterator(it_bfs);
}

/*
 * Does not do deep copy of Datums For performance reasons. Make sure the Caggsinfo is not
 * deallocated before the arrays.
 */
TSDLLEXPORT void
ts_create_arrays_from_caggs_info(const CaggsInfo *all_caggs, ArrayType **mat_hypertable_ids,
								 ArrayType **bucket_widths, ArrayType **bucket_functions)
{
	ListCell *lc1, *lc2, *lc3;
	unsigned i;

	Datum *matiddatums = palloc(sizeof(Datum) * list_length(all_caggs->mat_hypertable_ids));
	Datum *widthdatums = palloc(sizeof(Datum) * list_length(all_caggs->bucket_widths));
	Datum *bucketfunctions = palloc(sizeof(Datum) * list_length(all_caggs->bucket_functions));

	i = 0;
	forthree (lc1,
			  all_caggs->mat_hypertable_ids,
			  lc2,
			  all_caggs->bucket_widths,
			  lc3,
			  all_caggs->bucket_functions)
	{
		int32 cagg_hyper_id = lfirst_int(lc1);
		matiddatums[i] = Int32GetDatum(cagg_hyper_id);

		widthdatums[i] = PointerGetDatum(lfirst(lc2));

		const ContinuousAggsBucketFunction *bucket_function = lfirst(lc3);
		bucketfunctions[i] = CStringGetTextDatum(bucket_function_serialize(bucket_function));

		++i;
	}

	*mat_hypertable_ids = construct_array(matiddatums,
										  list_length(all_caggs->mat_hypertable_ids),
										  INT4OID,
										  4,
										  true,
										  TYPALIGN_INT);

	*bucket_widths = construct_array(widthdatums,
									 list_length(all_caggs->bucket_widths),
									 INT8OID,
									 8,
									 FLOAT8PASSBYVAL,
									 TYPALIGN_DOUBLE);

	*bucket_functions = construct_array(bucketfunctions,
										list_length(all_caggs->bucket_functions),
										TEXTOID,
										-1,
										false,
										TYPALIGN_INT);
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
ts_continuous_agg_find_by_mat_hypertable_id(int32 mat_hypertable_id)
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
			if (ts_cm_functions->remote_invalidation_log_delete)
				ts_cm_functions->remote_invalidation_log_delete(form.raw_hypertable_id,
																HypertableIsRawTable);
		}

		ts_materialization_invalidation_log_delete_inner(form.mat_hypertable_id);
		if (ts_cm_functions->remote_invalidation_log_delete)
			ts_cm_functions->remote_invalidation_log_delete(form.mat_hypertable_id,
															HypertableIsMaterialization);

		if (!raw_hypertable_has_other_caggs)
		{
			invalidation_threshold_delete(form.raw_hypertable_id);
		}

		/* Delete watermark */
		ts_cagg_watermark_delete_by_mat_hypertable_id(form.mat_hypertable_id);
	}

	if (cadata->bucket_width == BUCKET_WIDTH_VARIABLE)
	{
		cagg_bucket_function_delete(cadata->mat_hypertable_id);
	}

	/* Perform actual deletions now */
	if (OidIsValid(user_view.objectId))
		performDeletion(&user_view, DROP_RESTRICT, 0);

	if (OidIsValid(raw_hypertable_trig.objectId))
	{
		ts_hypertable_drop_trigger(raw_hypertable.objectId, CAGGINVAL_TRIGGER_NAME);
		if (ts_cm_functions->remote_drop_dist_ht_invalidation_trigger)
			ts_cm_functions->remote_drop_dist_ht_invalidation_trigger(cadata->raw_hypertable_id);
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

TSDLLEXPORT int32
ts_number_of_continuous_aggs()
{
	int32 count = 0;
	ScanIterator iterator =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);
	ts_scanner_foreach(&iterator) { count++; }

	return count;
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

/* Determines if bucket width if variable for given continuous aggregate. */
bool
ts_continuous_agg_bucket_width_variable(const ContinuousAgg *agg)
{
	return agg->data.bucket_width == BUCKET_WIDTH_VARIABLE;
}

/* Determines bucket width for given continuous aggregate. */
int64
ts_continuous_agg_bucket_width(const ContinuousAgg *agg)
{
	if (ts_continuous_agg_bucket_width_variable(agg))
	{
		/* should never happen, this code is useful mostly for debugging purposes */
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("bucket width is not defined for a variable bucket")));
	}

	return agg->data.bucket_width;
}

/*
 * Calls one of time_bucket_ng() versions depending on the arguments. This is
 * a common procedure used by ts_compute_* below.
 */
static Datum
generic_time_bucket(const ContinuousAggsBucketFunction *bf, Datum timestamp)
{
	/* bf->timezone can't be NULL. If timezone is not specified, "" is stored */
	Assert(bf->timezone != NULL);

	if (!bf->experimental)
	{
		if (strlen(bf->timezone) > 0)
		{
			if (TIMESTAMP_NOT_FINITE(bf->origin))
			{
				/* using default origin */
				return DirectFunctionCall3(ts_timestamptz_timezone_bucket,
										   IntervalPGetDatum(bf->bucket_width),
										   timestamp,
										   CStringGetTextDatum(bf->timezone));
			}
			else
			{
				/* custom origin specified */
				return DirectFunctionCall4(ts_timestamptz_timezone_bucket,
										   IntervalPGetDatum(bf->bucket_width),
										   timestamp,
										   CStringGetTextDatum(bf->timezone),
										   TimestampTzGetDatum((TimestampTz) bf->origin));
			}
		}

		if (TIMESTAMP_NOT_FINITE(bf->origin))
		{
			/* using default origin */
			return DirectFunctionCall2(ts_timestamp_bucket,
									   IntervalPGetDatum(bf->bucket_width),
									   timestamp);
		}
		else
		{
			/* custom origin specified */
			return DirectFunctionCall3(ts_timestamp_bucket,
									   IntervalPGetDatum(bf->bucket_width),
									   timestamp,
									   TimestampGetDatum(bf->origin));
		}
	}
	else
	{
		if (strlen(bf->timezone) > 0)
		{
			if (TIMESTAMP_NOT_FINITE(bf->origin))
			{
				/* using default origin */
				return DirectFunctionCall3(ts_time_bucket_ng_timezone,
										   IntervalPGetDatum(bf->bucket_width),
										   timestamp,
										   CStringGetTextDatum(bf->timezone));
			}
			else
			{
				/* custom origin specified */
				return DirectFunctionCall4(ts_time_bucket_ng_timezone_origin,
										   IntervalPGetDatum(bf->bucket_width),
										   timestamp,
										   TimestampTzGetDatum((TimestampTz) bf->origin),
										   CStringGetTextDatum(bf->timezone));
			}
		}

		if (TIMESTAMP_NOT_FINITE(bf->origin))
		{
			/* using default origin */
			return DirectFunctionCall2(ts_time_bucket_ng_timestamp,
									   IntervalPGetDatum(bf->bucket_width),
									   timestamp);
		}
		else
		{
			/* custom origin specified */
			return DirectFunctionCall3(ts_time_bucket_ng_timestamp,
									   IntervalPGetDatum(bf->bucket_width),
									   timestamp,
									   TimestampGetDatum(bf->origin));
		}
	}
}

/*
 * Adds one bf->bucket_size interval to the timestamp. This is a common
 * procedure used by ts_compute_* below.
 *
 * If bf->timezone is specified, the math happens in this timezone.
 * Otherwise, it happens in UTC.
 */
static Datum
generic_add_interval(const ContinuousAggsBucketFunction *bf, Datum timestamp)
{
	Datum tzname = 0;
	bool has_timezone;

	/* bf->timezone can't be NULL. If timezone is not specified, "" is stored */
	Assert(bf->timezone != NULL);

	has_timezone = (strlen(bf->timezone) > 0);

	if (has_timezone)
	{
		/*
		 * Convert 'timestamp' to TIMESTAMP at given timezone.
		 * The code is equal to 'timestamptz AT TIME ZONE tzname'.
		 */
		tzname = CStringGetTextDatum(bf->timezone);
		timestamp = DirectFunctionCall2(timestamptz_zone, tzname, timestamp);
	}

	timestamp =
		DirectFunctionCall2(timestamp_pl_interval, timestamp, IntervalPGetDatum(bf->bucket_width));

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
	Datum start_old, end_old, start_new, end_new;
	/*
	 * It's OK to use TIMESTAMPOID here. Variable-sized buckets can be used
	 * only for dates, timestamps and timestamptz's. For all these types our
	 * internal time representation is microseconds relative the UNIX epoch.
	 * So the results will be correct regardless of the actual type used in
	 * the CAGG. For more details see ts_internal_to_time_value() implementation.
	 */
	start_old = ts_internal_to_time_value(*start, TIMESTAMPOID);
	end_old = ts_internal_to_time_value(*end, TIMESTAMPOID);

	start_new = generic_time_bucket(bf, start_old);
	end_new = generic_time_bucket(bf, end_old);

	if (DatumGetTimestamp(start_new) != DatumGetTimestamp(start_old))
	{
		start_new = generic_add_interval(bf, start_new);
	}

	*start = ts_time_value_to_internal(start_new, TIMESTAMPOID);
	*end = ts_time_value_to_internal(end_new, TIMESTAMPOID);
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
