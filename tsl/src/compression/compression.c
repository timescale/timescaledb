/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "compression/compression.h"

#include <access/heapam.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <catalog/pg_attribute.h>
#include <catalog/pg_type.h>
#include <funcapi.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/tuplesort.h>
#include <utils/typcache.h>

#include <base64_compat.h>
#include <catalog.h>
#include <utils.h>

#include "array.h"
#include "deltadelta.h"
#include "dictionary.h"
#include "gorilla.h"

#define MAX_ROWS_PER_COMPRESSION 1000

static const CompressionAlgorithmDefinition definitions[_END_COMPRESSION_ALGORITHMS] = {
	[COMPRESSION_ALGORITHM_ARRAY] = ARRAY_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_DICTIONARY] = DICTIONARY_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_GORILLA] = GORILLA_ALGORITHM_DEFINITION,
	[COMPRESSION_ALGORITHM_DELTADELTA] = DELTA_DELTA_ALGORITHM_DEFINITION,
};

static Compressor *
compressor_for_algorithm_and_type(CompressionAlgorithms algorithm, Oid type)
{
	if (algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);

	return definitions[algorithm].compressor_for_type(type);
}

typedef struct SegmentInfo
{
	Datum val;
	FmgrInfo eq_fn;
	FunctionCallInfoData eq_fcinfo;
	int16 typlen;
	bool is_null;
	bool typ_by_val;
} SegmentInfo;

typedef struct PerColumn
{
	/* the compressor to use for regular columns, NULL for segmenters */
	Compressor *compressor;

	/* segment info; only used if compressor is NULL */
	SegmentInfo *segment_info;
} PerColumn;

typedef struct RowCompressor
{
	/* the table we're writing the compressed data to */
	Relation compressed_table;
	BulkInsertState bistate;

	/* in theory we could have more input columns than outputted ones, so we
	   store the number of inputs/compressors seperately*/
	int n_input_columns;

	/* info about each column */
	struct PerColumn *per_column;

	/* the order of columns in the compressed data need not match the order in the
	 * uncompressed. This array maps each attribute offset in the uncompressed
	 * data to the corresponding one in the compressed
	 */
	int16 *uncompressed_col_to_compressed_col;

	/* the number of uncompressed rows compressed into the current compressed row */
	uint32 rows_compressed_into_current_value;

	/* cached arrays used to build the HeapTuple */
	Datum *compressed_values;
	bool *compressed_is_null;
} RowCompressor;

static int16 *compress_chunk_populate_keys(Oid in_table, const ColumnCompressionInfo **columns,
										   int n_columns, int *n_keys_out,
										   const ColumnCompressionInfo ***keys_out);
static Tuplesortstate *compress_chunk_sort_relation(Relation in_rel, int n_keys,
													const ColumnCompressionInfo **keys);
static void row_compressor_init(RowCompressor *row_compressor, TupleDesc uncompressed_tuple_desc,
								Relation compressed_table, int num_compression_infos,
								const ColumnCompressionInfo **column_compression_info,
								int16 *column_offsets, int16 num_compressed_columns);
static void row_compressor_append_sorted_rows(RowCompressor *row_compressor,
											  Tuplesortstate *sorted_rel, TupleDesc sorted_desc);
static void row_compressor_finish(RowCompressor *row_compressor);

/********************
 ** compress_chunk **
 ********************/

void
compress_chunk(Oid in_table, Oid out_table, const ColumnCompressionInfo **column_compression_info,
			   int num_compression_infos)
{
	int n_keys;
	const ColumnCompressionInfo **keys;
	int16 *in_column_offsets = compress_chunk_populate_keys(in_table,
															column_compression_info,
															num_compression_infos,
															&n_keys,
															&keys);

	/*We want to prevent other compressors from compressing this table,
	 * and we want to prevent INSERTs or UPDATEs which could mess up our compression.
	 * We may as well allow readers to keep reading the uncompressed data while
	 * we are compressing, so we only take an ExclusiveLock instead of AccessExclusive.
	 */
	Relation in_rel = relation_open(in_table, ExclusiveLock);
	/* we are _just_ INSERTing into the out_table so in principle we could take
	 * a RowExclusive lock, and let other operations read and write this table
	 * as we work. However, we currently compress each table as a oneshot, so
	 * we're taking the stricter lock to prevent accidents.
	 */
	Relation out_rel = relation_open(out_table, ExclusiveLock);
	// TODO error if out_rel is non-empty

	TupleDesc in_desc = RelationGetDescr(in_rel);
	TupleDesc out_desc = RelationGetDescr(out_rel);

	Tuplesortstate *sorted_rel = compress_chunk_sort_relation(in_rel, n_keys, keys);

	RowCompressor row_compressor;

	Assert(num_compression_infos <= in_desc->natts);
	Assert(num_compression_infos <= out_desc->natts);

	row_compressor_init(&row_compressor,
						in_desc,
						out_rel,
						num_compression_infos,
						column_compression_info,
						in_column_offsets,
						out_desc->natts);

	row_compressor_append_sorted_rows(&row_compressor, sorted_rel, in_desc);

	row_compressor_finish(&row_compressor);

	tuplesort_end(sorted_rel);

	RelationClose(out_rel);
	RelationClose(in_rel);
}

static int16 *
compress_chunk_populate_keys(Oid in_table, const ColumnCompressionInfo **columns, int n_columns,
							 int *n_keys_out, const ColumnCompressionInfo ***keys_out)
{
	int16 *column_offsets = palloc(sizeof(*column_offsets) * n_columns);

	int i;
	int n_segment_keys = 0;
	*n_keys_out = 0;

	for (i = 0; i < n_columns; i++)
	{
		bool is_segmentby = columns[i]->segmentby_column_index >= 0;
		bool is_orderby = columns[i]->orderby_column_index >= 0;
		if (is_segmentby)
			n_segment_keys += 1;

		if (is_segmentby || is_orderby)
			*n_keys_out += 1;
	}

	*keys_out = palloc(sizeof(**keys_out) * *n_keys_out);

	for (i = 0; i < n_columns; i++)
	{
		const ColumnCompressionInfo *column = columns[i];
		int16 segment_offset = column->segmentby_column_index;
		int16 orderby_offset = column->orderby_column_index;
		AttrNumber compressed_att;
		if (segment_offset >= 0)
			(*keys_out)[segment_offset] = column;

		if (columns[i]->orderby_column_index >= 0)
			(*keys_out)[n_segment_keys + orderby_offset] = column;

		compressed_att = get_attnum(in_table, NameStr(column->attname));
		if (!AttributeNumberIsValid(compressed_att))
			elog(ERROR, "could not find compressed column for \"%s\"", NameStr(column->attname));

		column_offsets[i] = AttrNumberGetAttrOffset(compressed_att);
	}

	return column_offsets;
}

static void compress_chunk_populate_sort_info_for_column(Oid table,
														 const ColumnCompressionInfo *column,
														 AttrNumber *att_nums, Oid *sort_operator,
														 Oid *collation, bool *nulls_first);

static Tuplesortstate *
compress_chunk_sort_relation(Relation in_rel, int n_keys, const ColumnCompressionInfo **keys)
{
	TupleDesc tupDesc = RelationGetDescr(in_rel);
	Tuplesortstate *tuplesortstate;
	HeapTuple tuple;
	HeapScanDesc heapScan;
	TupleTableSlot *heap_tuple_slot = MakeTupleTableSlotCompat(tupDesc);
	AttrNumber *sort_keys = palloc(sizeof(*sort_keys) * n_keys);
	Oid *sort_operators = palloc(sizeof(*sort_operators) * n_keys);
	Oid *sort_collations = palloc(sizeof(*sort_collations) * n_keys);
	bool *nulls_first = palloc(sizeof(*nulls_first) * n_keys);
	int n;

	for (n = 0; n < n_keys; n++)
		compress_chunk_populate_sort_info_for_column(RelationGetRelid(in_rel),
													 keys[n],
													 &sort_keys[n],
													 &sort_operators[n],
													 &sort_collations[n],
													 &nulls_first[n]);

	tuplesortstate = tuplesort_begin_heap(tupDesc,
										  n_keys,
										  sort_keys,
										  sort_operators,
										  sort_collations,
										  nulls_first,
										  work_mem,
#if PG11
										  NULL,
#endif
										  false /*=randomAccess*/);

	heapScan = heap_beginscan(in_rel, GetLatestSnapshot(), 0, (ScanKey) NULL);
	for (tuple = heap_getnext(heapScan, ForwardScanDirection); tuple != NULL;
		 tuple = heap_getnext(heapScan, ForwardScanDirection))
	{
		if (HeapTupleIsValid(tuple))
		{
			// TODO is this the most efficient way to do this?
			//     (since we use begin_heap() the tuplestore expects tupleslots,
			//      so ISTM that the options are this or maybe putdatum())
			ExecStoreTuple(tuple, heap_tuple_slot, InvalidBuffer, false);
			tuplesort_puttupleslot(tuplesortstate, heap_tuple_slot);
		}
	}

	heap_endscan(heapScan);

	ExecDropSingleTupleTableSlot(heap_tuple_slot);

	tuplesort_performsort(tuplesortstate);

	return tuplesortstate;
}

static void
compress_chunk_populate_sort_info_for_column(Oid table, const ColumnCompressionInfo *column,
											 AttrNumber *att_nums, Oid *sort_operator,
											 Oid *collation, bool *nulls_first)
{
	HeapTuple tp;
	Form_pg_attribute att_tup;
	TypeCacheEntry *tentry;

	tp = SearchSysCacheAttName(table, NameStr(column->attname));
	if (!HeapTupleIsValid(tp))
		elog(ERROR, "table %d does not have column \"%s\"", table, NameStr(column->attname));

	att_tup = (Form_pg_attribute) GETSTRUCT(tp);
	// TODO other valdation checks?

	*att_nums = att_tup->attnum;
	*collation = att_tup->attcollation;
	*nulls_first = column->segmentby_column_index < 0 && column->orderby_nullsfirst;

	tentry = lookup_type_cache(att_tup->atttypid, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	if (column->segmentby_column_index >= 0 || column->orderby_asc)
		*sort_operator = tentry->lt_opr;
	else
		*sort_operator = tentry->gt_opr;

	if (!OidIsValid(*sort_operator))
		elog(ERROR,
			 "no valid sort operator for column \"%s\" of type \"%s\"",
			 NameStr(column->attname),
			 format_type_be(att_tup->atttypid));

	ReleaseSysCache(tp);
}

/********************
 ** row_compressor **
 ********************/

static void row_compressor_update_group(RowCompressor *row_compressor, TupleTableSlot *row);
static bool row_compressor_new_row_is_in_new_group(RowCompressor *row_compressor,
												   TupleTableSlot *row);
static void row_compressor_append_row(RowCompressor *row_compressor, TupleTableSlot *row);
static void row_compressor_flush(RowCompressor *row_compressor, CommandId mycid);

static SegmentInfo *segment_info_new(Form_pg_attribute column_attr);
static void segment_info_update(SegmentInfo *segment_info, Datum val, bool is_null);
static bool segment_info_datum_is_in_group(SegmentInfo *segment_info, Datum datum, bool is_null);

/* num_compression_infos is the number of columns we will write to in the compressed table */
static void
row_compressor_init(RowCompressor *row_compressor, TupleDesc uncompressed_tuple_desc,
					Relation compressed_table, int num_compression_infos,
					const ColumnCompressionInfo **column_compression_info, int16 *in_column_offsets,
					int16 num_compressed_columns)
{
	TupleDesc out_desc = RelationGetDescr(compressed_table);
	int col;
	*row_compressor = (RowCompressor){
		.compressed_table = compressed_table,
		.bistate = GetBulkInsertState(),
		.n_input_columns = uncompressed_tuple_desc->natts,
		.per_column = palloc0(sizeof(PerColumn) * uncompressed_tuple_desc->natts),
		.uncompressed_col_to_compressed_col =
			palloc0(sizeof(*row_compressor->uncompressed_col_to_compressed_col) *
					uncompressed_tuple_desc->natts),
		.compressed_values = palloc(sizeof(Datum) * num_compressed_columns),
		.compressed_is_null = palloc(sizeof(bool) * num_compressed_columns),
		.rows_compressed_into_current_value = 0,
	};

	memset(row_compressor->compressed_is_null, 1, sizeof(bool) * num_compressed_columns);

	for (col = 0; col < num_compression_infos; col++)
	{
		const ColumnCompressionInfo *compression_info = column_compression_info[col];
		/* we want row_compressor.per_column to be in the same order as the underlying table */
		int16 in_column_offset = in_column_offsets[col];
		PerColumn *column = &row_compressor->per_column[in_column_offset];
		Form_pg_attribute column_attr = TupleDescAttr(uncompressed_tuple_desc, in_column_offset);
		AttrNumber compressed_colnum =
			attno_find_by_attname(out_desc, (Name) &compression_info->attname);
		row_compressor->uncompressed_col_to_compressed_col[in_column_offset] =
			AttrNumberGetAttrOffset(compressed_colnum);
		Assert(AttrNumberGetAttrOffset(compressed_colnum) < num_compressed_columns);

		if (compression_info->segmentby_column_index < 0)
		{
			*column = (PerColumn){
				.compressor = compressor_for_algorithm_and_type(compression_info->algo_id,
																column_attr->atttypid),
			};
		}
		else
		{
			*column = (PerColumn){
				.segment_info = segment_info_new(column_attr),
			};
		}
	}
}

static void
row_compressor_append_sorted_rows(RowCompressor *row_compressor, Tuplesortstate *sorted_rel,
								  TupleDesc sorted_desc)
{
	CommandId mycid = GetCurrentCommandId(true);
	TupleTableSlot *slot = MakeTupleTableSlotCompat(sorted_desc);
	bool got_tuple;
	bool first_iteration = true;
	for (got_tuple = tuplesort_gettupleslot(sorted_rel,
											true /*=forward*/,
											false /*=copy*/,
											slot,
											NULL /*=abbrev*/);
		 got_tuple;
		 got_tuple = tuplesort_gettupleslot(sorted_rel,
											true /*=forward*/,
											false /*=copy*/,
											slot,
											NULL /*=abbrev*/))
	{
		bool changed_groups, compressed_row_is_full;
		slot_getallattrs(slot);

		/* first time through */
		if (first_iteration)
		{
			row_compressor_update_group(row_compressor, slot);
			first_iteration = false;
		}

		changed_groups = row_compressor_new_row_is_in_new_group(row_compressor, slot);
		compressed_row_is_full =
			row_compressor->rows_compressed_into_current_value >= MAX_ROWS_PER_COMPRESSION;
		if (compressed_row_is_full || changed_groups)
		{
			if (row_compressor->rows_compressed_into_current_value > 0)
				row_compressor_flush(row_compressor, mycid);
			if (changed_groups)
				row_compressor_update_group(row_compressor, slot);
		}

		row_compressor_append_row(row_compressor, slot);
		ExecClearTuple(slot);
	}

	if (row_compressor->rows_compressed_into_current_value > 0)
		row_compressor_flush(row_compressor, mycid);

	ExecDropSingleTupleTableSlot(slot);
}

static void
row_compressor_update_group(RowCompressor *row_compressor, TupleTableSlot *row)
{
	int col;

	Assert(row_compressor->rows_compressed_into_current_value == 0);
	Assert(row_compressor->n_input_columns <= row->tts_nvalid);

	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		Datum val;
		bool is_null;

		if (column->segment_info == NULL)
			continue;

		Assert(column->compressor == NULL);

		// TODO we should just use array access here; everything is guaranteed to be fetched
		val = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);
		segment_info_update(column->segment_info, val, is_null);
	}
}

static bool
row_compressor_new_row_is_in_new_group(RowCompressor *row_compressor, TupleTableSlot *row)
{
	int col;
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		Datum datum = CharGetDatum(0);
		bool is_null;

		if (column->segment_info == NULL)
			continue;

		Assert(column->compressor == NULL);

		datum = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);

		if (!segment_info_datum_is_in_group(column->segment_info, datum, is_null))
			return true;
	}

	return false;
}

static void
row_compressor_append_row(RowCompressor *row_compressor, TupleTableSlot *row)
{
	int col;
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		Compressor *compressor = row_compressor->per_column[col].compressor;
		bool is_null;
		Datum val;

		/* if there is no compressor, this must be a segmenter, so just skip */
		if (compressor == NULL)
			continue;

		// TODO since we call getallatts at the beginning, slot_getattr is useless
		//     overhead here, and we should just access the array directly
		val = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);
		if (is_null)
			compressor->append_null(compressor);
		else
			compressor->append_val(compressor, val);
	}

	row_compressor->rows_compressed_into_current_value += 1;
}

static void
row_compressor_flush(RowCompressor *row_compressor, CommandId mycid)
{
	int16 col;
	HeapTuple compressed_tuple;

	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		Compressor *compressor;
		int16 compressed_col;
		if (column->compressor == NULL && column->segment_info == NULL)
			continue;

		compressor = column->compressor;
		compressed_col = row_compressor->uncompressed_col_to_compressed_col[col];

		Assert(compressed_col >= 0);

		if (compressor != NULL)
		{
			void *compressed_data;
			Assert(column->segment_info == NULL);
			Assert(compressed_col < row_compressor->n_input_columns);

			compressed_data = compressor->finish(compressor);

			/* non-segment columns are NULL iff all the values are NULL */
			row_compressor->compressed_is_null[compressed_col] = compressed_data == NULL;
			if (compressed_data != NULL)
				row_compressor->compressed_values[compressed_col] =
					PointerGetDatum(compressed_data);
		}
		else if (column->segment_info != NULL)
		{
			row_compressor->compressed_values[compressed_col] = column->segment_info->val;
			row_compressor->compressed_is_null[compressed_col] = column->segment_info->is_null;
		}
	}

	compressed_tuple = heap_form_tuple(RelationGetDescr(row_compressor->compressed_table),
									   row_compressor->compressed_values,
									   row_compressor->compressed_is_null);
	heap_insert(row_compressor->compressed_table,
				compressed_tuple,
				mycid,
				0 /*=options*/,
				row_compressor->bistate);

	/* free the compressed values now that we're done with them (the old compressor is freed in
	 * finish()) */
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		int16 compressed_col;
		if (column->compressor == NULL && column->segment_info == NULL)
			continue;

		compressed_col = row_compressor->uncompressed_col_to_compressed_col[col];
		Assert(compressed_col >= 0);
		Assert(compressed_col < row_compressor->n_input_columns);
		if (row_compressor->compressed_is_null[compressed_col])
			continue;

		if (column->compressor != NULL || !column->segment_info->typ_by_val)
			pfree(DatumGetPointer(row_compressor->compressed_values[compressed_col]));

		row_compressor->compressed_values[compressed_col] = 0;
		row_compressor->compressed_is_null[compressed_col] = true;
	}

	row_compressor->rows_compressed_into_current_value = 0;
}

static void
row_compressor_finish(RowCompressor *row_compressor)
{
	FreeBulkInsertState(row_compressor->bistate);
}

/******************
 ** segment_info **
 ******************/

static SegmentInfo *
segment_info_new(Form_pg_attribute column_attr)
{
	Oid eq_fn_oid =
		lookup_type_cache(column_attr->atttypid, TYPECACHE_EQ_OPR_FINFO)->eq_opr_finfo.fn_oid;
	SegmentInfo *segment_info = palloc(sizeof(*segment_info));

	*segment_info = (SegmentInfo){
		.typlen = column_attr->attlen,
		.typ_by_val = column_attr->attbyval,
	};

	if (!OidIsValid(eq_fn_oid))
		elog(ERROR, "no equality function for column \"%s\"", NameStr(column_attr->attname));
	fmgr_info_cxt(eq_fn_oid, &segment_info->eq_fn, CurrentMemoryContext);

	InitFunctionCallInfoData(segment_info->eq_fcinfo,
							 &segment_info->eq_fn /*=Flinfo*/,
							 2 /*=Nargs*/,
							 column_attr->attcollation /*=Collation*/,
							 NULL, /*=Context*/
							 NULL  /*=ResultInfo*/
	);

	return segment_info;
}

static void
segment_info_update(SegmentInfo *segment_info, Datum val, bool is_null)
{
	segment_info->is_null = is_null;
	if (is_null)
		segment_info->val = 0;
	else
		segment_info->val = datumCopy(val, segment_info->typ_by_val, segment_info->typlen);
}

static bool
segment_info_datum_is_in_group(SegmentInfo *segment_info, Datum datum, bool is_null)
{
	Datum data_is_eq;
	FunctionCallInfoData *eq_fcinfo;
	/* if one of the datums is null and the other isn't, we must be in a new group */
	if (segment_info->is_null != is_null)
		return false;

	/* they're both null */
	if (segment_info->is_null)
		return true;

	/* neither is null, call the eq function */
	eq_fcinfo = &segment_info->eq_fcinfo;
	eq_fcinfo->arg[0] = segment_info->val;
	eq_fcinfo->argnull[0] = false;

	eq_fcinfo->arg[1] = datum;
	eq_fcinfo->isnull = false;

	data_is_eq = FunctionCallInvoke(eq_fcinfo);

	if (eq_fcinfo->isnull)
		return false;

	return DatumGetBool(data_is_eq);
}

/********************/
/*** SQL Bindings ***/
/********************/

Datum
tsl_compressed_data_decompress_forward(PG_FUNCTION_ARGS)
{
	Datum compressed = PG_ARGISNULL(0) ? ({
		PG_RETURN_NULL();
		0;
	}) :
										 PG_GETARG_DATUM(0);
	CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(compressed);
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	DecompressionIterator *iter;
	DecompressResult res;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		iter =
			definitions[header->compression_algorithm]
				.iterator_init_forward(PG_GETARG_DATUM(0), get_fn_expr_argtype(fcinfo->flinfo, 1));

		funcctx->user_fctx = iter;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	iter = funcctx->user_fctx;
	res = iter->try_next(iter);

	if (res.is_done)
		SRF_RETURN_DONE(funcctx);

	if (res.is_null)
		SRF_RETURN_NEXT_NULL(funcctx);

	SRF_RETURN_NEXT(funcctx, res.val);
}

Datum
tsl_compressed_data_decompress_reverse(PG_FUNCTION_ARGS)
{
	Datum compressed = PG_ARGISNULL(0) ? ({
		PG_RETURN_NULL();
		0;
	}) :
										 PG_GETARG_DATUM(0);
	CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(compressed);
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	DecompressionIterator *iter;
	DecompressResult res;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		iter =
			definitions[header->compression_algorithm]
				.iterator_init_reverse(PG_GETARG_DATUM(0), get_fn_expr_argtype(fcinfo->flinfo, 1));

		funcctx->user_fctx = iter;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	iter = funcctx->user_fctx;
	res = iter->try_next(iter);

	if (res.is_done)
		SRF_RETURN_DONE(funcctx);

	if (res.is_null)
		SRF_RETURN_NEXT_NULL(funcctx);

	SRF_RETURN_NEXT(funcctx, res.val);
	;
}

Datum
tsl_compressed_data_send(PG_FUNCTION_ARGS)
{
	CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(PG_GETARG_DATUM(0));
	StringInfoData buf;
	pq_begintypsend(&buf);
	pq_sendbyte(&buf, header->compression_algorithm);

	definitions[header->compression_algorithm].compressed_data_send(header, &buf);

	PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}

Datum
tsl_compressed_data_recv(PG_FUNCTION_ARGS)
{
	StringInfo buf = (StringInfo) PG_GETARG_POINTER(0);
	CompressedDataHeader header = {};

	header.compression_algorithm = pq_getmsgbyte(buf);

	return definitions[header.compression_algorithm].compressed_data_recv(buf);
}

extern Datum
tsl_compressed_data_in(PG_FUNCTION_ARGS)
{
	const char *input = PG_GETARG_CSTRING(0);
	size_t input_len = strlen(input);
	int decoded_len;
	char *decoded;
	StringInfoData data;
	Datum result;

	if (input_len > PG_INT32_MAX)
		elog(ERROR, "input too long");

	decoded_len = pg_b64_dec_len(input_len);
	decoded = palloc(decoded_len + 1);
	decoded_len = pg_b64_decode(input, input_len, decoded);
	decoded[decoded_len] = '\0';
	data = (StringInfoData){
		.data = decoded,
		.len = decoded_len,
		.maxlen = decoded_len,
	};

	result = DirectFunctionCall1(tsl_compressed_data_recv, PointerGetDatum(&data));

	PG_RETURN_DATUM(result);
}

extern Datum
tsl_compressed_data_out(PG_FUNCTION_ARGS)
{
	Datum bytes_data = DirectFunctionCall1(tsl_compressed_data_send, PG_GETARG_DATUM(0));
	bytea *bytes = DatumGetByteaP(bytes_data);
	int raw_len = VARSIZE_ANY_EXHDR(bytes);
	const char *raw_data = VARDATA(bytes);
	int encoded_len = pg_b64_enc_len(raw_len);
	char *encoded = palloc(encoded_len + 1);
	encoded_len = pg_b64_encode(raw_data, raw_len, encoded);
	encoded[encoded_len] = '\0';

	PG_RETURN_CSTRING(encoded);
}
