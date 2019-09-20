/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "compression/compression.h"

#include <access/heapam.h>
#include <access/htup_details.h>
#include <access/multixact.h>
#include <access/xact.h>
#include <catalog/namespace.h>
#include <catalog/pg_attribute.h>
#include <catalog/pg_type.h>
#include <catalog/index.h>
#include <catalog/heap.h>
#include <funcapi.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <storage/predicate.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
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
#include "create.h"
#include "custom_type_cache.h"
#include "segment_meta.h"

#define MAX_ROWS_PER_COMPRESSION 1000
/* gap in sequence id between rows, potential for adding rows in gap later */
#define SEQUENCE_NUM_GAP 10
#define COMPRESSIONCOL_IS_SEGMENT_BY(col) (col->segmentby_column_index > 0)
#define COMPRESSIONCOL_IS_ORDER_BY(col) (col->orderby_column_index > 0)

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

DecompressionIterator *(*tsl_get_decompression_iterator_init(CompressionAlgorithms algorithm,
															 bool reverse))(Datum, Oid)
{
	if (algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);

	if (reverse)
		return definitions[algorithm].iterator_init_reverse;
	else
		return definitions[algorithm].iterator_init_forward;
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
	/*
	 * Information on the metadata we'll store for this column (currently only min/max).
	 * Only used for order-by columns right now, will be {-1, NULL} for others.
	 */
	int16 min_max_metadata_attr_offset;
	SegmentMetaMinMaxBuilder *min_max_metadata_builder;

	/* segment info; only used if compressor is NULL */
	SegmentInfo *segment_info;
} PerColumn;

typedef struct RowCompressor
{
	/* memory context reset per-row is stored */
	MemoryContext per_row_ctx;

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
	int16 count_metadata_column_offset;
	int16 sequence_num_metadata_column_offset;

	/* the number of uncompressed rows compressed into the current compressed row */
	uint32 rows_compressed_into_current_value;
	/* a unique monotonically increasing (according to order by) id for each compressed row */
	int32 sequence_num;

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

/* Truncate the relation WITHOUT applying triggers. This is the
 * main difference with ExecuteTruncate. Triggers aren't applied
 * because the data remains, just in compressed form. Also don't
 * restart sequences. Use the transactional branch through ExecuteTruncate.
 */
static void
truncate_relation(Oid table_oid)
{
	List *fks = heap_truncate_find_FKs(list_make1_oid(table_oid));
	/* Take an access exclusive lock now. Note that this may very well
	 *  be a lock upgrade. */
	Relation rel = relation_open(table_oid, AccessExclusiveLock);
	MultiXactId minmulti;
	Oid toast_relid;

	/* Chunks should never have fks into them, but double check */
	if (fks != NIL)
		elog(ERROR, "found a FK into a chunk while truncating");

	CheckTableForSerializableConflictIn(rel);

	minmulti = GetOldestMultiXactId();

	RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence, RecentXmin, minmulti);

	toast_relid = rel->rd_rel->reltoastrelid;

	heap_close(rel, NoLock);

	if (OidIsValid(toast_relid))
	{
		rel = relation_open(toast_relid, AccessExclusiveLock);
		RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence, RecentXmin, minmulti);
		Assert(rel->rd_rel->relpersistence != RELPERSISTENCE_UNLOGGED);
		heap_close(rel, NoLock);
	}

	reindex_relation(table_oid, REINDEX_REL_PROCESS_TOAST, 0);
}

void
compress_chunk(Oid in_table, Oid out_table, const ColumnCompressionInfo **column_compression_info,
			   int num_compression_infos)
{
	int n_keys;
	const ColumnCompressionInfo **keys;

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
	// TODO typecheck the output types
	int16 *in_column_offsets = compress_chunk_populate_keys(in_table,
															column_compression_info,
															num_compression_infos,
															&n_keys,
															&keys);

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

	truncate_relation(in_table);

	/* recreate all indexes on out rel, we already have an exvclusive lock on it
	 * so the strong locks taken by reindex_relation shouldn't matter. */
	reindex_relation(out_table, 0, 0);

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
		if (COMPRESSIONCOL_IS_SEGMENT_BY(columns[i]))
			n_segment_keys += 1;

		if (COMPRESSIONCOL_IS_SEGMENT_BY(columns[i]) || COMPRESSIONCOL_IS_ORDER_BY(columns[i]))
			*n_keys_out += 1;
	}

	if (*n_keys_out == 0)
		elog(ERROR, "compression should be configured with an orderby or segment by");

	*keys_out = palloc(sizeof(**keys_out) * *n_keys_out);

	for (i = 0; i < n_columns; i++)
	{
		const ColumnCompressionInfo *column = columns[i];
		/* valid values for segmentby_columnn_index and orderby_column_index
		   are > 0 */
		int16 segment_offset = column->segmentby_column_index - 1;
		int16 orderby_offset = column->orderby_column_index - 1;
		AttrNumber compressed_att;
		if (COMPRESSIONCOL_IS_SEGMENT_BY(column))
			(*keys_out)[segment_offset] = column;
		else if (COMPRESSIONCOL_IS_ORDER_BY(column))
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
	*nulls_first = (!(COMPRESSIONCOL_IS_SEGMENT_BY(column))) && column->orderby_nullsfirst;

	tentry = lookup_type_cache(att_tup->atttypid, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	if (COMPRESSIONCOL_IS_SEGMENT_BY(column) || column->orderby_asc)
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
static void row_compressor_flush(RowCompressor *row_compressor, CommandId mycid,
								 bool changed_groups);

static SegmentInfo *segment_info_new(Form_pg_attribute column_attr);
static void segment_info_update(SegmentInfo *segment_info, Datum val, bool is_null);
static bool segment_info_datum_is_in_group(SegmentInfo *segment_info, Datum datum, bool is_null);

/* num_compression_infos is the number of columns we will write to in the compressed table */
static void
row_compressor_init(RowCompressor *row_compressor, TupleDesc uncompressed_tuple_desc,
					Relation compressed_table, int num_compression_infos,
					const ColumnCompressionInfo **column_compression_info, int16 *in_column_offsets,
					int16 num_columns_in_compressed_table)
{
	TupleDesc out_desc = RelationGetDescr(compressed_table);
	int col;
	Name count_metadata_name = DatumGetName(
		DirectFunctionCall1(namein, CStringGetDatum(COMPRESSION_COLUMN_METADATA_COUNT_NAME)));
	Name sequence_num_metadata_name = DatumGetName(
		DirectFunctionCall1(namein,
							CStringGetDatum(COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME)));
	AttrNumber count_metadata_column_num =
		get_attnum(compressed_table->rd_id, NameStr(*count_metadata_name));
	AttrNumber sequence_num_column_num =
		get_attnum(compressed_table->rd_id, NameStr(*sequence_num_metadata_name));
	Oid compressed_data_type_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;

	if (count_metadata_column_num == InvalidAttrNumber)
		elog(ERROR,
			 "missing metadata column '%s' in compressed table",
			 COMPRESSION_COLUMN_METADATA_COUNT_NAME);

	if (sequence_num_column_num == InvalidAttrNumber)
		elog(ERROR,
			 "missing metadata column '%s' in compressed table",
			 COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME);

	*row_compressor = (RowCompressor){
		.per_row_ctx = AllocSetContextCreate(CurrentMemoryContext,
											 "compress chunk per-row",
											 ALLOCSET_DEFAULT_SIZES),
		.compressed_table = compressed_table,
		.bistate = GetBulkInsertState(),
		.n_input_columns = uncompressed_tuple_desc->natts,
		.per_column = palloc0(sizeof(PerColumn) * uncompressed_tuple_desc->natts),
		.uncompressed_col_to_compressed_col =
			palloc0(sizeof(*row_compressor->uncompressed_col_to_compressed_col) *
					uncompressed_tuple_desc->natts),
		.count_metadata_column_offset = AttrNumberGetAttrOffset(count_metadata_column_num),
		.sequence_num_metadata_column_offset = AttrNumberGetAttrOffset(sequence_num_column_num),
		.compressed_values = palloc(sizeof(Datum) * num_columns_in_compressed_table),
		.compressed_is_null = palloc(sizeof(bool) * num_columns_in_compressed_table),
		.rows_compressed_into_current_value = 0,
		.sequence_num = SEQUENCE_NUM_GAP,
	};

	memset(row_compressor->compressed_is_null, 1, sizeof(bool) * num_columns_in_compressed_table);

	for (col = 0; col < num_compression_infos; col++)
	{
		const ColumnCompressionInfo *compression_info = column_compression_info[col];
		/* we want row_compressor.per_column to be in the same order as the underlying table */
		int16 in_column_offset = in_column_offsets[col];
		PerColumn *column = &row_compressor->per_column[in_column_offset];
		Form_pg_attribute column_attr = TupleDescAttr(uncompressed_tuple_desc, in_column_offset);
		AttrNumber compressed_colnum =
			get_attnum(compressed_table->rd_id, NameStr(compression_info->attname));
		Form_pg_attribute compressed_column_attr =
			TupleDescAttr(out_desc, AttrNumberGetAttrOffset(compressed_colnum));
		row_compressor->uncompressed_col_to_compressed_col[in_column_offset] =
			AttrNumberGetAttrOffset(compressed_colnum);
		Assert(AttrNumberGetAttrOffset(compressed_colnum) < num_columns_in_compressed_table);
		if (!COMPRESSIONCOL_IS_SEGMENT_BY(compression_info))
		{
			int16 segment_min_max_attr_offset = -1;
			SegmentMetaMinMaxBuilder *segment_min_max_builder = NULL;
			if (compressed_column_attr->atttypid != compressed_data_type_oid)
				elog(ERROR,
					 "expected column '%s' to be a compressed data type",
					 compression_info->attname.data);

			if (compression_info->orderby_column_index > 0)
			{
				char *segment_col_name = compression_column_segment_min_max_name(compression_info);
				AttrNumber segment_min_max_attr_number =
					get_attnum(compressed_table->rd_id, segment_col_name);
				if (segment_min_max_attr_number == InvalidAttrNumber)
					elog(ERROR, "couldn't find metadata column %s", segment_col_name);
				segment_min_max_attr_offset = AttrNumberGetAttrOffset(segment_min_max_attr_number);
				segment_min_max_builder =
					segment_meta_min_max_builder_create(column_attr->atttypid,
														column_attr->attcollation);
			}
			*column = (PerColumn){
				.compressor = compressor_for_algorithm_and_type(compression_info->algo_id,
																column_attr->atttypid),
				.min_max_metadata_attr_offset = segment_min_max_attr_offset,
				.min_max_metadata_builder = segment_min_max_builder,
			};
		}
		else
		{
			if (column_attr->atttypid != compressed_column_attr->atttypid)
				elog(ERROR,
					 "expected segment by column '%s' to be same type as uncompressed column",
					 compression_info->attname.data);
			*column = (PerColumn){
				.segment_info = segment_info_new(column_attr),
				.min_max_metadata_attr_offset = -1,
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
#if !PG96
											false /*=copy*/,
#endif
											slot,
											NULL /*=abbrev*/);
		 got_tuple;
		 got_tuple = tuplesort_gettupleslot(sorted_rel,
											true /*=forward*/,
#if !PG96
											false /*=copy*/,
#endif
											slot,
											NULL /*=abbrev*/))
	{
		bool changed_groups, compressed_row_is_full;
		MemoryContext old_ctx;
		slot_getallattrs(slot);
		old_ctx = MemoryContextSwitchTo(row_compressor->per_row_ctx);

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
				row_compressor_flush(row_compressor, mycid, changed_groups);
			if (changed_groups)
				row_compressor_update_group(row_compressor, slot);
		}

		row_compressor_append_row(row_compressor, slot);
		MemoryContextSwitchTo(old_ctx);
		ExecClearTuple(slot);
	}

	if (row_compressor->rows_compressed_into_current_value > 0)
		row_compressor_flush(row_compressor, mycid, true);

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

		MemoryContextSwitchTo(row_compressor->per_row_ctx->parent);
		// TODO we should just use array access here; everything is guaranteed to be fetched
		val = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);
		segment_info_update(column->segment_info, val, is_null);
		MemoryContextSwitchTo(row_compressor->per_row_ctx);
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
		{
			compressor->append_null(compressor);
			if (row_compressor->per_column[col].min_max_metadata_builder != NULL)
				segment_meta_min_max_builder_update_null(
					row_compressor->per_column[col].min_max_metadata_builder);
		}
		else
		{
			compressor->append_val(compressor, val);
			if (row_compressor->per_column[col].min_max_metadata_builder != NULL)
				segment_meta_min_max_builder_update_val(row_compressor->per_column[col]
															.min_max_metadata_builder,
														val);
		}
	}

	row_compressor->rows_compressed_into_current_value += 1;
}

static void
row_compressor_flush(RowCompressor *row_compressor, CommandId mycid, bool changed_groups)
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

			compressed_data = compressor->finish(compressor);

			/* non-segment columns are NULL iff all the values are NULL */
			row_compressor->compressed_is_null[compressed_col] = compressed_data == NULL;
			if (compressed_data != NULL)
				row_compressor->compressed_values[compressed_col] =
					PointerGetDatum(compressed_data);

			if (column->min_max_metadata_builder != NULL)
			{
				SegmentMetaMinMax *segment_meta_min_max =
					segment_meta_min_max_builder_finish_and_reset(column->min_max_metadata_builder);

				Assert(column->min_max_metadata_attr_offset >= 0);

				/* both the data and metadata are only NULL if all the data is NULL, thus: either
				 * both the data and the metadata are both null or neither are */
				Assert((compressed_data == NULL && segment_meta_min_max == NULL) ||
					   (compressed_data != NULL && segment_meta_min_max != NULL));

				row_compressor->compressed_is_null[column->min_max_metadata_attr_offset] =
					segment_meta_min_max == NULL;
				if (segment_meta_min_max != NULL)
					row_compressor->compressed_values[column->min_max_metadata_attr_offset] =
						PointerGetDatum(segment_meta_min_max);
			}
		}
		else if (column->segment_info != NULL)
		{
			row_compressor->compressed_values[compressed_col] = column->segment_info->val;
			row_compressor->compressed_is_null[compressed_col] = column->segment_info->is_null;
		}
	}

	row_compressor->compressed_values[row_compressor->count_metadata_column_offset] =
		Int32GetDatum(row_compressor->rows_compressed_into_current_value);
	row_compressor->compressed_is_null[row_compressor->count_metadata_column_offset] = false;

	row_compressor->compressed_values[row_compressor->sequence_num_metadata_column_offset] =
		Int32GetDatum(row_compressor->sequence_num);
	row_compressor->compressed_is_null[row_compressor->sequence_num_metadata_column_offset] = false;

	/* overflow could happen only if chunk has more than 200B rows */
	if (row_compressor->sequence_num > PG_INT32_MAX - SEQUENCE_NUM_GAP)
		elog(ERROR, "sequence id overflow");

	row_compressor->sequence_num += SEQUENCE_NUM_GAP;

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
		if (row_compressor->compressed_is_null[compressed_col])
			continue;

		/* don't free the segment-bys if we've overflowed the row, we still need them */
		if (column->segment_info != NULL && !changed_groups)
			continue;

		if (column->compressor != NULL || !column->segment_info->typ_by_val)
			pfree(DatumGetPointer(row_compressor->compressed_values[compressed_col]));

		if (column->min_max_metadata_builder != NULL &&
			row_compressor->compressed_is_null[column->min_max_metadata_attr_offset])
		{
			pfree(DatumGetPointer(
				row_compressor->compressed_values[column->min_max_metadata_attr_offset]));
			row_compressor->compressed_values[column->min_max_metadata_attr_offset] = 0;
			row_compressor->compressed_is_null[column->min_max_metadata_attr_offset] = true;
		}

		row_compressor->compressed_values[compressed_col] = 0;
		row_compressor->compressed_is_null[compressed_col] = true;
	}

	row_compressor->rows_compressed_into_current_value = 0;
	MemoryContextReset(row_compressor->per_row_ctx);
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

/**********************
 ** decompress_chunk **
 **********************/

typedef struct PerCompressedColumn
{
	Oid decompressed_type;

	/* the compressor to use for compressed columns, always NULL for segmenters
	 * only use if is_compressed
	 */
	DecompressionIterator *iterator;

	/* segment info; only used if !is_compressed */
	Datum val;

	/* is this a compressed column or a segment-by column */
	bool is_compressed;

	/* the value stored in the compressed table was NULL */
	bool is_null;

	/* the index in the decompressed table of the data -1,
	 * if the data is metadata not found in the decompressed table
	 */
	int16 decompressed_column_offset;
} PerCompressedColumn;

typedef struct RowDecompressor
{
	PerCompressedColumn *per_compressed_cols;
	int16 num_compressed_columns;

	TupleDesc out_desc;
	Relation out_rel;

	CommandId mycid;
	BulkInsertState bistate;

	/* cache memory used to store the decompressed datums/is_null for form_tuple */
	Datum *decompressed_datums;
	bool *decompressed_is_nulls;
} RowDecompressor;

static PerCompressedColumn *create_per_compressed_column(TupleDesc in_desc, TupleDesc out_desc,
														 Oid out_relid,
														 Oid compressed_data_type_oid);
static void populate_per_compressed_columns_from_data(PerCompressedColumn *per_compressed_cols,
													  int16 num_cols, Datum *compressed_datums,
													  bool *compressed_is_nulls);
static void row_decompressor_decompress_row(RowDecompressor *row_decompressor);
static bool per_compressed_col_get_data(PerCompressedColumn *per_compressed_col,
										Datum *decompressed_datums, bool *decompressed_is_nulls);

void
decompress_chunk(Oid in_table, Oid out_table)
{
	/* these locks are taken in the order uncompressed table then compressed table
	 * for consistency with compress_chunk
	 */
	/* we are _just_ INSERTing into the out_table so in principle we could take
	 * a RowExclusive lock, and let other operations read and write this table
	 * as we work. However, we currently compress each table as a oneshot, so
	 * we're taking the stricter lock to prevent accidents.
	 */
	Relation out_rel = relation_open(out_table, ExclusiveLock);
	/*We want to prevent other decompressors from decompressing this table,
	 * and we want to prevent INSERTs or UPDATEs which could mess up our decompression.
	 * We may as well allow readers to keep reading the compressed data while
	 * we are compressing, so we only take an ExclusiveLock instead of AccessExclusive.
	 */
	Relation in_rel = relation_open(in_table, ExclusiveLock);
	// TODO error if out_rel is non-empty

	TupleDesc in_desc = RelationGetDescr(in_rel);
	TupleDesc out_desc = RelationGetDescr(out_rel);

	Oid compressed_data_type_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;

	Assert(in_desc->natts >= out_desc->natts);
	Assert(OidIsValid(compressed_data_type_oid));

	{
		RowDecompressor decompressor = {
			.per_compressed_cols = create_per_compressed_column(in_desc,
																out_desc,
																out_table,
																compressed_data_type_oid),
			.num_compressed_columns = in_desc->natts,

			.out_desc = out_desc,
			.out_rel = out_rel,

			.mycid = GetCurrentCommandId(true),
			.bistate = GetBulkInsertState(),

			/* cache memory used to store the decompressed datums/is_null for form_tuple */
			.decompressed_datums = palloc(sizeof(Datum) * out_desc->natts),
			.decompressed_is_nulls = palloc(sizeof(bool) * out_desc->natts),
		};
		Datum *compressed_datums = palloc(sizeof(*compressed_datums) * in_desc->natts);
		bool *compressed_is_nulls = palloc(sizeof(*compressed_is_nulls) * in_desc->natts);

		HeapTuple compressed_tuple;
		HeapScanDesc heapScan = heap_beginscan(in_rel, GetLatestSnapshot(), 0, (ScanKey) NULL);
		MemoryContext per_compressed_row_ctx =
			AllocSetContextCreate(CurrentMemoryContext,
								  "decompress chunk per-compressed row",
								  ALLOCSET_DEFAULT_SIZES);

		for (compressed_tuple = heap_getnext(heapScan, ForwardScanDirection);
			 compressed_tuple != NULL;
			 compressed_tuple = heap_getnext(heapScan, ForwardScanDirection))
		{
			MemoryContext old_ctx;

			if (!HeapTupleIsValid(compressed_tuple))
				continue;

			old_ctx = MemoryContextSwitchTo(per_compressed_row_ctx);

			heap_deform_tuple(compressed_tuple, in_desc, compressed_datums, compressed_is_nulls);
			populate_per_compressed_columns_from_data(decompressor.per_compressed_cols,
													  in_desc->natts,
													  compressed_datums,
													  compressed_is_nulls);

			row_decompressor_decompress_row(&decompressor);
			MemoryContextSwitchTo(old_ctx);
		}

		heap_endscan(heapScan);
		FreeBulkInsertState(decompressor.bistate);
	}

	/* recreate all indexes on out rel, we already have an exvclusive lock on it
	 * so the strong locks taken by reindex_relation shouldn't matter. */
	reindex_relation(out_table, 0, 0);

	RelationClose(out_rel);
	RelationClose(in_rel);
}

static PerCompressedColumn *
create_per_compressed_column(TupleDesc in_desc, TupleDesc out_desc, Oid out_relid,
							 Oid compressed_data_type_oid)
{
	PerCompressedColumn *per_compressed_cols =
		palloc(sizeof(*per_compressed_cols) * in_desc->natts);

	Assert(OidIsValid(compressed_data_type_oid));

	for (int16 col = 0; col < in_desc->natts; col++)
	{
		Oid decompressed_type;
		bool is_compressed;
		int16 decompressed_column_offset;
		PerCompressedColumn *per_compressed_col = &per_compressed_cols[col];
		Form_pg_attribute compressed_attr = TupleDescAttr(in_desc, col);
		char *col_name = NameStr(compressed_attr->attname);

		/* find the mapping from compressed column to uncompressed column, setting
		 * the index of columns that don't have an uncompressed version
		 * (such as metadata) to -1
		 */
		AttrNumber decompressed_colnum = get_attnum(out_relid, col_name);
		if (!AttributeNumberIsValid(decompressed_colnum))
		{
			*per_compressed_col = (PerCompressedColumn){
				.decompressed_column_offset = -1,
				.is_null = true,
			};
			continue;
		}

		decompressed_column_offset = AttrNumberGetAttrOffset(decompressed_colnum);

		decompressed_type = TupleDescAttr(out_desc, decompressed_column_offset)->atttypid;

		/* determine if the data is compressed or not */
		is_compressed = compressed_attr->atttypid == compressed_data_type_oid;
		if (!is_compressed && compressed_attr->atttypid != decompressed_type)
			elog(ERROR,
				 "compressed table type '%s' does not match decompressed table type '%s' for "
				 "segment-by column \"%s\"",
				 format_type_be(compressed_attr->atttypid),
				 format_type_be(decompressed_type),
				 col_name);

		*per_compressed_col = (PerCompressedColumn){
			.decompressed_column_offset = decompressed_column_offset,
			.is_null = true,
			.is_compressed = is_compressed,
			.decompressed_type = decompressed_type,
		};
	}

	return per_compressed_cols;
}

static void
populate_per_compressed_columns_from_data(PerCompressedColumn *per_compressed_cols, int16 num_cols,
										  Datum *compressed_datums, bool *compressed_is_nulls)
{
	for (int16 col = 0; col < num_cols; col++)
	{
		PerCompressedColumn *per_col = &per_compressed_cols[col];
		if (per_col->decompressed_column_offset < 0)
			continue;

		per_col->is_null = compressed_is_nulls[col];
		if (per_col->is_null)
		{
			per_col->is_null = true;
			per_col->iterator = NULL;
			per_col->val = 0;
			continue;
		}

		if (per_col->is_compressed)
		{
			char *data = (char *) PG_DETOAST_DATUM(compressed_datums[col]);
			CompressedDataHeader *header = (CompressedDataHeader *) data;

			per_col->iterator =
				definitions[header->compression_algorithm]
					.iterator_init_forward(PointerGetDatum(data), per_col->decompressed_type);
		}
		else
			per_col->val = compressed_datums[col];
	}
}

static void
row_decompressor_decompress_row(RowDecompressor *row_decompressor)
{
	/* each compressed row decompresses to at least one row,
	 * even if all the data is NULL
	 */
	bool wrote_data = false;
	bool is_done = false;
	do
	{
		/* we're done if all the decompressors return NULL */
		is_done = true;
		for (int16 col = 0; col < row_decompressor->num_compressed_columns; col++)
		{
			bool col_is_done =
				per_compressed_col_get_data(&row_decompressor->per_compressed_cols[col],
											row_decompressor->decompressed_datums,
											row_decompressor->decompressed_is_nulls);
			is_done &= col_is_done;
		}

		/* if we're not done we have data to write. even if we're done, each
		 * compressed should decompress to at least one row, so we should write that
		 */
		if (!is_done || !wrote_data)
		{
			// FIXME getting invalid bool here
			HeapTuple decompressed_tuple = heap_form_tuple(row_decompressor->out_desc,
														   row_decompressor->decompressed_datums,
														   row_decompressor->decompressed_is_nulls);
			heap_insert(row_decompressor->out_rel,
						decompressed_tuple,
						row_decompressor->mycid,
						0 /*=options*/,
						row_decompressor->bistate);

			heap_freetuple(decompressed_tuple);
			wrote_data = true;
		}
	} while (!is_done);
}

/* populate the relevent index in an array from a per_compressed_col.
 * returns if decompression is done for this column
 */
bool
per_compressed_col_get_data(PerCompressedColumn *per_compressed_col, Datum *decompressed_datums,
							bool *decompressed_is_nulls)
{
	DecompressResult decompressed;
	int16 decompressed_column_offset = per_compressed_col->decompressed_column_offset;

	/* skip metadata columns */
	if (decompressed_column_offset < 0)
		return true;

	/* segment-bys */
	if (!per_compressed_col->is_compressed)
	{
		decompressed_datums[decompressed_column_offset] = per_compressed_col->val;
		decompressed_is_nulls[decompressed_column_offset] = per_compressed_col->is_null;
		return true;
	}

	/* compressed NULL */
	if (per_compressed_col->is_null)
	{
		decompressed_is_nulls[decompressed_column_offset] = true;
		return true;
	}

	/* other compressed data */
	if (per_compressed_col->iterator == NULL)
		elog(ERROR, "tried to decompress more data than was compressed in column");

	decompressed = per_compressed_col->iterator->try_next(per_compressed_col->iterator);
	if (decompressed.is_done)
	{
		// TODO we want a way to free the decompression iterator's data
		per_compressed_col->iterator = NULL;
		decompressed_is_nulls[decompressed_column_offset] = true;
		return true;
	}

	decompressed_is_nulls[decompressed_column_offset] = decompressed.is_null;
	if (decompressed.is_null)
		decompressed_datums[decompressed_column_offset] = 0;
	else
		decompressed_datums[decompressed_column_offset] = decompressed.val;

	return false;
}

/********************/
/*** SQL Bindings ***/
/********************/

Datum
tsl_compressed_data_decompress_forward(PG_FUNCTION_ARGS)
{
	Datum compressed;
	CompressedDataHeader *header;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	DecompressionIterator *iter;
	DecompressResult res;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	compressed = PG_GETARG_DATUM(0);
	header = (CompressedDataHeader *) PG_DETOAST_DATUM(compressed);

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
	Datum compressed;
	CompressedDataHeader *header;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	DecompressionIterator *iter;
	DecompressResult res;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	compressed = PG_GETARG_DATUM(0);
	header = (CompressedDataHeader *) PG_DETOAST_DATUM(compressed);
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
	CompressedDataHeader header = { { 0 } };

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

extern CompressionStorage
compression_get_toast_storage(CompressionAlgorithms algorithm)
{
	if (algorithm == _INVALID_COMPRESSION_ALGORITHM || algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", algorithm);
	return definitions[algorithm].compressed_data_storage;
}
