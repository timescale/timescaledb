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
#include <catalog/heap.h>
#include <catalog/index.h>
#include <catalog/namespace.h>
#include <catalog/pg_am.h>
#include <catalog/pg_attribute.h>
#include <catalog/pg_type.h>
#include <common/base64.h>
#include <executor/tuptable.h>
#include <funcapi.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <nodes/pg_list.h>
#include <nodes/print.h>
#include <storage/lmgr.h>
#include <storage/predicate.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/tuplesort.h>
#include <utils/typcache.h>

#include "compat/compat.h"

#include "array.h"
#include "chunk.h"
#include "create.h"
#include "custom_type_cache.h"
#include "debug_point.h"
#include "deltadelta.h"
#include "dictionary.h"
#include "gorilla.h"
#include "guc.h"
#include "nodes/chunk_dispatch/chunk_insert_state.h"
#include "indexing.h"
#include "segment_meta.h"
#include "ts_catalog/compression_chunk_size.h"
#include "ts_catalog/hypertable_compression.h"

#define MAX_ROWS_PER_COMPRESSION 1000
/* gap in sequence id between rows, potential for adding rows in gap later */
#define SEQUENCE_NUM_GAP 10
#define COMPRESSIONCOL_IS_SEGMENT_BY(col) ((col)->segmentby_column_index > 0)
#define COMPRESSIONCOL_IS_ORDER_BY(col) ((col)->orderby_column_index > 0)

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
	FunctionCallInfo eq_fcinfo;
	int16 typlen;
	bool is_null;
	bool typ_by_val;
	Oid collation;
} SegmentInfo;

typedef struct PerColumn
{
	/* the compressor to use for regular columns, NULL for segmenters */
	Compressor *compressor;
	/*
	 * Information on the metadata we'll store for this column (currently only min/max).
	 * Only used for order-by columns right now, will be {-1, NULL} for others.
	 */
	int16 min_metadata_attr_offset;
	int16 max_metadata_attr_offset;
	SegmentMetaMinMaxBuilder *min_max_metadata_builder;

	/* segment info; only used if compressor is NULL */
	SegmentInfo *segment_info;
	int16 segmentby_column_index;
} PerColumn;

typedef struct RowCompressor
{
	/* memory context reset per-row is stored */
	MemoryContext per_row_ctx;

	/* the table we're writing the compressed data to */
	Relation compressed_table;
	BulkInsertState bistate;
	/* segment by index Oid if any */
	Oid index_oid;

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
	int64 rowcnt_pre_compression;
	int64 num_compressed_rows;
} RowCompressor;

static int16 *compress_chunk_populate_keys(Oid in_table, const ColumnCompressionInfo **columns,
										   int n_columns, int *n_keys_out,
										   const ColumnCompressionInfo ***keys_out);
static Tuplesortstate *compress_chunk_sort_relation(Relation in_rel, int n_keys,
													const ColumnCompressionInfo **keys);
static void row_compressor_init(RowCompressor *row_compressor, TupleDesc uncompressed_tuple_desc,
								Relation compressed_table, int num_compression_infos,
								const ColumnCompressionInfo **column_compression_info,
								int16 *column_offsets, int16 num_columns_in_compressed_table,
								bool need_bistate);
static void row_compressor_append_sorted_rows(RowCompressor *row_compressor,
											  Tuplesortstate *sorted_rel, TupleDesc sorted_desc);
static void row_compressor_finish(RowCompressor *row_compressor);
static void row_compressor_update_group(RowCompressor *row_compressor, TupleTableSlot *row);
static bool row_compressor_new_row_is_in_new_group(RowCompressor *row_compressor,
												   TupleTableSlot *row);
static void row_compressor_append_row(RowCompressor *row_compressor, TupleTableSlot *row);
static void row_compressor_flush(RowCompressor *row_compressor, CommandId mycid,
								 bool changed_groups);

static SegmentInfo *segment_info_new(Form_pg_attribute column_attr);
static void segment_info_update(SegmentInfo *segment_info, Datum val, bool is_null);
static bool segment_info_datum_is_in_group(SegmentInfo *segment_info, Datum datum, bool is_null);
static void run_analyze_on_chunk(Oid chunk_relid);

/********************
 ** compress_chunk **
 ********************/

static CompressedDataHeader *
get_compressed_data_header(Datum data)
{
	CompressedDataHeader *header = (CompressedDataHeader *) PG_DETOAST_DATUM(data);

	if (header->compression_algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", header->compression_algorithm);

	return header;
}

static void
capture_pgclass_stats(Oid table_oid, int *out_pages, int *out_visible, float *out_tuples)
{
	Relation pg_class = table_open(RelationRelationId, RowExclusiveLock);
	HeapTuple tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(table_oid));
	Form_pg_class classform;

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for relation %u", table_oid);

	classform = (Form_pg_class) GETSTRUCT(tuple);

	*out_pages = classform->relpages;
	*out_visible = classform->relallvisible;
	*out_tuples = classform->reltuples;

	heap_freetuple(tuple);
	table_close(pg_class, RowExclusiveLock);
}

static void
restore_pgclass_stats(Oid table_oid, int pages, int visible, float tuples)
{
	Relation pg_class;
	HeapTuple tuple;
	Form_pg_class classform;

	pg_class = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(table_oid));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for relation %u", table_oid);
	classform = (Form_pg_class) GETSTRUCT(tuple);

	classform->relpages = pages;
	classform->relallvisible = visible;
	classform->reltuples = tuples;

	CatalogTupleUpdate(pg_class, &tuple->t_self, tuple);

	heap_freetuple(tuple);
	table_close(pg_class, RowExclusiveLock);
}

/* Merge the relstats when merging chunks while compressing them.
 * We need to do this in order to update the relstats of the chunk
 * that is merged into since the compressed one will be dropped by
 * the merge.
 */
extern void
merge_chunk_relstats(Oid merged_relid, Oid compressed_relid)
{
	int comp_pages, merged_pages, comp_visible, merged_visible;
	float comp_tuples, merged_tuples;

	capture_pgclass_stats(compressed_relid, &comp_pages, &comp_visible, &comp_tuples);
	capture_pgclass_stats(merged_relid, &merged_pages, &merged_visible, &merged_tuples);

	merged_pages += comp_pages;
	merged_visible += comp_visible;
	merged_tuples += comp_tuples;

	restore_pgclass_stats(merged_relid, merged_pages, merged_visible, merged_tuples);
}

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
	Relation rel = table_open(table_oid, AccessExclusiveLock);
	Oid toast_relid;
	int pages, visible;
	float tuples;

	/* Chunks should never have fks into them, but double check */
	if (fks != NIL)
		elog(ERROR, "found a FK into a chunk while truncating");

	CheckTableForSerializableConflictIn(rel);

	capture_pgclass_stats(table_oid, &pages, &visible, &tuples);
	RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);

	toast_relid = rel->rd_rel->reltoastrelid;

	table_close(rel, NoLock);

	if (OidIsValid(toast_relid))
	{
		rel = table_open(toast_relid, AccessExclusiveLock);
		RelationSetNewRelfilenode(rel, rel->rd_rel->relpersistence);
		Assert(rel->rd_rel->relpersistence != RELPERSISTENCE_UNLOGGED);
		table_close(rel, NoLock);
	}

#if PG14_LT
	int options = 0;
#else
	ReindexParams params = { 0 };
	ReindexParams *options = &params;
#endif
	reindex_relation(table_oid, REINDEX_REL_PROCESS_TOAST, options);
	rel = table_open(table_oid, AccessExclusiveLock);
	restore_pgclass_stats(table_oid, pages, visible, tuples);
	CommandCounterIncrement();
	table_close(rel, NoLock);
}

CompressionStats
compress_chunk(Oid in_table, Oid out_table, const ColumnCompressionInfo **column_compression_info,
			   int num_compression_infos)
{
	int n_keys;
	ListCell *lc;
	int indexscan_direction = NoMovementScanDirection;
	List *in_rel_index_oids;
	Relation matched_index_rel = NULL;
	TupleTableSlot *slot;
	IndexScanDesc index_scan;
	bool first_iteration = true;
	bool changed_groups, compressed_row_is_full;
	MemoryContext old_ctx;
	CommandId mycid = GetCurrentCommandId(true);
	HeapTuple in_table_tp = NULL, index_tp = NULL;
	Form_pg_attribute in_table_attr_tp, index_attr_tp;
	const ColumnCompressionInfo **keys;
	CompressionStats cstat;

	/* We want to prevent other compressors from compressing this table,
	 * and we want to prevent INSERTs or UPDATEs which could mess up our compression.
	 * We may as well allow readers to keep reading the uncompressed data while
	 * we are compressing, so we only take an ExclusiveLock instead of AccessExclusive.
	 */
	Relation in_rel = table_open(in_table, ExclusiveLock);
	/* We are _just_ INSERTing into the out_table so in principle we could take
	 * a RowExclusive lock, and let other operations read and write this table
	 * as we work. However, we currently compress each table as a oneshot, so
	 * we're taking the stricter lock to prevent accidents.
	 */
	Relation out_rel = relation_open(out_table, ExclusiveLock);
	int16 *in_column_offsets = compress_chunk_populate_keys(in_table,
															column_compression_info,
															num_compression_infos,
															&n_keys,
															&keys);

	TupleDesc in_desc = RelationGetDescr(in_rel);
	TupleDesc out_desc = RelationGetDescr(out_rel);
	in_rel_index_oids = RelationGetIndexList(in_rel);
	int i = 0;
	/* Before calling row compressor relation should be segmented and sorted as per
	 * compress_segmentby and compress_orderby column/s configured in ColumnCompressionInfo.
	 * Cost of sorting can be mitigated if we find an existing BTREE index defined for
	 * uncompressed chunk otherwise expensive tuplesort will come into play.
	 *
	 * The following code is trying to find an existing index that
	 * matches the ColumnCompressionInfo so that we can skip sequential scan and
	 * tuplesort.
	 *
	 * Matching Criteria for Each IndexAtt[i] and ColumnCompressionInfo Keys[i]
	 * ========================================================================
	 * a) Index attnum must match with ColumnCompressionInfo Key {keys[i]}.
	 * b) Index attOption(ASC/DESC and NULL_FIRST) can be mapped with ColumnCompressionInfo
	 * orderby_asc and null_first.
	 *
	 * BTREE Indexes Ordering
	 * =====================
	 * a) ASC[Null_Last] ==> [1]->[2]->NULL
	 * b) [Null_First]ASC ==> NULL->[1]->[2]
	 * c) DSC[Null_Last]  ==> [2]->[1]->NULL
	 * d) [Null_First]DSC ==> NULL->[2]->[1]
	 */
	if (ts_guc_enable_compression_indexscan)
	{
		foreach (lc, in_rel_index_oids)
		{
			Oid index_oid = lfirst_oid(lc);
			Relation index_rel = index_open(index_oid, AccessShareLock);
			IndexInfo *index_info = BuildIndexInfo(index_rel);
			int previous_direction = NoMovementScanDirection;
			int current_direction = NoMovementScanDirection;

			if (n_keys <= index_info->ii_NumIndexKeyAttrs && index_info->ii_Am == BTREE_AM_OID)
			{
				for (i = 0; i < n_keys; i++)
				{
					int16 att_num = get_attnum(in_table, NameStr(keys[i]->attname));

					int16 option = index_rel->rd_indoption[i];
					bool index_orderby_asc = ((option & INDOPTION_DESC) == 0);
					bool index_null_first = ((option & INDOPTION_NULLS_FIRST) != 0);
					bool is_orderby_asc =
						COMPRESSIONCOL_IS_SEGMENT_BY(keys[i]) ? true : keys[i]->orderby_asc;
					bool is_null_first =
						COMPRESSIONCOL_IS_SEGMENT_BY(keys[i]) ? false : keys[i]->orderby_nullsfirst;

					if (att_num == 0 || index_info->ii_IndexAttrNumbers[i] != att_num)
					{
						break;
					}

					in_table_tp = SearchSysCacheAttNum(in_table, att_num);
					if (!HeapTupleIsValid(in_table_tp))
						elog(ERROR,
							 "table \"%s\" does not have column \"%s\"",
							 get_rel_name(in_table),
							 NameStr(keys[i]->attname));

					index_tp = SearchSysCacheAttNum(index_oid, i + 1);
					if (!HeapTupleIsValid(index_tp))
						elog(ERROR,
							 "index \"%s\" does not have column \"%s\"",
							 get_rel_name(index_oid),
							 NameStr(keys[i]->attname));

					in_table_attr_tp = (Form_pg_attribute) GETSTRUCT(in_table_tp);
					index_attr_tp = (Form_pg_attribute) GETSTRUCT(index_tp);

					if (index_orderby_asc == is_orderby_asc && index_null_first == is_null_first &&
						in_table_attr_tp->attcollation == index_attr_tp->attcollation)
					{
						current_direction = ForwardScanDirection;
					}
					else if (index_orderby_asc != is_orderby_asc &&
							 index_null_first != is_null_first &&
							 in_table_attr_tp->attcollation == index_attr_tp->attcollation)
					{
						current_direction = BackwardScanDirection;
					}
					else
					{
						current_direction = NoMovementScanDirection;
						break;
					}

					ReleaseSysCache(in_table_tp);
					in_table_tp = NULL;
					ReleaseSysCache(index_tp);
					index_tp = NULL;
					if (previous_direction == NoMovementScanDirection)
					{
						previous_direction = current_direction;
					}
					else if (previous_direction != current_direction)
					{
						break;
					}
				}

				if (n_keys == i && (previous_direction == current_direction &&
									current_direction != NoMovementScanDirection))
				{
					matched_index_rel = index_rel;
					indexscan_direction = current_direction;
					break;
				}
				else
				{
					if (HeapTupleIsValid(in_table_tp))
					{
						ReleaseSysCache(in_table_tp);
						in_table_tp = NULL;
					}
					if (HeapTupleIsValid(index_tp))
					{
						ReleaseSysCache(index_tp);
						index_tp = NULL;
					}
					index_close(index_rel, AccessShareLock);
				}
			}
			else
			{
				index_close(index_rel, AccessShareLock);
			}
		}
	}

	Assert(num_compression_infos <= in_desc->natts);
	Assert(num_compression_infos <= out_desc->natts);
	RowCompressor row_compressor;
	row_compressor_init(&row_compressor,
						in_desc,
						out_rel,
						num_compression_infos,
						column_compression_info,
						in_column_offsets,
						out_desc->natts,
						true /*need_bistate*/);

	if (matched_index_rel != NULL)
	{
#ifdef TS_DEBUG
		const char *compression_path =
			GetConfigOption("timescaledb.show_compression_path_info", true, false);
		if (compression_path != NULL && strcmp(compression_path, "on") == 0)
			elog(INFO,
				 "compress_chunk_indexscan_start matched index \"%s\"",
				 get_rel_name(matched_index_rel->rd_id));
#endif
		index_scan = index_beginscan(in_rel, matched_index_rel, GetTransactionSnapshot(), 0, 0);
		slot = table_slot_create(in_rel, NULL);
		index_rescan(index_scan, NULL, 0, NULL, 0);
		while (index_getnext_slot(index_scan, indexscan_direction, slot))
		{
			slot_getallattrs(slot);
			old_ctx = MemoryContextSwitchTo(row_compressor.per_row_ctx);
			/* first time through */
			if (first_iteration)
			{
				row_compressor_update_group(&row_compressor, slot);
				first_iteration = false;
			}
			changed_groups = row_compressor_new_row_is_in_new_group(&row_compressor, slot);
			compressed_row_is_full =
				row_compressor.rows_compressed_into_current_value >= MAX_ROWS_PER_COMPRESSION;
			if (compressed_row_is_full || changed_groups)
			{
				if (row_compressor.rows_compressed_into_current_value > 0)
					row_compressor_flush(&row_compressor, mycid, changed_groups);
				if (changed_groups)
					row_compressor_update_group(&row_compressor, slot);
			}

			row_compressor_append_row(&row_compressor, slot);
			MemoryContextSwitchTo(old_ctx);
			ExecClearTuple(slot);
		}

		run_analyze_on_chunk(in_rel->rd_id);
		if (row_compressor.rows_compressed_into_current_value > 0)
			row_compressor_flush(&row_compressor, mycid, true);

		ExecDropSingleTupleTableSlot(slot);
		index_endscan(index_scan);
		index_close(matched_index_rel, AccessShareLock);
	}
	else
	{
#ifdef TS_DEBUG
		const char *compression_path =
			GetConfigOption("timescaledb.show_compression_path_info", true, false);
		if (compression_path != NULL && strcmp(compression_path, "on") == 0)
			elog(INFO, "compress_chunk_tuplesort_start");
#endif
		Tuplesortstate *sorted_rel = compress_chunk_sort_relation(in_rel, n_keys, keys);
		row_compressor_append_sorted_rows(&row_compressor, sorted_rel, in_desc);
		tuplesort_end(sorted_rel);
	}

	row_compressor_finish(&row_compressor);
	truncate_relation(in_table);

	/* Recreate all indexes on out rel, we already have an exclusive lock on it,
	 * so the strong locks taken by reindex_relation shouldn't matter. */
#if PG14_LT
	int options = 0;
#else
	ReindexParams params = { 0 };
	ReindexParams *options = &params;
#endif
	reindex_relation(out_table, 0, options);

	table_close(out_rel, NoLock);
	table_close(in_rel, NoLock);
	cstat.rowcnt_pre_compression = row_compressor.rowcnt_pre_compression;
	cstat.rowcnt_post_compression = row_compressor.num_compressed_rows;
	return cstat;
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
	TableScanDesc heapScan;
	TupleTableSlot *heap_tuple_slot = MakeTupleTableSlot(tupDesc, &TTSOpsHeapTuple);
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
										  maintenance_work_mem,
										  NULL,
										  false /*=randomAccess*/);

	heapScan = table_beginscan(in_rel, GetLatestSnapshot(), 0, (ScanKey) NULL);
	for (tuple = heap_getnext(heapScan, ForwardScanDirection); tuple != NULL;
		 tuple = heap_getnext(heapScan, ForwardScanDirection))
	{
		if (HeapTupleIsValid(tuple))
		{
			/*    This may not be the most efficient way to do things.
			 *     Since we use begin_heap() the tuplestore expects tupleslots,
			 *      so ISTM that the options are this or maybe putdatum().
			 */
			ExecStoreHeapTuple(tuple, heap_tuple_slot, false);

			tuplesort_puttupleslot(tuplesortstate, heap_tuple_slot);
		}
	}

	heap_endscan(heapScan);

	/* Perform an analyze on the chunk to get up-to-date stats before compressing.
	 * We do it at this point because we've just read out the entire chunk into
	 * tuplesort, so its pages are likely to be cached and we can save on I/O.
	 */
	run_analyze_on_chunk(in_rel->rd_id);

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
		elog(ERROR,
			 "table \"%s\" does not have column \"%s\"",
			 get_rel_name(table),
			 NameStr(column->attname));

	att_tup = (Form_pg_attribute) GETSTRUCT(tp);
	/* Other valdation checks beyond just existence of a valid comparison operator could be useful
	 */

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

static void
run_analyze_on_chunk(Oid chunk_relid)
{
	VacuumRelation vr = {
		.type = T_VacuumRelation,
		.relation = NULL,
		.oid = chunk_relid,
		.va_cols = NIL,
	};
	VacuumStmt vs = {
		.type = T_VacuumStmt,
		.rels = list_make1(&vr),
		.is_vacuumcmd = false,
		.options = NIL,
	};

	ExecVacuum(NULL, &vs, true);
}

/* Find segment by index for setting the correct sequence number if
 * we are trying to roll up chunks while compressing
 */
static Oid
get_compressed_chunk_index(Relation compressed_chunk, int16 *uncompressed_col_to_compressed_col,
						   PerColumn *per_column, int n_input_columns)
{
	ListCell *lc;
	int i;

	List *index_oids = RelationGetIndexList(compressed_chunk);

	foreach (lc, index_oids)
	{
		Oid index_oid = lfirst_oid(lc);
		bool matches = true;
		int num_segmentby_columns = 0;
		Relation index_rel = index_open(index_oid, AccessShareLock);
		IndexInfo *index_info = BuildIndexInfo(index_rel);

		for (i = 0; i < n_input_columns; i++)
		{
			if (per_column[i].segmentby_column_index < 1)
				continue;

			/* Last member of the index must be the sequence number column. */
			if (per_column[i].segmentby_column_index >= index_rel->rd_att->natts)
			{
				matches = false;
				break;
			}

			int index_att_offset = AttrNumberGetAttrOffset(per_column[i].segmentby_column_index);

			if (index_info->ii_IndexAttrNumbers[index_att_offset] !=
				AttrOffsetGetAttrNumber(uncompressed_col_to_compressed_col[i]))
			{
				matches = false;
				break;
			}

			num_segmentby_columns++;
		}

		/* Check that we have the correct number of index attributes
		 * and that the last one is the sequence number
		 */
		if (num_segmentby_columns != index_rel->rd_att->natts - 1 ||
			namestrcmp((Name) &index_rel->rd_att->attrs[num_segmentby_columns].attname,
					   COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME) != 0)
			matches = false;

		index_close(index_rel, AccessShareLock);

		if (matches)
			return index_oid;
	}

	return InvalidOid;
}

static int32
index_scan_sequence_number(Relation table_rel, Oid index_oid, ScanKeyData *scankey,
						   int num_scankeys)
{
	int32 result = 0;
	bool is_null;
	Relation index_rel = index_open(index_oid, AccessShareLock);

	IndexScanDesc index_scan =
		index_beginscan(table_rel, index_rel, GetTransactionSnapshot(), num_scankeys, 0);
	index_scan->xs_want_itup = true;
	index_rescan(index_scan, scankey, num_scankeys, NULL, 0);

	if (index_getnext_tid(index_scan, BackwardScanDirection))
	{
		result = index_getattr(index_scan->xs_itup,
							   index_scan->xs_itupdesc
								   ->natts, /* Last attribute of the index is sequence number. */
							   index_scan->xs_itupdesc,
							   &is_null);
		if (is_null)
			result = 0;
	}

	index_endscan(index_scan);
	index_close(index_rel, AccessShareLock);

	return result;
}

static int32
table_scan_sequence_number(Relation table_rel, int16 seq_num_column_num, ScanKeyData *scankey,
						   int num_scankeys)
{
	int32 curr_seq_num = 0, max_seq_num = 0;
	bool is_null;
	HeapTuple compressed_tuple;
	Datum seq_num;
	TupleDesc in_desc = RelationGetDescr(table_rel);

	TableScanDesc heap_scan =
		table_beginscan(table_rel, GetLatestSnapshot(), num_scankeys, scankey);

	for (compressed_tuple = heap_getnext(heap_scan, ForwardScanDirection); compressed_tuple != NULL;
		 compressed_tuple = heap_getnext(heap_scan, ForwardScanDirection))
	{
		Assert(HeapTupleIsValid(compressed_tuple));

		seq_num = heap_getattr(compressed_tuple, seq_num_column_num, in_desc, &is_null);
		if (!is_null)
		{
			curr_seq_num = DatumGetInt32(seq_num);
			if (max_seq_num < curr_seq_num)
			{
				max_seq_num = curr_seq_num;
			}
		}
	}

	heap_endscan(heap_scan);

	return max_seq_num;
}

/* Scan compressed chunk to get the sequence number for current group.
 * This is necessary to do when merging chunks. If the chunk is empty,
 * scan will always return 0 and the sequence number will start from
 * SEQUENCE_NUM_GAP.
 */
static int32
get_sequence_number_for_current_group(Relation table_rel, Oid index_oid,
									  int16 *uncompressed_col_to_compressed_col,
									  PerColumn *per_column, int n_input_columns,
									  int16 seq_num_column_num)
{
	/* No point scanning an empty relation. */
	if (table_rel->rd_rel->relpages == 0)
		return SEQUENCE_NUM_GAP;

	/* If there is a suitable index, use index scan otherwise fallback to heap scan. */
	bool is_index_scan = OidIsValid(index_oid);

	int i, num_scankeys = 0;
	int32 result = 0;

	for (i = 0; i < n_input_columns; i++)
	{
		if (per_column[i].segmentby_column_index < 1)
			continue;

		num_scankeys++;
	}

	MemoryContext scan_ctx = AllocSetContextCreate(CurrentMemoryContext,
												   "get max sequence number scan",
												   ALLOCSET_DEFAULT_SIZES);
	MemoryContext old_ctx;
	old_ctx = MemoryContextSwitchTo(scan_ctx);

	ScanKeyData *scankey = NULL;

	if (num_scankeys > 0)
	{
		scankey = palloc0(sizeof(ScanKeyData) * num_scankeys);

		for (i = 0; i < n_input_columns; i++)
		{
			if (per_column[i].segmentby_column_index < 1)
				continue;

			PerColumn col = per_column[i];
			int16 attno = is_index_scan ?
							  col.segmentby_column_index :
							  AttrOffsetGetAttrNumber(uncompressed_col_to_compressed_col[i]);

			if (col.segment_info->is_null)
			{
				ScanKeyEntryInitialize(&scankey[col.segmentby_column_index - 1],
									   SK_ISNULL | SK_SEARCHNULL,
									   attno,
									   InvalidStrategy, /* no strategy */
									   InvalidOid,		/* no strategy subtype */
									   InvalidOid,		/* no collation */
									   InvalidOid,		/* no reg proc for this */
									   (Datum) 0);		/* constant */
			}
			else
			{
				ScanKeyEntryInitializeWithInfo(&scankey[col.segmentby_column_index - 1],
											   0, /* flags */
											   attno,
											   BTEqualStrategyNumber,
											   InvalidOid, /* No strategy subtype. */
											   col.segment_info->collation,
											   &col.segment_info->eq_fn,
											   col.segment_info->val);
			}
		}
	}

	if (is_index_scan)
	{
		/* Index scan should always use at least one scan key to get the sequence number. */
		Assert(num_scankeys > 0);

		result = index_scan_sequence_number(table_rel, index_oid, scankey, num_scankeys);
	}
	else
	{
		/* Table scan can work without scan keys. */
		result = table_scan_sequence_number(table_rel, seq_num_column_num, scankey, num_scankeys);
	}

	MemoryContextSwitchTo(old_ctx);
	MemoryContextDelete(scan_ctx);

	return result + SEQUENCE_NUM_GAP;
}

/********************
 ** row_compressor **
 ********************/
/* num_compression_infos is the number of columns we will write to in the compressed table */
static void
row_compressor_init(RowCompressor *row_compressor, TupleDesc uncompressed_tuple_desc,
					Relation compressed_table, int num_compression_infos,
					const ColumnCompressionInfo **column_compression_info, int16 *in_column_offsets,
					int16 num_columns_in_compressed_table, bool need_bistate)
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
		.bistate = need_bistate ? GetBulkInsertState() : NULL,
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
		.rowcnt_pre_compression = 0,
		.num_compressed_rows = 0,
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
			int16 segment_min_attr_offset = -1;
			int16 segment_max_attr_offset = -1;
			SegmentMetaMinMaxBuilder *segment_min_max_builder = NULL;
			if (compressed_column_attr->atttypid != compressed_data_type_oid)
				elog(ERROR,
					 "expected column '%s' to be a compressed data type",
					 compression_info->attname.data);

			if (compression_info->orderby_column_index > 0)
			{
				char *segment_min_col_name = compression_column_segment_min_name(compression_info);
				char *segment_max_col_name = compression_column_segment_max_name(compression_info);
				AttrNumber segment_min_attr_number =
					get_attnum(compressed_table->rd_id, segment_min_col_name);
				AttrNumber segment_max_attr_number =
					get_attnum(compressed_table->rd_id, segment_max_col_name);
				if (segment_min_attr_number == InvalidAttrNumber)
					elog(ERROR, "couldn't find metadata column \"%s\"", segment_min_col_name);
				if (segment_max_attr_number == InvalidAttrNumber)
					elog(ERROR, "couldn't find metadata column \"%s\"", segment_max_col_name);
				segment_min_attr_offset = AttrNumberGetAttrOffset(segment_min_attr_number);
				segment_max_attr_offset = AttrNumberGetAttrOffset(segment_max_attr_number);
				segment_min_max_builder =
					segment_meta_min_max_builder_create(column_attr->atttypid,
														column_attr->attcollation);
			}
			*column = (PerColumn){
				.compressor = compressor_for_algorithm_and_type(compression_info->algo_id,
																column_attr->atttypid),
				.min_metadata_attr_offset = segment_min_attr_offset,
				.max_metadata_attr_offset = segment_max_attr_offset,
				.min_max_metadata_builder = segment_min_max_builder,
				.segmentby_column_index = -1,
			};
		}
		else
		{
			if (column_attr->atttypid != compressed_column_attr->atttypid)
				elog(ERROR,
					 "expected segment by column \"%s\" to be same type as uncompressed column",
					 compression_info->attname.data);
			*column = (PerColumn){
				.segment_info = segment_info_new(column_attr),
				.segmentby_column_index = compression_info->segmentby_column_index,
				.min_metadata_attr_offset = -1,
				.max_metadata_attr_offset = -1,
			};
		}
	}

	row_compressor->index_oid =
		get_compressed_chunk_index(compressed_table,
								   row_compressor->uncompressed_col_to_compressed_col,
								   row_compressor->per_column,
								   row_compressor->n_input_columns);
}

static void
row_compressor_append_sorted_rows(RowCompressor *row_compressor, Tuplesortstate *sorted_rel,
								  TupleDesc sorted_desc)
{
	CommandId mycid = GetCurrentCommandId(true);
	TupleTableSlot *slot = MakeTupleTableSlot(sorted_desc, &TTSOpsMinimalTuple);
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
	/* save original memory context */
	const MemoryContext oldcontext = CurrentMemoryContext;

	Assert(row_compressor->rows_compressed_into_current_value == 0);
	Assert(row_compressor->n_input_columns <= row->tts_nvalid);

	MemoryContextSwitchTo(row_compressor->per_row_ctx->parent);
	for (col = 0; col < row_compressor->n_input_columns; col++)
	{
		PerColumn *column = &row_compressor->per_column[col];
		Datum val;
		bool is_null;

		if (column->segment_info == NULL)
			continue;

		Assert(column->compressor == NULL);

		/* Performance Improvment: We should just use array access here; everything is guaranteed to
		   be fetched */
		val = slot_getattr(row, AttrOffsetGetAttrNumber(col), &is_null);
		segment_info_update(column->segment_info, val, is_null);
	}
	/* switch to original memory context */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * The sequence number of the compressed tuple is per segment by grouping
	 * and should be reset when the grouping changes to prevent overflows with
	 * many segmentby columns.
	 *
	 */
	row_compressor->sequence_num =
		get_sequence_number_for_current_group(row_compressor->compressed_table,
											  row_compressor->index_oid,
											  row_compressor->uncompressed_col_to_compressed_col,
											  row_compressor->per_column,
											  row_compressor->n_input_columns,
											  AttrOffsetGetAttrNumber(
												  row_compressor
													  ->sequence_num_metadata_column_offset));
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

		/* Performance Improvement: Since we call getallatts at the beginning, slot_getattr is
		 * useless overhead here, and we should just access the array directly.
		 */
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
				Assert(column->min_metadata_attr_offset >= 0);
				Assert(column->max_metadata_attr_offset >= 0);

				if (!segment_meta_min_max_builder_empty(column->min_max_metadata_builder))
				{
					Assert(compressed_data != NULL);
					row_compressor->compressed_is_null[column->min_metadata_attr_offset] = false;
					row_compressor->compressed_is_null[column->max_metadata_attr_offset] = false;

					row_compressor->compressed_values[column->min_metadata_attr_offset] =
						segment_meta_min_max_builder_min(column->min_max_metadata_builder);
					row_compressor->compressed_values[column->max_metadata_attr_offset] =
						segment_meta_min_max_builder_max(column->min_max_metadata_builder);
				}
				else
				{
					Assert(compressed_data == NULL);
					row_compressor->compressed_is_null[column->min_metadata_attr_offset] = true;
					row_compressor->compressed_is_null[column->max_metadata_attr_offset] = true;
				}
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
	Assert(row_compressor->bistate != NULL);
	heap_insert(row_compressor->compressed_table,
				compressed_tuple,
				mycid,
				0 /*=options*/,
				row_compressor->bistate);

	heap_freetuple(compressed_tuple);

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

		if (column->min_max_metadata_builder != NULL)
		{
			/* segment_meta_min_max_builder_reset will free the values, so  clear here */
			if (!row_compressor->compressed_is_null[column->min_metadata_attr_offset])
			{
				row_compressor->compressed_values[column->min_metadata_attr_offset] = 0;
				row_compressor->compressed_is_null[column->min_metadata_attr_offset] = true;
			}
			if (!row_compressor->compressed_is_null[column->max_metadata_attr_offset])
			{
				row_compressor->compressed_values[column->max_metadata_attr_offset] = 0;
				row_compressor->compressed_is_null[column->max_metadata_attr_offset] = true;
			}
			segment_meta_min_max_builder_reset(column->min_max_metadata_builder);
		}

		row_compressor->compressed_values[compressed_col] = 0;
		row_compressor->compressed_is_null[compressed_col] = true;
	}
	row_compressor->rowcnt_pre_compression += row_compressor->rows_compressed_into_current_value;
	row_compressor->num_compressed_rows++;
	row_compressor->rows_compressed_into_current_value = 0;

	MemoryContextReset(row_compressor->per_row_ctx);
}

static void
row_compressor_finish(RowCompressor *row_compressor)
{
	if (row_compressor->bistate)
		FreeBulkInsertState(row_compressor->bistate);
}

/******************
 ** segment_info **
 ******************/

static SegmentInfo *
segment_info_new(Form_pg_attribute column_attr)
{
	TypeCacheEntry *tce = lookup_type_cache(column_attr->atttypid, TYPECACHE_EQ_OPR_FINFO);

	if (!OidIsValid(tce->eq_opr_finfo.fn_oid))
		elog(ERROR, "no equality function for column \"%s\"", NameStr(column_attr->attname));

	SegmentInfo *segment_info = palloc(sizeof(*segment_info));

	*segment_info = (SegmentInfo){
		.typlen = column_attr->attlen,
		.typ_by_val = column_attr->attbyval,
	};

	fmgr_info_cxt(tce->eq_opr_finfo.fn_oid, &segment_info->eq_fn, CurrentMemoryContext);

	segment_info->eq_fcinfo = HEAP_FCINFO(2);
	segment_info->collation = column_attr->attcollation;
	InitFunctionCallInfoData(*segment_info->eq_fcinfo,
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
	FunctionCallInfo eq_fcinfo;
	/* if one of the datums is null and the other isn't, we must be in a new group */
	if (segment_info->is_null != is_null)
		return false;

	/* they're both null */
	if (segment_info->is_null)
		return true;

	/* neither is null, call the eq function */
	eq_fcinfo = segment_info->eq_fcinfo;

	FC_SET_ARG(eq_fcinfo, 0, segment_info->val);
	FC_SET_ARG(eq_fcinfo, 1, datum);

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

	TupleDesc in_desc;
	Relation in_rel;

	TupleDesc out_desc;
	Relation out_rel;
	ResultRelInfo *indexstate;

	CommandId mycid;
	BulkInsertState bistate;

	Datum *compressed_datums;
	bool *compressed_is_nulls;

	Datum *decompressed_datums;
	bool *decompressed_is_nulls;

	MemoryContext per_compressed_row_ctx;
} RowDecompressor;

static PerCompressedColumn *create_per_compressed_column(TupleDesc in_desc, TupleDesc out_desc,
														 Oid out_relid,
														 Oid compressed_data_type_oid);
static void populate_per_compressed_columns_from_data(PerCompressedColumn *per_compressed_cols,
													  int16 num_cols, Datum *compressed_datums,
													  bool *compressed_is_nulls);
static void row_decompressor_decompress_row(RowDecompressor *row_decompressor);
static bool per_compressed_col_get_data(PerCompressedColumn *per_compressed_col,
										Datum *decompressed_datums, bool *decompressed_is_nulls,
										TupleDesc out_desc);

static RowDecompressor
build_decompressor(Relation in_rel, Relation out_rel)
{
	TupleDesc in_desc = RelationGetDescr(in_rel);
	TupleDesc out_desc = RelationGetDescr(out_rel);

	Oid compressed_typeid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;

	Assert(OidIsValid(compressed_typeid));

	RowDecompressor decompressor = {
		.per_compressed_cols =
			create_per_compressed_column(in_desc, out_desc, out_rel->rd_id, compressed_typeid),
		.num_compressed_columns = in_desc->natts,

		.in_desc = in_desc,
		.in_rel = in_rel,

		.out_desc = out_desc,
		.out_rel = out_rel,
		.indexstate = ts_catalog_open_indexes(out_rel),

		.mycid = GetCurrentCommandId(true),
		.bistate = GetBulkInsertState(),

		.compressed_datums = palloc(sizeof(Datum) * in_desc->natts),
		.compressed_is_nulls = palloc(sizeof(bool) * in_desc->natts),

		/* cache memory used to store the decompressed datums/is_null for form_tuple */
		.decompressed_datums = palloc(sizeof(Datum) * out_desc->natts),
		.decompressed_is_nulls = palloc(sizeof(bool) * out_desc->natts),

		.per_compressed_row_ctx = AllocSetContextCreate(CurrentMemoryContext,
														"decompress chunk per-compressed row",
														ALLOCSET_DEFAULT_SIZES),
	};

	/*
	 * We need to make sure decompressed_is_nulls is in a defined state. While this
	 * will get written for normal columns it will not get written for dropped columns
	 * since dropped columns don't exist in the compressed chunk so we initiallize
	 * with true here.
	 */
	memset(decompressor.decompressed_is_nulls, true, out_desc->natts);

	return decompressor;
}

void
decompress_chunk(Oid in_table, Oid out_table)
{
	/*
	 * Locks are taken in the order uncompressed table then compressed table
	 * for consistency with compress_chunk.
	 * We are _just_ INSERTing into the out_table so in principle we could take
	 * a RowExclusive lock, and let other operations read and write this table
	 * as we work. However, we currently compress each table as a oneshot, so
	 * we're taking the stricter lock to prevent accidents.
	 * We want to prevent other decompressors from decompressing this table,
	 * and we want to prevent INSERTs or UPDATEs which could mess up our decompression.
	 * We may as well allow readers to keep reading the compressed data while
	 * we are compressing, so we only take an ExclusiveLock instead of AccessExclusive.
	 */
	Relation out_rel = table_open(out_table, AccessExclusiveLock);
	Relation in_rel = table_open(in_table, ExclusiveLock);

	RowDecompressor decompressor = build_decompressor(in_rel, out_rel);

	HeapTuple compressed_tuple;
	TableScanDesc heapScan = table_beginscan(in_rel, GetLatestSnapshot(), 0, (ScanKey) NULL);

	for (compressed_tuple = heap_getnext(heapScan, ForwardScanDirection); compressed_tuple != NULL;
		 compressed_tuple = heap_getnext(heapScan, ForwardScanDirection))
	{
		Assert(HeapTupleIsValid(compressed_tuple));
		heap_deform_tuple(compressed_tuple,
						  decompressor.in_desc,
						  decompressor.compressed_datums,
						  decompressor.compressed_is_nulls);

		row_decompressor_decompress_row(&decompressor);
	}

	heap_endscan(heapScan);

	FreeBulkInsertState(decompressor.bistate);
	MemoryContextDelete(decompressor.per_compressed_row_ctx);
	ts_catalog_close_indexes(decompressor.indexstate);

	table_close(out_rel, NoLock);
	table_close(in_rel, NoLock);
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
		 * Assumption: column names are the same on compressed and
		 *       uncompressed chunk.
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
			CompressedDataHeader *header = get_compressed_data_header(compressed_datums[col]);

			per_col->iterator =
				definitions[header->compression_algorithm]
					.iterator_init_forward(PointerGetDatum(header), per_col->decompressed_type);
		}
		else
			per_col->val = compressed_datums[col];
	}
}

static void
row_decompressor_decompress_row(RowDecompressor *decompressor)
{
	/* each compressed row decompresses to at least one row,
	 * even if all the data is NULL
	 */
	bool wrote_data = false;
	bool is_done = false;

	MemoryContext old_ctx = MemoryContextSwitchTo(decompressor->per_compressed_row_ctx);

	populate_per_compressed_columns_from_data(decompressor->per_compressed_cols,
											  decompressor->in_desc->natts,
											  decompressor->compressed_datums,
											  decompressor->compressed_is_nulls);

	do
	{
		/* we're done if all the decompressors return NULL */
		is_done = true;
		for (int16 col = 0; col < decompressor->num_compressed_columns; col++)
		{
			bool col_is_done = per_compressed_col_get_data(&decompressor->per_compressed_cols[col],
														   decompressor->decompressed_datums,
														   decompressor->decompressed_is_nulls,
														   decompressor->out_desc);
			is_done &= col_is_done;
		}

		/* if we're not done we have data to write. even if we're done, each
		 * compressed should decompress to at least one row, so we should write that
		 */
		if (!is_done || !wrote_data)
		{
			HeapTuple decompressed_tuple = heap_form_tuple(decompressor->out_desc,
														   decompressor->decompressed_datums,
														   decompressor->decompressed_is_nulls);

			heap_insert(decompressor->out_rel,
						decompressed_tuple,
						decompressor->mycid,
						0 /*=options*/,
						decompressor->bistate);

			ts_catalog_index_insert(decompressor->indexstate, decompressed_tuple);

			heap_freetuple(decompressed_tuple);
			wrote_data = true;
		}
	} while (!is_done);

	MemoryContextSwitchTo(old_ctx);
	MemoryContextReset(decompressor->per_compressed_row_ctx);
}

/* populate the relevent index in an array from a per_compressed_col.
 * returns if decompression is done for this column
 */
bool
per_compressed_col_get_data(PerCompressedColumn *per_compressed_col, Datum *decompressed_datums,
							bool *decompressed_is_nulls, TupleDesc out_desc)
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
		decompressed_datums[decompressed_column_offset] =
			getmissingattr(out_desc,
						   decompressed_column_offset + 1,
						   &decompressed_is_nulls[decompressed_column_offset]);

		return true;
	}

	/* other compressed data */
	if (per_compressed_col->iterator == NULL)
		elog(ERROR, "tried to decompress more data than was compressed in column");

	decompressed = per_compressed_col->iterator->try_next(per_compressed_col->iterator);
	if (decompressed.is_done)
	{
		/* We want a way to free the decompression iterator's data to avoid OOM issues */
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
	CompressedDataHeader *header;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	DecompressionIterator *iter;
	DecompressResult res;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		header = get_compressed_data_header(PG_GETARG_DATUM(0));

		iter = definitions[header->compression_algorithm]
				   .iterator_init_forward(PointerGetDatum(header),
										  get_fn_expr_argtype(fcinfo->flinfo, 1));

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
	CompressedDataHeader *header;
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	DecompressionIterator *iter;
	DecompressResult res;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		header = get_compressed_data_header(PG_GETARG_DATUM(0));

		iter = definitions[header->compression_algorithm]
				   .iterator_init_reverse(PointerGetDatum(header),
										  get_fn_expr_argtype(fcinfo->flinfo, 1));

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
	CompressedDataHeader *header = get_compressed_data_header(PG_GETARG_DATUM(0));
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
	CompressedDataHeader header = { .vl_len_ = { 0 } };

	header.compression_algorithm = pq_getmsgbyte(buf);

	if (header.compression_algorithm >= _END_COMPRESSION_ALGORITHMS)
		elog(ERROR, "invalid compression algorithm %d", header.compression_algorithm);

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
	decoded_len = pg_b64_decode_compat(input, input_len, decoded, decoded_len);

	if (decoded_len < 0)
		elog(ERROR, "could not decode base64-encoded compressed data");

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
	encoded_len = pg_b64_encode_compat(raw_data, raw_len, encoded, encoded_len);

	if (encoded_len < 0)
		elog(ERROR, "could not base64-encode compressed data");

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

/* Get relstats from compressed chunk and insert into relstats for the
 * corresponding chunk (that held the uncompressed data) from raw hypertable
 */
extern void
update_compressed_chunk_relstats(Oid uncompressed_relid, Oid compressed_relid)
{
	double rowcnt;
	int comp_pages, uncomp_pages, comp_visible, uncomp_visible;
	float comp_tuples, uncomp_tuples, out_tuples;
	Chunk *uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_relid, true);
	Chunk *compressed_chunk = ts_chunk_get_by_relid(compressed_relid, true);

	if (uncompressed_chunk->table_id != uncompressed_relid ||
		uncompressed_chunk->fd.compressed_chunk_id != compressed_chunk->fd.id ||
		compressed_chunk->table_id != compressed_relid)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("mismatched chunks for relstats update on compressed chunk \"%s\"",
						get_rel_name(uncompressed_relid))));
	}

	capture_pgclass_stats(uncompressed_relid, &uncomp_pages, &uncomp_visible, &uncomp_tuples);

	/* Before compressing a chunk in 2.0, we save its stats. Prior
	 * releases do not support this. So the stats on uncompressed relid
	 * could be invalid. In this case, do the best that we can.
	 */
	if (uncomp_tuples == 0)
	{
		/* we need page info from compressed relid */
		capture_pgclass_stats(compressed_relid, &comp_pages, &comp_visible, &comp_tuples);
		rowcnt = (double) ts_compression_chunk_size_row_count(uncompressed_chunk->fd.id);
		if (rowcnt > 0)
			out_tuples = (float4) rowcnt;
		else
			out_tuples = (float4) comp_tuples;
		restore_pgclass_stats(uncompressed_relid, comp_pages, comp_visible, out_tuples);
		CommandCounterIncrement();
	}
}

/*
 * Build scankeys for decompression of specific batches. key_columns references the
 * columns of the uncompressed chunk.
 */
static ScanKeyData *
build_scankeys(int32 hypertable_id, RowDecompressor decompressor, Bitmapset *key_columns,
			   Bitmapset **null_columns, TupleTableSlot *slot, int *num_scankeys)
{
	int key_index = 0;
	ScanKeyData *scankeys = NULL;

	if (!bms_is_empty(key_columns))
	{
		scankeys = palloc0(bms_num_members(key_columns) * 2 * sizeof(ScanKeyData));
		int i = -1;
		while ((i = bms_next_member(key_columns, i)) > 0)
		{
			AttrNumber attno = i + FirstLowInvalidHeapAttributeNumber;
			char *attname = get_attname(decompressor.out_rel->rd_id, attno, false);
			AttrNumber cmp_attno = get_attnum(decompressor.in_rel->rd_id, attname);
			FormData_hypertable_compression *fd =
				ts_hypertable_compression_get_by_pkey(hypertable_id, attname);

			/*
			 * There are 3 possible scenarios we have to consider
			 * when dealing with columns which are part of unique
			 * constraints.
			 *
			 * 1. Column is segmentby-Column
			 * In this case we can add a single ScanKey with an
			 * equality check for the value.
			 * 2. Column is orderby-Column
			 * In this we can add 2 ScanKeys with range constraints
			 * utilizing batch metadata.
			 * 3. Column is neither segmentby nor orderby
			 * In this case we cannot utilize this column for
			 * batch filtering as the values are compressed and
			 * we have no metadata.
			 */

			if (COMPRESSIONCOL_IS_SEGMENT_BY(fd))
			{
				bool isnull;
				Datum value = slot_getattr(slot, attno, &isnull);
				Oid atttypid = decompressor.out_desc->attrs[attno - 1].atttypid;

				TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_EQ_OPR_FINFO);

				/* Segmentby column type should match in compressed and uncompressed chunk */
				Assert(decompressor.out_desc->attrs[AttrNumberGetAttrOffset(attno)].atttypid ==
					   decompressor.in_desc->attrs[AttrNumberGetAttrOffset(cmp_attno)].atttypid);

				if (!OidIsValid(tce->eq_opr_finfo.fn_oid))
					elog(ERROR, "no equality function for type \"%s\"", format_type_be(atttypid));

				/*
				 * In PG versions <= 14 NULL values are always considered distinct
				 * from other NULL values and therefore NULLABLE multi-columnn
				 * unique constraints might expose unexpected behaviour in the
				 * presence of NULL values.
				 * Since SK_SEARCHNULL is not supported by heap scans we cannot
				 * build a ScanKey for NOT NULL and instead have to do those
				 * checks manually.
				 */
				if (isnull)
				{
					*null_columns = bms_add_member(*null_columns, cmp_attno);
				}
				else
				{
					ScanKeyEntryInitialize(&scankeys[key_index],
										   0, /* flags */
										   cmp_attno,
										   BTEqualStrategyNumber,
										   InvalidOid, /* No strategy subtype. */
										   decompressor.out_desc
											   ->attrs[AttrNumberGetAttrOffset(attno)]
											   .attcollation,
										   tce->eq_opr_finfo.fn_oid,
										   value);
					key_index++;
				}
			}
		}
	}

	*num_scankeys = key_index;
	return scankeys;
}

void
decompress_batches_for_insert(ChunkInsertState *cis, Chunk *chunk, TupleTableSlot *slot)
{
	Relation out_rel = cis->rel;

	if (!ts_indexing_relation_has_primary_or_unique_index(out_rel))
	{
		/*
		 * If there are no unique constraints there is nothing to do here.
		 */
		return;
	}

	Chunk *comp = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);
	Relation in_rel = relation_open(comp->table_id, RowExclusiveLock);

	RowDecompressor decompressor = build_decompressor(in_rel, out_rel);
	Bitmapset *key_columns = RelationGetIndexAttrBitmap(out_rel, INDEX_ATTR_BITMAP_KEY);
	Bitmapset *null_columns = NULL;

	int num_scankeys;
	ScanKeyData *scankeys = build_scankeys(chunk->fd.hypertable_id,
										   decompressor,
										   key_columns,
										   &null_columns,
										   slot,
										   &num_scankeys);

	bms_free(key_columns);

	TableScanDesc heapScan =
		table_beginscan(in_rel, GetTransactionSnapshot(), num_scankeys, scankeys);

	for (HeapTuple compressed_tuple = heap_getnext(heapScan, ForwardScanDirection);
		 compressed_tuple != NULL;
		 compressed_tuple = heap_getnext(heapScan, ForwardScanDirection))
	{
		Assert(HeapTupleIsValid(compressed_tuple));
		bool valid = true;

		/*
		 * Since the heap scan API does not support SK_SEARCHNULL we have to check
		 * for NULL values manually when those are part of the constraints.
		 */
		for (int attno = bms_next_member(null_columns, -1); attno >= 0;
			 attno = bms_next_member(null_columns, attno))
		{
			if (!heap_attisnull(compressed_tuple, attno, decompressor.in_desc))
			{
				valid = false;
				break;
			}
		}

		/*
		 * Skip if NULL check failed.
		 */
		if (!valid)
			continue;

		heap_deform_tuple(compressed_tuple,
						  decompressor.in_desc,
						  decompressor.compressed_datums,
						  decompressor.compressed_is_nulls);

		row_decompressor_decompress_row(&decompressor);

		TM_FailureData tmfd;
		TM_Result result pg_attribute_unused();
		result = table_tuple_delete(in_rel,
									&compressed_tuple->t_self,
									decompressor.mycid,
									GetTransactionSnapshot(),
									InvalidSnapshot,
									true,
									&tmfd,
									false);
		Assert(result == TM_Ok);
	}

	heap_endscan(heapScan);

	ts_catalog_close_indexes(decompressor.indexstate);
	FreeBulkInsertState(decompressor.bistate);

	CommandCounterIncrement();

	table_close(in_rel, NoLock);
}
