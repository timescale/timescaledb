/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/xact.h>
#include <utils/snapmgr.h>

#include "recompression.h"
#include "compression.h"
#include "hypertable_cache.h"
#include "ts_catalog/hypertable_compression.h"
#include "create.h"

typedef struct CompressTuplesortstateCxt
{
	Tuplesortstate *tuplestore;
	AttrNumber *sort_keys;
	Oid *sort_operators;
	Oid *sort_collations;
	bool *nulls_first;
} CompressTuplesortstateCxt;

/* Helper method to update current segment which is begin recompressed */
static void
decompress_segment_update_current_segment(CompressedSegmentInfo **current_segment,
										  TupleTableSlot *slot, int nsegmentby_cols)
{
	Datum val;
	bool is_null;
	int seg_idx = 0;
	for (int i = 0; i < nsegmentby_cols; i++)
	{
		int16 col_offset = current_segment[i]->decompressed_chunk_offset;
		val = slot_getattr(slot, AttrOffsetGetAttrNumber(col_offset), &is_null);
		if (!segment_info_datum_is_in_group(current_segment[seg_idx++]->segment_info, val, is_null))
		{
			/* new segment, need to do per-segment processing */
			pfree(current_segment[seg_idx - 1]->segment_info); /* because increased previously */
			SegmentInfo *segment_info =
				segment_info_new(TupleDescAttr(slot->tts_tupleDescriptor, col_offset));
			segment_info_update(segment_info, val, is_null);
			current_segment[seg_idx - 1]->segment_info = segment_info;
			current_segment[seg_idx - 1]->decompressed_chunk_offset = col_offset;
		}
	}
}

/* Helper method to find if segment being recompressed, has encountered a new segment */
static bool
decompress_segment_changed_group(CompressedSegmentInfo **current_segment, TupleTableSlot *slot,
								 int nsegmentby_cols)
{
	Datum val;
	bool is_null;
	bool changed_segment = false;
	for (int i = 0; i < nsegmentby_cols; i++)
	{
		int16 col_offset = current_segment[i]->decompressed_chunk_offset;
		val = slot_getattr(slot, AttrOffsetGetAttrNumber(col_offset), &is_null);
		if (!segment_info_datum_is_in_group(current_segment[i++]->segment_info, val, is_null))
		{
			changed_segment = true;
			break;
		}
	}
	return changed_segment;
}

/* This is a wrapper around row_compressor_append_sorted_rows. */
static void
recompress_segment(Tuplesortstate *tuplesortstate, Relation compressed_chunk_rel,
				   RowCompressor *row_compressor)
{
	row_compressor_append_sorted_rows(row_compressor,
									  tuplesortstate,
									  RelationGetDescr(compressed_chunk_rel));
	/* make changes visible */
	CommandCounterIncrement();
}

/*
 * This function updates catalog chunk compression statistics
 * for an existing compressed chunk after it has been recompressed
 * segmentwise in-place (as opposed to creating a new compressed chunk).
 * Note that because of this it is not possible to accurately report
 * the fields
 * uncompressed_chunk_size, uncompressed_index_size, uncompressed_toast_size
 * anymore, so these are not updated.
 */
static int
compression_chunk_size_catalog_update_recompressed(int32 uncompressed_chunk_id,
												   int32 compressed_chunk_id,
												   const RelationSize *recompressed_size,
												   int64 rowcnt_pre_compression,
												   int64 rowcnt_post_compression)
{
	ScanIterator iterator =
		ts_scan_iterator_create(COMPRESSION_CHUNK_SIZE, RowExclusiveLock, CurrentMemoryContext);
	bool updated = false;

	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), COMPRESSION_CHUNK_SIZE, COMPRESSION_CHUNK_SIZE_PKEY);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_compression_chunk_size_pkey_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   Int32GetDatum(uncompressed_chunk_id));
	ts_scanner_foreach(&iterator)
	{
		Datum values[Natts_compression_chunk_size];
		bool replIsnull[Natts_compression_chunk_size] = { false };
		bool repl[Natts_compression_chunk_size] = { false };
		bool should_free;
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		HeapTuple tuple = ts_scanner_fetch_heap_tuple(ti, false, &should_free);
		HeapTuple new_tuple;
		heap_deform_tuple(tuple, ts_scanner_get_tupledesc(ti), values, replIsnull);

		/* Only update the information pertaining to the compressed chunk */
		/* these fields are about the compressed chunk so they can be updated */
		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] =
			Int64GetDatum(recompressed_size->heap_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_heap_size)] = true;

		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] =
			Int64GetDatum(recompressed_size->toast_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_toast_size)] = true;

		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] =
			Int64GetDatum(recompressed_size->index_size);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_compressed_index_size)] = true;

		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] =
			Int64GetDatum(rowcnt_pre_compression);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_pre_compression)] = true;

		values[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] =
			Int64GetDatum(rowcnt_post_compression);
		repl[AttrNumberGetAttrOffset(Anum_compression_chunk_size_numrows_post_compression)] = true;

		new_tuple =
			heap_modify_tuple(tuple, ts_scanner_get_tupledesc(ti), values, replIsnull, repl);
		ts_catalog_update(ti->scanrel, new_tuple);
		heap_freetuple(new_tuple);

		if (should_free)
			heap_freetuple(tuple);

		updated = true;
		break;
	}

	ts_scan_iterator_end(&iterator);
	ts_scan_iterator_close(&iterator);
	return updated;
}

/* Helper method to find if segment exists given a segmentby columns values */
static bool
is_segment_exists_in_chunk(Chunk *uncompressed_chunk, RowDecompressor *decompressor,
						   CompressedSegmentInfo **current_segment, int nsegmentby_cols,
						   HeapTuple *compressed_tuple)
{
	/* build scankeys for segmentby columns */
	ScanKeyData *scankeys = palloc0(nsegmentby_cols * sizeof(ScanKeyData));
	int num_scankeys = 0;
	int seg_idx = 0;
	bool segment_exists = false;

	/* build scan keys to do lookup into compressed chunks */
	for (int i = 0; i < decompressor->out_desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(decompressor->out_desc, i);
		if (attr->attisdropped)
			continue;
		FormData_hypertable_compression *fd =
			ts_hypertable_compression_get_by_pkey(uncompressed_chunk->fd.hypertable_id,
												  attr->attname.data);
		if (COMPRESSIONCOL_IS_SEGMENT_BY(fd))
		{
			num_scankeys =
				create_segment_filter_scankey(decompressor,
											  attr->attname.data,
											  BTEqualStrategyNumber,
											  scankeys,
											  num_scankeys,
											  NULL,
											  current_segment[seg_idx++]->segment_info->val,
											  false);
		}
	}
	HeapTuple tuple;
	TableScanDesc heapScan =
		table_beginscan(decompressor->in_rel, GetLatestSnapshot(), num_scankeys, scankeys);
	while ((tuple = heap_getnext(heapScan, ForwardScanDirection)) != NULL)
	{
		*compressed_tuple = heap_copytuple(tuple);
		segment_exists = true;
	}
	table_endscan(heapScan);
	pfree(scankeys);
	return segment_exists;
}

/*
 * Helper method to get statistics of compressed chunk, brefore
 * recompression.
 */
static void
get_compression_current_statistics(Chunk *compressed_chunk, unsigned long *num_decompressed_rows,
								   unsigned long *num_compressed_rows)
{
	Relation compressed_chunk_rel;
	TableScanDesc heapScan;
	HeapTuple compressed_tuple;
	TupleTableSlot *heap_tuple_slot = NULL;
	unsigned long total_decompressed_rows = 0;
	unsigned long total_compressed_rows = 0;

	compressed_chunk_rel = RelationIdGetRelation(compressed_chunk->table_id);
	heapScan = table_beginscan(compressed_chunk_rel, GetLatestSnapshot(), 0, NULL);
	heap_tuple_slot = MakeTupleTableSlot(RelationGetDescr(compressed_chunk_rel), &TTSOpsHeapTuple);
	AttrNumber meta_count =
		get_attnum(compressed_chunk_rel->rd_id, COMPRESSION_COLUMN_METADATA_COUNT_NAME);

	while ((compressed_tuple = heap_getnext(heapScan, ForwardScanDirection)) != NULL)
	{
		ExecStoreHeapTuple(compressed_tuple, heap_tuple_slot, false);
		slot_getallattrs(heap_tuple_slot);
		total_decompressed_rows += heap_tuple_slot->tts_values[meta_count - 1];
		total_compressed_rows++;
	}
	table_endscan(heapScan);
	ExecDropSingleTupleTableSlot(heap_tuple_slot);
	*num_decompressed_rows = total_decompressed_rows;
	*num_compressed_rows = total_compressed_rows;
	RelationClose(compressed_chunk_rel);
}

static void
initialize_segment(RowDecompressor *decompressor, CompressedSegmentInfo **current_segment,
				   Chunk *uncompressed_chunk, TupleTableSlot *uncompressed_slot)
{
	int i = 0;
	Datum val;
	bool is_null;
	SegmentInfo *segment_info = NULL;
	/* initialize current segment */
	for (int col = 0; col < uncompressed_slot->tts_tupleDescriptor->natts; col++)
	{
		if (uncompressed_slot->tts_tupleDescriptor->attrs[col].attisdropped)
			continue;
		val = slot_getattr(uncompressed_slot, AttrOffsetGetAttrNumber(col), &is_null);
		Form_pg_attribute decompressed_attr = TupleDescAttr(decompressor->out_desc, col);
		char *col_name = NameStr(decompressed_attr->attname);
		FormData_hypertable_compression *fd =
			ts_hypertable_compression_get_by_pkey(uncompressed_chunk->fd.hypertable_id, col_name);
		if (COMPRESSIONCOL_IS_SEGMENT_BY(fd))
		{
			segment_info =
				segment_info_new(TupleDescAttr(uncompressed_slot->tts_tupleDescriptor, col));
			current_segment[i]->decompressed_chunk_offset = col;
			/* also need to call segment_info_update here to update the val part */
			segment_info_update(segment_info, val, is_null);
			current_segment[i]->segment_info = segment_info;
			i++;
		}
	}
}

/* initialize tuplesort state */
static CompressTuplesortstateCxt *
initialize_tuplestore(Relation uncompressed_chunk_rel, const ColumnCompressionInfo **keys,
					  int n_keys)
{
	CompressTuplesortstateCxt *tuplestorecxt = NULL;
	tuplestorecxt = palloc0(sizeof(CompressTuplesortstateCxt));
	tuplestorecxt->sort_keys = palloc(sizeof(AttrNumber) * n_keys);
	tuplestorecxt->sort_operators = palloc(sizeof(Oid) * n_keys);
	tuplestorecxt->sort_collations = palloc(sizeof(Oid) * n_keys);
	tuplestorecxt->nulls_first = palloc(sizeof(bool) * n_keys);

	for (int n = 0; n < n_keys; n++)
		compress_chunk_populate_sort_info_for_column(RelationGetRelid(uncompressed_chunk_rel),
													 keys[n],
													 &tuplestorecxt->sort_keys[n],
													 &tuplestorecxt->sort_operators[n],
													 &tuplestorecxt->sort_collations[n],
													 &tuplestorecxt->nulls_first[n]);

	tuplestorecxt->tuplestore = tuplesort_begin_heap(RelationGetDescr(uncompressed_chunk_rel),
													 n_keys,
													 tuplestorecxt->sort_keys,
													 tuplestorecxt->sort_operators,
													 tuplestorecxt->sort_collations,
													 tuplestorecxt->nulls_first,
													 maintenance_work_mem,
													 NULL,
													 false);
	return tuplestorecxt;
}

/* cleaup tuplesort state */
static void
cleanup_tuplestorecxt(CompressTuplesortstateCxt *tuplestorecxt)
{
	if (tuplestorecxt)
	{
		tuplesort_end(tuplestorecxt->tuplestore);
		pfree(tuplestorecxt->sort_keys);
		pfree(tuplestorecxt->sort_operators);
		pfree(tuplestorecxt->sort_collations);
		pfree(tuplestorecxt->nulls_first);
		pfree(tuplestorecxt);
	}
}

/*
 * Helper method to fetch all rows from uncompressed chunk
 * and sort these rows and save it in tuplestore
 */
static int
save_uncompressed_rows_in_tuplestore(Chunk *uncompressed_chunk,
									 Tuplesortstate *uncompressed_rows_sortstate)
{
	Relation uncompressed_chunk_rel;
	TableScanDesc heapScan;
	HeapTuple uncompressed_tuple;
	TupleTableSlot *heap_tuple_slot = NULL;
	int total_uncompressed_rows = 0;

	uncompressed_chunk_rel = RelationIdGetRelation(uncompressed_chunk->table_id);
	heapScan = table_beginscan(uncompressed_chunk_rel, GetLatestSnapshot(), 0, NULL);
	heap_tuple_slot =
		MakeTupleTableSlot(RelationGetDescr(uncompressed_chunk_rel), &TTSOpsHeapTuple);

	while ((uncompressed_tuple = heap_getnext(heapScan, ForwardScanDirection)) != NULL)
	{
		ExecStoreHeapTuple(uncompressed_tuple, heap_tuple_slot, false);
		slot_getallattrs(heap_tuple_slot);
		tuplesort_puttupleslot(uncompressed_rows_sortstate, heap_tuple_slot);
		simple_heap_delete(uncompressed_chunk_rel, &uncompressed_tuple->t_self);
		total_uncompressed_rows++;
	}
	RelationClose(uncompressed_chunk_rel);
	ExecDropSingleTupleTableSlot(heap_tuple_slot);
	table_endscan(heapScan);
	/* sort the tuplestore */
	tuplesort_performsort(uncompressed_rows_sortstate);
	return total_uncompressed_rows;
}

/*
 * Helper method to fetch _ts_meta_count from a compressed tuple.
 */
static void
fetch_segment_metadata_count(RowDecompressor *decompressor, HeapTuple compressed_tuple,
							 Datum *metacount_val)
{
	Datum *values = palloc0(sizeof(Datum) * decompressor->in_desc->natts);
	bool *nulls = palloc0(sizeof(bool) * decompressor->in_desc->natts);
	heap_deform_tuple(compressed_tuple, decompressor->in_desc, values, nulls);
	if (metacount_val)
	{
		/* fetch number of rows present in current segment of compressed tuple */
		AttrNumber meta_count =
			get_attnum(decompressor->in_rel->rd_id, COMPRESSION_COLUMN_METADATA_COUNT_NAME);
		*metacount_val = values[meta_count - 1];
	}
	pfree(values);
	pfree(nulls);
}

/*
 * Helper method to get total rows per segment before recompressing
 * that particular segment. This helps us decide if we need to split
 * a segment into 2 equal halfs or not.
 */
static unsigned long
get_total_rows_per_segment(RowDecompressor *decompressor, HeapTuple compressed_tuple,
						   unsigned long uncompressed_rows_per_segment)
{
	Datum meta_count = 0;
	fetch_segment_metadata_count(decompressor, compressed_tuple, &meta_count);
	if ((meta_count + uncompressed_rows_per_segment) >= MAX_ROWS_PER_COMPRESSION)
		return (meta_count + uncompressed_rows_per_segment);
	else
		return (2 * MAX_ROWS_PER_COMPRESSION);
}

/*
 * Helper method to decompress a compressed tuple.
 * This method will return the sequence number of the current
 * compressed tuple.
 */
static int
decompress_in_tuplestore(RowDecompressor *decompressor, HeapTuple compressed_tuple,
						 Tuplesortstate *merge_tuplestore)
{
	Assert(HeapTupleIsValid(compressed_tuple));
	heap_deform_tuple(compressed_tuple,
					  decompressor->in_desc,
					  decompressor->compressed_datums,
					  decompressor->compressed_is_nulls);
	/* fetch sequence number of current compressed tuple */
	AttrNumber seq_num =
		get_attnum(decompressor->in_rel->rd_id, COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME);
	/* decompress found tuple and delete from compressed chunk */
	row_decompressor_decompress_row(decompressor, merge_tuplestore);
	TM_FailureData tmfd;
	TM_Result result pg_attribute_unused();
	result = table_tuple_delete(decompressor->in_rel,
								&compressed_tuple->t_self,
								GetCurrentCommandId(true),
								GetTransactionSnapshot(),
								InvalidSnapshot,
								true,
								&tmfd,
								false);
	Assert(result == TM_Ok);
	/* make changes visible */
	CommandCounterIncrement();
	return decompressor->compressed_datums[seq_num - 1];
}

/*
 * Helper method to identify all segmentby columns who have NULL values.
 */
static void
save_segmentby_null_columns(CompressedSegmentInfo **current_segment, TupleTableSlot *slot,
							int nsegmentby_cols, Bitmapset **null_columns)
{
	Datum val;
	bool is_null;
	for (int i = 0; i < nsegmentby_cols; i++)
	{
		int16 col_offset = current_segment[i]->decompressed_chunk_offset;
		val = slot_getattr(slot, AttrOffsetGetAttrNumber(col_offset), &is_null);
		if (val == 0 && is_null)
			*null_columns = bms_add_member(*null_columns, AttrOffsetGetAttrNumber(col_offset));
	}
}

/*
 * Helper method used to build scankeys based on segmentby and orderby
 * key columns.
 */
static ScanKeyData *
build_segment_order_by_scankeys(RowDecompressor *decompressor, Chunk *uncompressed_chunk,
								TupleTableSlot *slot, int *num_scankeys)
{
	ScanKeyData *seg_order_by_scankeys;
	Bitmapset *key_columns = NULL;
	Bitmapset *null_columns = NULL;
	int num_keys = 0;
	for (int i = 0; i < decompressor->out_desc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(decompressor->out_desc, i);
		if (attr->attisdropped)
			continue;
		FormData_hypertable_compression *fd =
			ts_hypertable_compression_get_by_pkey(uncompressed_chunk->fd.hypertable_id,
												  attr->attname.data);
		if (COMPRESSIONCOL_IS_SEGMENT_BY(fd) || COMPRESSIONCOL_IS_ORDER_BY(fd))
		{
			key_columns =
				bms_add_member(key_columns, attr->attnum - FirstLowInvalidHeapAttributeNumber);
		}
	}
	seg_order_by_scankeys = build_scankeys(uncompressed_chunk->fd.hypertable_id,
										   uncompressed_chunk->table_id,
										   *decompressor,
										   key_columns,
										   &null_columns,
										   slot,
										   &num_keys);
	bms_free(key_columns);
	*num_scankeys = num_keys;
	return seg_order_by_scankeys;
}

void
recompress_partial_chunks(Chunk *uncompressed_chunk, Chunk *compressed_chunk)
{
	Relation uncompressed_chunk_rel;
	Relation compressed_chunk_rel;
	RowDecompressor decompressor;
	RowCompressor row_compressor;
	const ColumnCompressionInfo **colinfo_array = NULL;
	const ColumnCompressionInfo **keys = NULL;
	CompressedSegmentInfo **current_segment = NULL;
	CompressTuplesortstateCxt *tuplestorecxt;
	CompressTuplesortstateCxt *uncompressed_tuplestorecxt;
	HeapTuple *prev_compressed_tuple = NULL;
	TupleTableSlot *uncompressed_slot;
	List *htcols_list;
	ListCell *lc;

	int htcols_listlen = 0;
	int nsegmentby_cols = 0;
	int i = 0;
	unsigned long total_segments_decompressed = 0;
	unsigned long total_uncompressed_rows_per_segment = 0;
	unsigned long total_uncompressed_rows_per_chunk = 0;
	int n_keys = 0;
	int16 *in_column_offsets = NULL;

	bool compressed_tuple_found = false;
	bool last_segment_tuple_found = false;

	htcols_list = ts_hypertable_compression_get(uncompressed_chunk->fd.hypertable_id);
	htcols_listlen = list_length(htcols_list);

	colinfo_array = palloc(sizeof(ColumnCompressionInfo *) * htcols_listlen);
	foreach (lc, htcols_list)
	{
		FormData_hypertable_compression *fd = (FormData_hypertable_compression *) lfirst(lc);
		colinfo_array[i++] = fd;
		if (COMPRESSIONCOL_IS_SEGMENT_BY(fd))
			nsegmentby_cols++;
	}
	in_column_offsets = compress_chunk_populate_keys(uncompressed_chunk->table_id,
													 colinfo_array,
													 htcols_listlen,
													 &n_keys,
													 &keys);

	uncompressed_chunk_rel = RelationIdGetRelation(uncompressed_chunk->table_id);
	compressed_chunk_rel = RelationIdGetRelation(compressed_chunk->table_id);
	decompressor = build_decompressor(compressed_chunk_rel, uncompressed_chunk_rel);
	/* do not need the indexes on the uncompressed chunk as we do not write to it anymore */
	ts_catalog_close_indexes(decompressor.indexstate);

	row_compressor_init(&row_compressor,
						RelationGetDescr(uncompressed_chunk_rel),
						compressed_chunk_rel,
						htcols_listlen,
						colinfo_array,
						in_column_offsets,
						RelationGetDescr(compressed_chunk_rel)->natts,
						true /*need_bistate*/,
						true /*reset_sequence*/,
						RECOMPRESS);

	uncompressed_tuplestorecxt = initialize_tuplestore(uncompressed_chunk_rel, keys, n_keys);
	/* save all rows from uncompressed chunk into tuplestore */
	total_uncompressed_rows_per_chunk =
		save_uncompressed_rows_in_tuplestore(uncompressed_chunk,
											 uncompressed_tuplestorecxt->tuplestore);
	if (total_uncompressed_rows_per_chunk)
	{
		unsigned long num_new_segment_rows = 0;
		unsigned long num_rows_pre_compression = 0;
		unsigned long num_rows_post_compression = 0;
		bool new_segment_found = false;
		/* fetch initial statistics of current chunk */
		get_compression_current_statistics(compressed_chunk,
										   &num_rows_pre_compression,
										   &num_rows_post_compression);
		/* fetch matching compressed rows and save in tuplestore */
		tuplestorecxt = initialize_tuplestore(decompressor.out_rel, keys, n_keys);
		uncompressed_slot = MakeTupleTableSlot(decompressor.out_desc, &TTSOpsMinimalTuple);

		/*
		 * For every row (R1) present in uncompressed chunk, do following:
		 * 1. Do a lookup into compressed chunk based on segmentby/orderby values
		 * 2. If compressed row (R2) exists:
		 * 		a. Save row R2 into tuplestore
		 *      b. Find all rows from uncompressed chunk which match R2 and save in tuplestore
		 *      c. Decompress R2 and save in tuplestore.
		 *      d. Sort tuplestore and perform compression.
		 * 3. If compressed row (R2) does not exists:
		 * 		a. Save all rows from uncompressed chunk with same segmentby values in tuplestore
		 *      b. Sort tuplestore and perform compression.
		 */
		while (tuplesort_gettupleslot(uncompressed_tuplestorecxt->tuplestore,
									  true /*=forward*/,
									  false /*=copy*/,
									  uncompressed_slot,
									  NULL /*=abbrev*/))
		{
			int num_scankeys = 0;
			bool is_new_segment = false;
			HeapTuple compressed_tuple;
			ScanKeyData *seg_order_by_scankeys;
			Bitmapset *null_columns = NULL;

			slot_getallattrs(uncompressed_slot);
			if (!current_segment)
			{
				/* initialize segment */
				current_segment = palloc(sizeof(CompressedSegmentInfo *) * nsegmentby_cols);
				for (int i = 0; i < nsegmentby_cols; i++)
					current_segment[i] = palloc(sizeof(CompressedSegmentInfo));
				initialize_segment(&decompressor,
								   current_segment,
								   uncompressed_chunk,
								   uncompressed_slot);
			}
			else
			{
				/* check if there is change in segmentby value */
				is_new_segment = decompress_segment_changed_group(current_segment,
																  uncompressed_slot,
																  nsegmentby_cols);
			}
			/* save segmentby column attribute numbers for all NULL values */
			save_segmentby_null_columns(current_segment,
										uncompressed_slot,
										nsegmentby_cols,
										&null_columns);
			if (is_new_segment)
			{
				if (compressed_tuple_found)
				{
					/*
					 * Check if this compressed row has max rows per compression.
					 * If total rows per compression exceeds MAX_ROWS_PER_COMPRESSION
					 * then we split this segment into 2 equal halfs
					 */
					row_compressor.total_rows_per_segment =
						get_total_rows_per_segment(&decompressor,
												   *prev_compressed_tuple,
												   total_uncompressed_rows_per_segment);

					/* there exists a previously fetched compressed row */
					/* perform decompression and save rows into tuplestore */
					row_compressor.update_sequence = false;
					row_compressor.sequence_num =
						decompress_in_tuplestore(&decompressor,
												 *prev_compressed_tuple,
												 tuplestorecxt->tuplestore);
					total_segments_decompressed++;
					compressed_tuple_found = false;
				}
				/* perform compression on accumulated tuples present in tuplestore */
				tuplesort_performsort(tuplestorecxt->tuplestore);
				recompress_segment(tuplestorecxt->tuplestore,
								   decompressor.out_rel,
								   &row_compressor);
				cleanup_tuplestorecxt(tuplestorecxt);
				tuplestorecxt = initialize_tuplestore(decompressor.out_rel, keys, n_keys);
				total_uncompressed_rows_per_segment = 0;
				if (prev_compressed_tuple)
				{
					pfree(prev_compressed_tuple);
					prev_compressed_tuple = NULL;
				}
				if (new_segment_found)
				{
					num_new_segment_rows++;
					new_segment_found = false;
				}
				/* update new segment */
				decompress_segment_update_current_segment(current_segment,
														  uncompressed_slot,
														  nsegmentby_cols);
				last_segment_tuple_found = false;
			}
			/* build scan keys to do lookup into compressed chunks */
			seg_order_by_scankeys = build_segment_order_by_scankeys(&decompressor,
																	uncompressed_chunk,
																	uncompressed_slot,
																	&num_scankeys);

			TableScanDesc heapScan = table_beginscan(decompressor.in_rel,
													 GetLatestSnapshot(),
													 num_scankeys,
													 seg_order_by_scankeys);
			if (null_columns)
			{
				/*
				 * Scankeys for heapscan do not support NULL value searches, thus
				 * fetch compressed tuples having NULLS in segmentby columns using
				 * below approach
				 */
				while ((compressed_tuple = heap_getnext(heapScan, ForwardScanDirection)) != NULL)
				{
					bool nulls_found = true;
					for (int attno = bms_next_member(null_columns, -1); attno >= 0;
						 attno = bms_next_member(null_columns, attno))
					{
						if (!heap_attisnull(compressed_tuple, attno, decompressor.in_desc))
						{
							nulls_found = false;
							break;
						}
					}
					if (nulls_found)
						break;
				}
			}
			else
			{
				compressed_tuple = heap_getnext(heapScan, ForwardScanDirection);
			}
			if (compressed_tuple)
			{
				compressed_tuple_found = true;
				if (prev_compressed_tuple == NULL)
				{
					/* matching compressed tuple is fetched */
					prev_compressed_tuple = (HeapTuple *) palloc0(sizeof(HeapTuple));
					*prev_compressed_tuple = heap_copytuple(compressed_tuple);
					total_uncompressed_rows_per_segment++;
					tuplesort_puttupleslot(tuplestorecxt->tuplestore, uncompressed_slot);
				}
				else
				{
					ItemPointer ptr1 = &(*prev_compressed_tuple)->t_self;
					ItemPointer ptr2 = &compressed_tuple->t_self;
					if (ItemPointerCompare(ptr1, ptr2) == 0)
					{
						/*
						 * check if fetched uncompressed tuple belongs to the same
						 * compressed tuple which was fetched earlier
						 */
						total_uncompressed_rows_per_segment++;
						tuplesort_puttupleslot(tuplestorecxt->tuplestore, uncompressed_slot);
					}
					else
					{
						row_compressor.total_rows_per_segment =
							get_total_rows_per_segment(&decompressor,
													   *prev_compressed_tuple,
													   total_uncompressed_rows_per_segment);
						/* perform compression on accumulated tuples */
						row_compressor.update_sequence = false;
						row_compressor.sequence_num =
							decompress_in_tuplestore(&decompressor,
													 *prev_compressed_tuple,
													 tuplestorecxt->tuplestore);
						total_segments_decompressed++;
						tuplesort_performsort(tuplestorecxt->tuplestore);
						recompress_segment(tuplestorecxt->tuplestore,
										   decompressor.out_rel,
										   &row_compressor);
						cleanup_tuplestorecxt(tuplestorecxt);
						tuplestorecxt = initialize_tuplestore(decompressor.out_rel, keys, n_keys);
						total_uncompressed_rows_per_segment = 1;
						*prev_compressed_tuple = heap_copytuple(compressed_tuple);
						tuplesort_puttupleslot(tuplestorecxt->tuplestore, uncompressed_slot);
					}
				}
			}
			else
			{
				/* there is no matching compressed tuple */
				if (compressed_tuple_found && prev_compressed_tuple && !last_segment_tuple_found)
				{
					row_compressor.update_sequence = false;
					row_compressor.sequence_num =
						decompress_in_tuplestore(&decompressor,
												 *prev_compressed_tuple,
												 tuplestorecxt->tuplestore);
					total_segments_decompressed++;
					/* perform compression on accumulated tuples */
					tuplesort_performsort(tuplestorecxt->tuplestore);
					recompress_segment(tuplestorecxt->tuplestore,
									   decompressor.out_rel,
									   &row_compressor);
					cleanup_tuplestorecxt(tuplestorecxt);
					tuplestorecxt = initialize_tuplestore(decompressor.out_rel, keys, n_keys);
					total_uncompressed_rows_per_segment = 1;
					tuplesort_puttupleslot(tuplestorecxt->tuplestore, uncompressed_slot);
					compressed_tuple_found = false;
					if (prev_compressed_tuple)
					{
						pfree(prev_compressed_tuple);
						prev_compressed_tuple = NULL;
					}
				}
				else
				{
					if (!last_segment_tuple_found)
					{
						HeapTuple last_tuple;
						if (is_segment_exists_in_chunk(uncompressed_chunk,
													   &decompressor,
													   current_segment,
													   nsegmentby_cols,
													   &last_tuple))
						{
							row_compressor.total_rows_per_segment =
								get_total_rows_per_segment(&decompressor,
														   last_tuple,
														   total_uncompressed_rows_per_segment);

							row_compressor.reset_sequence = false;
							if (!prev_compressed_tuple)
								prev_compressed_tuple = (HeapTuple *) palloc0(sizeof(HeapTuple));
							*prev_compressed_tuple = last_tuple;
							compressed_tuple_found = true;
							last_segment_tuple_found = true;
							total_uncompressed_rows_per_segment++;
							tuplesort_puttupleslot(tuplestorecxt->tuplestore, uncompressed_slot);
						}
						else
						{
							row_compressor.update_sequence = true;
							row_compressor.reset_sequence = true;
							total_uncompressed_rows_per_segment++;
							tuplesort_puttupleslot(tuplestorecxt->tuplestore, uncompressed_slot);
							new_segment_found = true;
						}
					}
					else
					{
						total_uncompressed_rows_per_segment++;
						tuplesort_puttupleslot(tuplestorecxt->tuplestore, uncompressed_slot);
					}
				}
			}
			table_endscan(heapScan);
		}
		if (prev_compressed_tuple)
		{
			row_compressor.total_rows_per_segment =
				get_total_rows_per_segment(&decompressor,
										   *prev_compressed_tuple,
										   total_uncompressed_rows_per_segment);

			/* perform decompression and save rows into tuplestore */
			row_compressor.update_sequence = false;
			row_compressor.sequence_num = decompress_in_tuplestore(&decompressor,
																   *prev_compressed_tuple,
																   tuplestorecxt->tuplestore);
			total_segments_decompressed++;
			pfree(prev_compressed_tuple);
			prev_compressed_tuple = NULL;
		}
		tuplesort_performsort(tuplestorecxt->tuplestore);
		recompress_segment(tuplestorecxt->tuplestore, decompressor.out_rel, &row_compressor);
		row_compressor_finish(&row_compressor);
		FreeBulkInsertState(decompressor.bistate);

		/****** update compression statistics ******/
		RelationSize after_size = ts_relation_size_impl(compressed_chunk->table_id);
		/* the compression size statistics we are able to update and accurately report are:
		 * rowcount pre/post compression,
		 * compressed chunk sizes */
		row_compressor.rowcnt_pre_compression =
			num_rows_pre_compression + total_uncompressed_rows_per_chunk;
		row_compressor.num_compressed_rows =
			num_rows_post_compression +
			(new_segment_found ? num_new_segment_rows + 1 : num_new_segment_rows);
		compression_chunk_size_catalog_update_recompressed(uncompressed_chunk->fd.id,
														   compressed_chunk->fd.id,
														   &after_size,
														   row_compressor.rowcnt_pre_compression,
														   row_compressor.num_compressed_rows);
		/* cleanup */
		cleanup_tuplestorecxt(tuplestorecxt);
		ExecDropSingleTupleTableSlot(uncompressed_slot);
		elog(LOG, "Total decompressed segments are: %lu", total_segments_decompressed);
	}
	cleanup_tuplestorecxt(uncompressed_tuplestorecxt);
	RelationClose(uncompressed_chunk_rel);
	RelationClose(compressed_chunk_rel);
}
