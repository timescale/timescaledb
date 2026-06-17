/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include "debug_point.h"
#include <access/tableam.h>
#include <miscadmin.h>
#include <parser/parse_coerce.h>
#include <parser/parse_relation.h>
#include <utils/datum.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "api.h"
#include "batch_metadata_builder.h"
#include "compression.h"
#include "compression_dml.h"
#include "create.h"
#include "debug_assert.h"
#include "foreach_ptr.h"
#include "guc.h"
#include "hypertable.h"
#include "indexing.h"
#include "recompress.h"
#include "sparse_index_bloom1.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/chunk_column_stats.h"
#include "ts_catalog/compression_chunk_size.h"
#include "ts_catalog/compression_settings.h"
#include "utils.h"
#include "with_clause/alter_table_with_clause.h"

/*
 * Timing parameters for spin locking heuristics.
 * These are the same as used by Postgres for truncate locking during lazy vacuum.
 * https://github.com/postgres/postgres/blob/4a0650d359c5981270039eeb634c3b7427aa0af5/src/backend/access/heap/vacuumlazy.c#L82
 */
#define RECOMPRESS_EXCLUSIVE_LOCK_WAIT_INTERVAL 50 /* ms */
#ifdef TS_DEBUG
/* Lock timeout reduced for the sake of faster testing. */
#define RECOMPRESS_EXCLUSIVE_LOCK_TIMEOUT 100 /* ms */
#else
#define RECOMPRESS_EXCLUSIVE_LOCK_TIMEOUT 5000 /* ms */
#endif

static bool fetch_uncompressed_chunk_into_tuplesort(Tuplesortstate *tuplesortstate,
													Relation uncompressed_chunk_rel,
													Snapshot snapshot);
static bool delete_tuple_for_recompression(Relation rel, ItemPointer tid, Snapshot snapshot);
static void update_current_segment(CompressedSegmentInfo *current_segment, Datum *values,
								   bool *isnulls, int nsegmentby_cols);
static void create_segmentby_scankeys(CompressionSettings *settings, Relation index_rel,
									  Relation compressed_chunk_rel, ScanKeyData *index_scankeys);
static void create_orderby_scankeys(CompressionSettings *settings, Relation index_rel,
									Relation compressed_chunk_rel, ScanKeyData *orderby_scankeys);
static void update_segmentby_scankeys(Datum *values, bool *isnulls, int num_segmentby,
									  ScanKey index_scankeys, bool *key_byval, int16 *key_typlen);
static void update_orderby_scankeys(Datum *values, bool *isnulls, int num_segmentby,
									int num_orderby, ScanKey orderby_scankeys, bool *key_byval,
									int16 *key_typlen);

static enum Batch_match_result match_tuple_batch(TupleTableSlot *compressed_slot, int num_orderby,
												 ScanKey orderby_scankeys, bool *nulls_first);
static bool check_changed_group(CompressedSegmentInfo *current_segment, Datum *values,
								bool *isnulls, int nsegmentby_cols);
static void recompress_segment(Tuplesortstate *tuplesortstate, Relation compressed_chunk_rel,
							   RowCompressor *row_compressor, BulkWriter *writer);
static void try_updating_chunk_status(Chunk *uncompressed_chunk, Relation uncompressed_chunk_rel);

/*
 * Recompress an existing chunk by decompressing the batches
 * that are affected by the addition of newer data. The existing
 * compressed chunk will not be recreated but modified in place.
 *
 * 0 uncompressed_relid REGCLASS
 * 1 if_not_compressed BOOL = false
 */
Datum
tsl_recompress_chunk_segmentwise(PG_FUNCTION_ARGS)
{
	Oid uncompressed_relid = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_not_compressed = PG_ARGISNULL(1) ? true : PG_GETARG_BOOL(1);

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_relid, true);
	ts_hypertable_permissions_check(chunk->hypertable_relid, GetUserId());

	if (!ts_chunk_is_partial(chunk))
	{
		int elevel = if_not_compressed ? NOTICE : ERROR;
		ereport(elevel,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("nothing to recompress in chunk %s.%s",
						NameStr(chunk->fd.schema_name),
						NameStr(chunk->fd.table_name))));
	}
	else
	{
		if (!ts_guc_enable_segmentwise_recompression)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("segmentwise recompression functionality disabled, "
							"enable it by first setting "
							"timescaledb.enable_segmentwise_recompression to on")));
		}
		CompressionSettings *settings = ts_compression_settings_get(uncompressed_relid);
		if (!settings->fd.orderby)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("segmentwise recompression cannot be applied for "
							"compression with no "
							"order by")));
		}
		bool orderby_not_handling_nulls = !is_chunk_orderby_nullhandling(settings);
		if (orderby_not_handling_nulls)
		{
			elog(ts_guc_debug_compression_path_info ? INFO : DEBUG1,
				 "in-memory recompression is disabled due to nullable order by with no firstlast, "
				 "performing segmentwise decompress/compress on chunk \"%s.%s\"",
				 NameStr(chunk->fd.schema_name),
				 NameStr(chunk->fd.table_name));
		}
		recompress_chunk_segmentwise_impl(chunk, orderby_not_handling_nulls);
	}

	PG_RETURN_OID(uncompressed_relid);
}

static RecompressContext *
compress_chunk_populate_recompress_ctx(CompressionSettings *settings,
									   Relation uncompressed_chunk_rel,
									   Relation compressed_chunk_rel, Relation index_rel,
									   const bool for_uncompressed)
{
	RecompressContext *recompress_ctx;
	int n;
	int position;
	const char *attname;
	AttrNumber col_attno;
	Relation chunk_rel = for_uncompressed ? uncompressed_chunk_rel : compressed_chunk_rel;
	/* Initialize sort info structure */
	recompress_ctx = palloc0(sizeof(RecompressContext));

	/* Calculate array sizes */
	recompress_ctx->num_segmentby = ts_array_length(settings->fd.segmentby);
	recompress_ctx->num_orderby = ts_array_length(settings->fd.orderby);
	recompress_ctx->n_keys = recompress_ctx->num_segmentby + recompress_ctx->num_orderby;

	/* Allocate arrays */
	Assert(recompress_ctx->n_keys <= INDEX_MAX_KEYS);

	/* Populate sort information for each column */
	for (n = 0; n < recompress_ctx->n_keys; n++)
	{
		Form_pg_attribute attr;
		if (n < recompress_ctx->num_segmentby)
		{
			position = n + 1;
			attname = ts_array_get_element_text(settings->fd.segmentby, position);
			col_attno = get_attnum(chunk_rel->rd_id, attname);
			recompress_ctx->current_segment[n].chunk_offset = AttrNumberGetAttrOffset(col_attno);
			recompress_ctx->current_segment[n].segment_info =
				segment_info_new(TupleDescAttr(RelationGetDescr(chunk_rel),
											   recompress_ctx->current_segment[n].chunk_offset));
		}
		else
		{
			position = n - recompress_ctx->num_segmentby + 1;
			attname = ts_array_get_element_text(settings->fd.orderby, position);
			col_attno = get_attnum(chunk_rel->rd_id, attname);
			recompress_ctx->current_segment[n].chunk_offset = AttrNumberGetAttrOffset(col_attno);
		}
		attr = TupleDescAttr(RelationGetDescr(chunk_rel),
							 recompress_ctx->current_segment[n].chunk_offset);
		recompress_ctx->key_byval[n] = attr->attbyval;
		recompress_ctx->key_typlen[n] = attr->attlen;
		compress_chunk_populate_sort_info_for_column(settings,
													 RelationGetRelid(uncompressed_chunk_rel),
													 attname,
													 &recompress_ctx->sort_keys[n],
													 &recompress_ctx->sort_operators[n],
													 &recompress_ctx->sort_collations[n],
													 &recompress_ctx->nulls_first[n]);
	}

	/* Populate scankeys */
	create_segmentby_scankeys(settings,
							  index_rel,
							  compressed_chunk_rel,
							  recompress_ctx->index_scankeys);
	create_orderby_scankeys(settings,
							index_rel,
							compressed_chunk_rel,
							recompress_ctx->orderby_scankeys);

	return recompress_ctx;
}

static void
free_chunk_recompress_ctx(RecompressContext *recompress_ctx)
{
	if (recompress_ctx == NULL)
	{
		return;
	}

	for (int i = 0; i < recompress_ctx->num_segmentby; i++)
	{
		ScanKey key = &recompress_ctx->index_scankeys[i];
		if (!(key->sk_flags & SK_ISNULL) && !recompress_ctx->key_byval[i] &&
			PointerIsValid(DatumGetPointer(key->sk_argument)))
		{
			pfree(DatumGetPointer(key->sk_argument));
		}
	}

	/* Free orderby scankey datums (min only — max shares the same pointer). */
	for (int i = 0; i < recompress_ctx->num_orderby; i++)
	{
		int key_idx = recompress_ctx->num_segmentby + i;
		ScanKey key = &recompress_ctx->orderby_scankeys[i * 2];
		if (!(key->sk_flags & SK_ISNULL) && !recompress_ctx->key_byval[key_idx] &&
			PointerIsValid(DatumGetPointer(key->sk_argument)))
		{
			pfree(DatumGetPointer(key->sk_argument));
		}
	}

	pfree(recompress_ctx);
}

void
recompress_chunk_segmentwise_impl(Chunk *uncompressed_chunk,
								  bool fullrecompress /* do full decompress/compress segmentwise */)
{
	Oid uncompressed_relid = uncompressed_chunk->table_id;

	/*
	 * only proceed if status in (3, 9, 11)
	 * 1: compressed
	 * 2: compressed_unordered
	 * 4: frozen
	 * 8: compressed_partial
	 */
	if (!ts_chunk_is_compressed(uncompressed_chunk) && ts_chunk_is_partial(uncompressed_chunk))
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unexpected chunk status %d in chunk %s.%s",
						uncompressed_chunk->fd.status,
						NameStr(uncompressed_chunk->fd.schema_name),
						NameStr(uncompressed_chunk->fd.table_name))));
	}

	/* need it to find the segby cols from the catalog */
	CompressionSettings *settings = ts_compression_settings_get(uncompressed_chunk->table_id);

	/* We should not do segment-wise recompression with empty orderby, see #7748
	 */
	Ensure(settings->fd.orderby, "empty order by, cannot recompress segmentwise");

	ereport(DEBUG1,
			(errmsg("acquiring locks for recompression: \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));

	LOCKMODE recompression_lockmode =
		ts_guc_enable_exclusive_locking_recompression ? ExclusiveLock : ShareUpdateExclusiveLock;
	/* lock both chunks, compressed and uncompressed */
	Relation uncompressed_chunk_rel =
		table_open(uncompressed_chunk->table_id, recompression_lockmode);
	Relation compressed_chunk_rel = table_open(settings->fd.compress_relid, recompression_lockmode);

	bool has_unique_constraints =
		ts_indexing_relation_has_primary_or_unique_index(uncompressed_chunk_rel);
	int count;
	LOCKTAG locktag;
	SET_LOCKTAG_RELATION(locktag, MyDatabaseId, uncompressed_relid);

	/*
	 * Recompression does not block inserts but it can interfere with
	 * constraint checking since it moves uncompressed tuples from
	 * uncompressed chunk to compressed chunk but the INSERTs check
	 * tuples in the opposite order.
	 *
	 * If there are unique constraints and multiple INSERTs happening at start
	 * we want to just bail out so not to cause wasted work and bloat.
	 */
	if (has_unique_constraints)
	{
		GetLockConflicts(&locktag, ExclusiveLock, &count);

		if (count > 1)
		{
			elog(WARNING,
				 "skipping recompression of chunk %s.%s due to unique constraints and concurrent "
				 "DML",
				 NameStr(uncompressed_chunk->fd.schema_name),
				 NameStr(uncompressed_chunk->fd.table_name));

			table_close(uncompressed_chunk_rel, NoLock);
			table_close(compressed_chunk_rel, NoLock);

			return;
		}
	}

	Hypertable *ht = ts_hypertable_get_by_id(uncompressed_chunk->fd.hypertable_id);
	if (ht->range_space)
	{
		ts_chunk_column_stats_calculate(ht, uncompressed_chunk);
	}

	TupleDesc compressed_rel_tupdesc = RelationGetDescr(compressed_chunk_rel);
	TupleDesc uncompressed_rel_tupdesc = RelationGetDescr(uncompressed_chunk_rel);
	/******************** row decompressor **************/

	RowDecompressor decompressor = build_decompressor(RelationGetDescr(compressed_chunk_rel),
													  RelationGetDescr(uncompressed_chunk_rel),
													  RelationGetRelid(compressed_chunk_rel),
													  RelationGetRelid(uncompressed_chunk_rel));

	/********** row compressor *******************/
	RowCompressor row_compressor;
	Assert(settings->fd.compress_relid == RelationGetRelid(compressed_chunk_rel));
	row_compressor_init(&row_compressor,
						settings,
						RelationGetDescr(uncompressed_chunk_rel),
						RelationGetDescr(compressed_chunk_rel));

	BulkWriter writer = bulk_writer_build(compressed_chunk_rel, 0);
	Oid index_oid = get_compressed_chunk_index(writer.indexstate, settings);

	/* For chunks with no segmentby settings, we can still do segmentwise recompression
	 * The entire chunk is treated as a single segment
	 */
	elog(ts_guc_debug_compression_path_info ? INFO : DEBUG1,
		 "Using index \"%s\" for recompression",
		 get_rel_name(index_oid));

	LOCKMODE index_lockmode =
		ts_guc_enable_exclusive_locking_recompression ? ExclusiveLock : RowExclusiveLock;
	Relation index_rel = index_open(index_oid, index_lockmode);
	ereport(DEBUG1,
			(errmsg("locks acquired for recompression: \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));

	/* Need to populate recompress context of an uncompressed chunk */
	RecompressContext *recompress_ctx =
		compress_chunk_populate_recompress_ctx(settings,
											   uncompressed_chunk_rel,
											   compressed_chunk_rel,
											   index_rel,
											   true);
	/* Used for sorting and iterating over all the uncompressed tuples that have
	 * to be recompressed. These tuples are sorted based on the segmentby and
	 * orderby settings.
	 */
	Tuplesortstate *input_tuplesortstate = tuplesort_begin_heap(uncompressed_rel_tupdesc,
																recompress_ctx->n_keys,
																recompress_ctx->sort_keys,
																recompress_ctx->sort_operators,
																recompress_ctx->sort_collations,
																recompress_ctx->nulls_first,
																maintenance_work_mem,
																NULL,
																false);

	/* Used for gathering and resorting the tuples that should be recompressed together.
	 * Since we are working on a per-segment level here, we only need to sort them
	 * based on the orderby settings.
	 */
	Tuplesortstate *recompress_tuplesortstate =
		tuplesort_begin_heap(uncompressed_rel_tupdesc,
							 recompress_ctx->num_orderby,
							 &recompress_ctx->sort_keys[recompress_ctx->num_segmentby],
							 &recompress_ctx->sort_operators[recompress_ctx->num_segmentby],
							 &recompress_ctx->sort_collations[recompress_ctx->num_segmentby],
							 &recompress_ctx->nulls_first[recompress_ctx->num_segmentby],
							 maintenance_work_mem,
							 NULL,
							 false);

	/************** snapshot ****************************/
	Snapshot snapshot = RegisterSnapshot(GetTransactionSnapshot());

	TupleTableSlot *uncompressed_slot =
		MakeTupleTableSlot(uncompressed_rel_tupdesc, &TTSOpsMinimalTuple);
	TupleTableSlot *compressed_slot = table_slot_create(compressed_chunk_rel, NULL);

	Datum *values = palloc(sizeof(Datum) * recompress_ctx->n_keys);
	bool *isnulls = palloc(sizeof(bool) * recompress_ctx->n_keys);

	HeapTuple compressed_tuple;
	IndexScanDesc index_scan = index_beginscan_compat(compressed_chunk_rel,
													  index_rel,
													  snapshot,
													  NULL,
													  recompress_ctx->num_segmentby,
													  0);

	bool found_tuple = fetch_uncompressed_chunk_into_tuplesort(input_tuplesortstate,
															   uncompressed_chunk_rel,
															   snapshot);
	if (!found_tuple)
	{
		goto finish;
	}
	tuplesort_performsort(input_tuplesortstate);

	for (found_tuple = tuplesort_gettupleslot(input_tuplesortstate,
											  true /*=forward*/,
											  false /*=copy*/,
											  uncompressed_slot,
											  NULL /*=abbrev*/);
		 found_tuple;)
	{
		CHECK_FOR_INTERRUPTS();

		for (int i = 0; i < recompress_ctx->n_keys; i++)
		{
			values[i] = slot_getattr(uncompressed_slot,
									 AttrOffsetGetAttrNumber(
										 recompress_ctx->current_segment[i].chunk_offset),
									 &isnulls[i]);
		}

		update_current_segment(recompress_ctx->current_segment,
							   values,
							   isnulls,
							   recompress_ctx->num_segmentby);

		/* Build scankeys based on uncompressed tuple values */
		update_segmentby_scankeys(values,
								  isnulls,
								  recompress_ctx->num_segmentby,
								  recompress_ctx->index_scankeys,
								  recompress_ctx->key_byval,
								  recompress_ctx->key_typlen);

		/* We do not match orderby boundaries for full recompress,
		 * so do not need orderby scankeys */
		if (!fullrecompress)
		{
			update_orderby_scankeys(values,
									isnulls,
									recompress_ctx->num_segmentby,
									recompress_ctx->num_orderby,
									recompress_ctx->orderby_scankeys,
									recompress_ctx->key_byval,
									recompress_ctx->key_typlen);
		}
		index_rescan(index_scan,
					 recompress_ctx->index_scankeys,
					 recompress_ctx->num_segmentby,
					 NULL,
					 0);

		bool done_with_segment = false;
		bool tuples_for_recompression = false;
		enum Batch_match_result result;

		/* For full segmentwise decompress/compress we decompress all batches in
		 * the current segment (i.e. treat each batch as a match) */
		if (fullrecompress)
		{
			result = Tuple_match;
		}

		while (index_getnext_slot(index_scan, ForwardScanDirection, compressed_slot))
		{
			/* Check if the uncompressed tuple is before, inside, or after the compressed batch */
			if (!fullrecompress)
			{
				result =
					match_tuple_batch(compressed_slot,
									  recompress_ctx->num_orderby,
									  recompress_ctx->orderby_scankeys,
									  &recompress_ctx->nulls_first[recompress_ctx->num_segmentby]);
			}

			/* If the tuple is before the batch, add it for recompression
			 * also keep adding uncompressed tuples while they are:
			 * - any left
			 * - before the current batch
			 * - in the same segment group
			 */
			while (result == Tuple_before)
			{
				tuples_for_recompression = true;
				tuplesort_puttupleslot(recompress_tuplesortstate, uncompressed_slot);
				/* If we happen to hit the end of uncompressed tuples or tuple changed segment group
				 * we are done with the segment group
				 */
				found_tuple = tuplesort_gettupleslot(input_tuplesortstate,
													 true /*=forward*/,
													 false /*=copy*/,
													 uncompressed_slot,
													 NULL /*=abbrev*/);

				if (!found_tuple)
				{
					done_with_segment = true;
					break;
				}

				for (int i = 0; i < recompress_ctx->n_keys; i++)
				{
					values[i] = slot_getattr(uncompressed_slot,
											 AttrOffsetGetAttrNumber(
												 recompress_ctx->current_segment[i].chunk_offset),
											 &isnulls[i]);
				}

				done_with_segment = check_changed_group(recompress_ctx->current_segment,
														values,
														isnulls,
														recompress_ctx->num_segmentby);
				if (done_with_segment)
				{
					break;
				}

				update_orderby_scankeys(values,
										isnulls,
										recompress_ctx->num_segmentby,
										recompress_ctx->num_orderby,
										recompress_ctx->orderby_scankeys,
										recompress_ctx->key_byval,
										recompress_ctx->key_typlen);
				result =
					match_tuple_batch(compressed_slot,
									  recompress_ctx->num_orderby,
									  recompress_ctx->orderby_scankeys,
									  &recompress_ctx->nulls_first[recompress_ctx->num_segmentby]);
			}

			/* If we are done with segment, recompress everything we have so far
			 * and break out of this segment index scan
			 */
			if (done_with_segment)
			{
				tuples_for_recompression = false;
				recompress_segment(recompress_tuplesortstate,
								   uncompressed_chunk_rel,
								   &row_compressor,
								   &writer);
				break;
			}

			/* If the tuple matches the batch, add the batch for recompression */
			/* Potential optimization: merge uncompressed tuples and decompressed tuples
			 * into the tuplesortstate since they are both already sorted
			 */
			if (result == Tuple_match)
			{
				tuples_for_recompression = true;
				bool should_free;

				compressed_tuple = ExecFetchSlotHeapTuple(compressed_slot, false, &should_free);

				heap_deform_tuple(compressed_tuple,
								  compressed_rel_tupdesc,
								  decompressor.compressed_datums,
								  decompressor.compressed_is_nulls);

				row_decompressor_decompress_row_to_tuplesort(&decompressor,
															 recompress_tuplesortstate);

				if (!delete_tuple_for_recompression(compressed_chunk_rel,
													&(compressed_slot->tts_tid),
													snapshot))
				{
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("aborting recompression due to concurrent updates on "
									"compressed data, retrying with next policy run")));
				}
				CommandCounterIncrement();

				if (should_free)
				{
					heap_freetuple(compressed_tuple);
				}

				continue;
			}

			/* At this point, tuple is after the batch
			 * If there are tuples added for recompression, do it
			 * and continue to the next batch
			 */
			if (tuples_for_recompression)
			{
				tuples_for_recompression = false;
				recompress_segment(recompress_tuplesortstate,
								   uncompressed_chunk_rel,
								   &row_compressor,
								   &writer);
			}
		}

		/* End if we are finished with all uncompressed tuples */
		if (!found_tuple)
		{
			break;
		}

		/* Reset index scan if we are done with this segment */
		if (done_with_segment)
		{
			continue;
		}

		/* We are done with existing batches for this segment group
		 * Everything after this point goes into new batches
		 * until we hit a new segment group or exhaust the uncompressed tuples
		 */
		while (!check_changed_group(recompress_ctx->current_segment,
									values,
									isnulls,
									recompress_ctx->num_segmentby))
		{
			tuples_for_recompression = true;
			tuplesort_puttupleslot(recompress_tuplesortstate, uncompressed_slot);
			found_tuple = tuplesort_gettupleslot(input_tuplesortstate,
												 true /*=forward*/,
												 false /*=copy*/,
												 uncompressed_slot,
												 NULL /*=abbrev*/);
			if (!found_tuple)
			{
				tuples_for_recompression = false;
				recompress_segment(recompress_tuplesortstate,
								   uncompressed_chunk_rel,
								   &row_compressor,
								   &writer);
				break;
			}

			for (int i = 0; i < recompress_ctx->num_segmentby; i++)
			{
				values[i] = slot_getattr(uncompressed_slot,
										 AttrOffsetGetAttrNumber(
											 recompress_ctx->current_segment[i].chunk_offset),
										 &isnulls[i]);
			}
		}

		if (tuples_for_recompression)
		{
			recompress_segment(recompress_tuplesortstate,
							   uncompressed_chunk_rel,
							   &row_compressor,
							   &writer);
		}
	}

finish:
	row_compressor_close(&row_compressor);
	bulk_writer_close(&writer);
	ExecDropSingleTupleTableSlot(uncompressed_slot);
	ExecDropSingleTupleTableSlot(compressed_slot);
	index_endscan(index_scan);
	UnregisterSnapshot(snapshot);
	index_close(index_rel, NoLock);
	row_decompressor_close(&decompressor);

	tuplesort_end(input_tuplesortstate);
	tuplesort_end(recompress_tuplesortstate);

	free_chunk_recompress_ctx(recompress_ctx);

	/* If we can quickly upgrade the lock, lets try updating the chunk status to fully
	 * compressed. But we need to check if there are any uncompressed tuples in the
	 * relation since somebody might have inserted new tuples while we were recompressing.
	 */
	if (ConditionalLockRelation(uncompressed_chunk_rel, ExclusiveLock))
	{
		try_updating_chunk_status(uncompressed_chunk, uncompressed_chunk_rel);
	}
	else if (has_unique_constraints)
	{
		/*
		 * This can be problematic since we cannot acquire ExclusiveLock meaning its
		 * possible there are inserts going which need to check unique constraints.
		 * Due to the reverse direction of tuple movement, concurrent recompression
		 * and speculative insertion could potentially cause false negatives during
		 * constraint checking. For now, our best option here is to bail.
		 *
		 * We use a spin lock to wait for the ExclusiveLock or bail out if we can't get it in time.
		 */

		int lock_retry = 0;
		while (true)
		{
			if (ConditionalLockRelation(uncompressed_chunk_rel, ExclusiveLock))
			{
				try_updating_chunk_status(uncompressed_chunk, uncompressed_chunk_rel);
				break;
			}

			/*
			 * Check for interrupts while trying to (re-)acquire the exclusive
			 * lock.
			 */
			CHECK_FOR_INTERRUPTS();

			if (++lock_retry >
				(RECOMPRESS_EXCLUSIVE_LOCK_TIMEOUT / RECOMPRESS_EXCLUSIVE_LOCK_WAIT_INTERVAL))
			{
				/*
				 * We failed to establish the lock in the specified number of
				 * retries. This means we give up trying to get the exclusive lock are abort the
				 * recompression operation
				 */
				ereport(ERROR,
						(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						 errmsg("aborting recompression due to concurrent DML on uncompressed "
								"data, retrying with next policy run")));
				break;
			}

			(void) WaitLatch(MyLatch,
							 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							 RECOMPRESS_EXCLUSIVE_LOCK_WAIT_INTERVAL,
							 WAIT_EVENT_VACUUM_TRUNCATE);
			ResetLatch(MyLatch);
			DEBUG_WAITPOINT("chunk_recompress_after_latch");
		}
	}

	table_close(uncompressed_chunk_rel, NoLock);
	table_close(compressed_chunk_rel, NoLock);
}

/*
 * perform_recompression expects appropriate permissions and checks have already been done.
 * Relations must have appropriate locks and the CompressionSettings of compressed_chunk and
 * new_compressed_chunk should match
 */
static void
perform_recompression(RecompressContext *recompress_ctx, Relation compressed_chunk_rel,
					  Relation uncompressed_chunk_rel, Relation index_rel,
					  CompressionSettings *new_settings, Relation new_compressed_chunk_rel)
{
	RowDecompressor decompressor;
	Tuplesortstate *tuplesortstate;
	RowCompressor row_compressor;
	BulkWriter writer;
	TupleTableSlot *compressed_slot;
	bool first_iteration = true;
	IndexScanDesc index_scan;
	HeapTuple compressed_tuple;

	PushActiveSnapshot(GetTransactionSnapshot());

	decompressor = build_decompressor(RelationGetDescr(compressed_chunk_rel),
									  RelationGetDescr(uncompressed_chunk_rel),
									  RelationGetRelid(compressed_chunk_rel),
									  RelationGetRelid(uncompressed_chunk_rel));

	tuplesortstate = tuplesort_begin_heap(RelationGetDescr(uncompressed_chunk_rel),
										  recompress_ctx->n_keys,
										  recompress_ctx->sort_keys,
										  recompress_ctx->sort_operators,
										  recompress_ctx->sort_collations,
										  recompress_ctx->nulls_first,
										  maintenance_work_mem,
										  NULL,
										  false);

	row_compressor_init(&row_compressor,
						new_settings,
						RelationGetDescr(uncompressed_chunk_rel),
						RelationGetDescr(new_compressed_chunk_rel));

	writer = bulk_writer_build(new_compressed_chunk_rel, 0);
	compressed_slot = table_slot_create(compressed_chunk_rel, NULL);
	Datum *values = palloc(sizeof(Datum) * recompress_ctx->num_segmentby);
	bool *isnulls = palloc(sizeof(bool) * recompress_ctx->num_segmentby);

	/*
	 * we use the compressed chunk's index to scan so that we get the compressed tuples sorted
	 * by segment-by and order-by minmax
	 */
	index_scan =
		index_beginscan_compat(compressed_chunk_rel, index_rel, GetActiveSnapshot(), NULL, 0, 0);
	index_scan->xs_want_itup = true;
	index_rescan(index_scan, NULL, 0, NULL, 0);

	while (index_getnext_slot(index_scan, ForwardScanDirection, compressed_slot))
	{
		for (int i = 0; i < recompress_ctx->num_segmentby; i++)
		{
			values[i] = index_getattr(index_scan->xs_itup,
									  AttrOffsetGetAttrNumber(i),
									  index_scan->xs_itupdesc,
									  &isnulls[i]);
		}

		if (first_iteration)
		{
			update_current_segment(recompress_ctx->current_segment,
								   values,
								   isnulls,
								   recompress_ctx->num_segmentby);
			first_iteration = false;
		}
		else if (check_changed_group(recompress_ctx->current_segment,
									 values,
									 isnulls,
									 recompress_ctx->num_segmentby))
		{
			recompress_segment(tuplesortstate, uncompressed_chunk_rel, &row_compressor, &writer);
			update_current_segment(recompress_ctx->current_segment,
								   values,
								   isnulls,
								   recompress_ctx->num_segmentby);
		}

		bool should_free;

		compressed_tuple = ExecFetchSlotHeapTuple(compressed_slot, false, &should_free);

		heap_deform_tuple(compressed_tuple,
						  RelationGetDescr(compressed_chunk_rel),
						  decompressor.compressed_datums,
						  decompressor.compressed_is_nulls);

		row_decompressor_decompress_row_to_tuplesort(&decompressor, tuplesortstate);

		if (should_free)
		{
			heap_freetuple(compressed_tuple);
		}
	}

	recompress_segment(tuplesortstate, uncompressed_chunk_rel, &row_compressor, &writer);

	row_compressor_close(&row_compressor);
	bulk_writer_close(&writer);
	ExecDropSingleTupleTableSlot(compressed_slot);
	index_endscan(index_scan);
	row_decompressor_close(&decompressor);
	tuplesort_end(tuplesortstate);
	PopActiveSnapshot();
}

/*
 * Builds recompression settings for a chunk.
 * Keeps the chunk's existing compression settings unless
 * the user has explicitly configured settings at the hypertable level.
 */
static CompressionSettings *
resolve_recompression_settings(Chunk *uncompressed_chunk)
{
	CompressionSettings *chunk_settings = ts_compression_settings_get(uncompressed_chunk->table_id);
	Ensure(chunk_settings != NULL,
		   "compression settings not found for chunk \"%s\"",
		   get_rel_name(uncompressed_chunk->table_id));

	/* get hypertable level settings */
	CompressionSettings *new_settings =
		ts_compression_settings_get(uncompressed_chunk->hypertable_relid);
	Ensure(new_settings != NULL,
		   "compression settings not found for hypertable of chunk \"%s\"",
		   get_rel_name(uncompressed_chunk->table_id));

	new_settings->fd.relid = chunk_settings->fd.relid;
	new_settings->fd.compress_relid = InvalidOid;

	/* Set the chunk settings where hypertable settings are unset */
	if (!new_settings->fd.orderby)
	{
		new_settings->fd.orderby = chunk_settings->fd.orderby;
		new_settings->fd.orderby_desc = chunk_settings->fd.orderby_desc;
		new_settings->fd.orderby_nullsfirst = chunk_settings->fd.orderby_nullsfirst;
	}

	if (!new_settings->fd.segmentby)
	{
		new_settings->fd.segmentby = chunk_settings->fd.segmentby;
	}

	if (ts_can_set_default_sparse_index(new_settings))
	{
		new_settings->fd.index = chunk_settings->fd.index;
	}

	/* Rebuild orderby sparse indexes for the final orderby columns */
	if (new_settings->fd.orderby)
	{
		if (new_settings->fd.index)
		{
			new_settings->fd.index = ts_remove_orderby_sparse_index(new_settings);
		}
		new_settings->fd.index = ts_add_orderby_sparse_index(new_settings);
	}

	Ensure(new_settings->fd.orderby, "empty orderby after resolving recompression settings");

	return new_settings;
}

/*
 * Perform per segment in-memory recompression of a compressed chunk.
 */
bool
recompress_chunk_in_memory_impl(Chunk *uncompressed_chunk)
{
	if (uncompressed_chunk == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("chunk cannot be NULL")));
	}

	Ensure(ts_guc_enable_in_memory_recompression, "in-memory recompression functionality disabled");

	if (!ts_chunk_is_compressed(uncompressed_chunk) || ts_chunk_is_frozen(uncompressed_chunk))
	{
		return false;
	}

	CompressionSettings *settings = ts_compression_settings_get(uncompressed_chunk->table_id);
	Oid compressed_relid = settings->fd.compress_relid;

	Ensure(settings && OidIsValid(compressed_relid),
		   "compressed chunk not found for chunk \"%s\"",
		   get_rel_name(uncompressed_chunk->table_id));

	Ensure(settings->fd.orderby, "empty order by, cannot recompress in-memory");

	LOCKMODE lockmode = ExclusiveLock;
	Relation uncompressed_chunk_rel = table_open(uncompressed_chunk->table_id, lockmode);
	Relation compressed_chunk_rel = table_open(compressed_relid, lockmode);

	CompressionSettings *new_settings = resolve_recompression_settings(uncompressed_chunk);

	/* if segmentby settings have changed, we need to fallback to full recompression */
	if (!ts_array_equal(settings->fd.segmentby, new_settings->fd.segmentby))
	{
		table_close(uncompressed_chunk_rel, lockmode);
		table_close(compressed_chunk_rel, lockmode);
		return false;
	}

	/* Check that the compressed chunk's index exist. TODO: Add support for this scenario */
	CatalogIndexState indstate = CatalogOpenIndexes(compressed_chunk_rel);
	Oid index_oid = get_compressed_chunk_index(indstate, settings);
	CatalogCloseIndexes(indstate);

	if (!OidIsValid(index_oid))
	{
		table_close(uncompressed_chunk_rel, lockmode);
		table_close(compressed_chunk_rel, lockmode);
		return false;
	}

	Relation index_rel = index_open(index_oid, lockmode);
	RecompressContext *recompress_ctx =
		compress_chunk_populate_recompress_ctx(settings,
											   uncompressed_chunk_rel,
											   compressed_chunk_rel,
											   index_rel,
											   false);

	Hypertable *ht = ts_hypertable_get_by_id(uncompressed_chunk->fd.hypertable_id);
	Hypertable *compressed_ht = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
	Chunk *new_compressed_chunk =
		create_compress_chunk(compressed_ht, uncompressed_chunk, InvalidOid, false, new_settings);
	Relation new_compressed_chunk_rel = table_open(new_compressed_chunk->table_id, lockmode);

	perform_recompression(recompress_ctx,
						  compressed_chunk_rel,
						  uncompressed_chunk_rel,
						  index_rel,
						  new_settings,
						  new_compressed_chunk_rel);

	free_chunk_recompress_ctx(recompress_ctx);
	index_close(index_rel, NoLock);
	table_close(uncompressed_chunk_rel, NoLock);
	table_close(compressed_chunk_rel, NoLock);
	table_close(new_compressed_chunk_rel, NoLock);

	LockRelationOid(uncompressed_chunk->table_id, AccessExclusiveLock);
	LockRelationOid(compressed_relid, AccessExclusiveLock);
	ts_chunk_drop_by_relid(compressed_relid, DROP_RESTRICT, -1);
	if (ts_chunk_clear_status(uncompressed_chunk, CHUNK_STATUS_COMPRESSED_UNORDERED))
	{
		ereport(DEBUG1,
				(errmsg("cleared chunk status for recompression: \"%s.%s\"",
						NameStr(uncompressed_chunk->fd.schema_name),
						NameStr(uncompressed_chunk->fd.table_name))));
	}

	/* recompress successful */
	return true;
}

static void
update_scankey(ScanKey index_scankey, Datum val, bool is_null, bool typByVal, int16 typLen)
{
	/* Free previous pass-by-reference datum to prevent memory leaks. */
	if (!(index_scankey->sk_flags & SK_ISNULL) && !typByVal &&
		PointerIsValid(DatumGetPointer(index_scankey->sk_argument)))
	{
		pfree(DatumGetPointer(index_scankey->sk_argument));
	}

	index_scankey->sk_flags = is_null ? SK_ISNULL | SK_SEARCHNULL : 0;
	/*
	 * Deep-copy the value because the tuplesort owns the slot memory
	 * and advancing the tuplesort can invalidate it (use-after-free).
	 */
	index_scankey->sk_argument = is_null ? (Datum) 0 : datumCopy(val, typByVal, typLen);
}

static void
update_segmentby_scankeys(Datum *values, bool *isnulls, int num_segmentby, ScanKey index_scankeys,
						  bool *key_byval, int16 *key_typlen)
{
	for (int i = 0; i < num_segmentby; i++)
	{
		update_scankey(&index_scankeys[i], values[i], isnulls[i], key_byval[i], key_typlen[i]);
	}
}

static void
update_orderby_scankeys(Datum *values, bool *isnulls, int num_segmentby, int num_orderby,
						ScanKey orderby_scankeys, bool *key_byval, int16 *key_typlen)
{
	int min_index, max_index;
	for (int i = 0; i < num_orderby; i++)
	{
		min_index = i * 2;
		max_index = min_index + 1;
		update_scankey(&orderby_scankeys[min_index],
					   values[num_segmentby + i],
					   isnulls[num_segmentby + i],
					   key_byval[num_segmentby + i],
					   key_typlen[num_segmentby + i]);
		update_scankey(&orderby_scankeys[max_index],
					   values[num_segmentby + i],
					   isnulls[num_segmentby + i],
					   key_byval[num_segmentby + i],
					   key_typlen[num_segmentby + i]);
	}
}

static enum Batch_match_result
handle_null_scan(int key_flags, bool nulls_first, enum Batch_match_result result)
{
	/* uncompressed tuple key is NULL */
	if (key_flags & SK_ISNULL)
	{
		return nulls_first ? Tuple_before : Tuple_after;
	}

	return result;
}

static enum Batch_match_result
match_tuple_batch(TupleTableSlot *compressed_slot, int num_orderby, ScanKey orderby_scankeys,
				  bool *nulls_first)
{
	/*
	 * Only the leading orderby column gives a sound before/after verdict from
	 * batch metadata. The min/max for later orderby columns are aggregated
	 * over all rows in the batch, not conditional on the leading column, so a
	 * tuple whose leading column is in range but whose later column is out of
	 * range is interleaved with the batch in multi-column sort order — not
	 * strictly before or after it.
	 */
	if (num_orderby >= 1)
	{
		ScanKey key = &orderby_scankeys[0];
		if (!slot_key_test(compressed_slot, key, nulls_first[0]))
		{
			return handle_null_scan(key->sk_flags, nulls_first[0], Tuple_before);
		}

		key = &orderby_scankeys[1];
		if (!slot_key_test(compressed_slot, key, nulls_first[0]))
		{
			return handle_null_scan(key->sk_flags, nulls_first[0], Tuple_after);
		}
	}
	return Tuple_match;
}

static bool
fetch_uncompressed_chunk_into_tuplesort(Tuplesortstate *tuplesortstate,
										Relation uncompressed_chunk_rel, Snapshot snapshot)
{
	bool matching_exist = false;

	TableScanDesc scan = table_beginscan(uncompressed_chunk_rel, snapshot, 0, 0);
	TupleTableSlot *slot = table_slot_create(uncompressed_chunk_rel, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		matching_exist = true;
		slot_getallattrs(slot);
		tuplesort_puttupleslot(tuplesortstate, slot);
		if (!delete_tuple_for_recompression(uncompressed_chunk_rel, &slot->tts_tid, snapshot))
		{
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("aborting recompression due to concurrent updates on "
							"uncompressed data, retrying with next policy run")));
		}
	}
	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);

	return matching_exist;
}

/* Sort the tuples and recompress them */
static void
recompress_segment(Tuplesortstate *tuplesortstate, Relation compressed_chunk_rel,
				   RowCompressor *row_compressor, BulkWriter *writer)
{
	tuplesort_performsort(tuplesortstate);
	row_compressor_reset(row_compressor);
	row_compressor_append_sorted_rows(row_compressor, tuplesortstate, compressed_chunk_rel, writer);
	tuplesort_reset(tuplesortstate);
	CommandCounterIncrement();
}

static void
update_current_segment(CompressedSegmentInfo *current_segment, Datum *values, bool *isnulls,
					   int nsegmentby_cols)
{
	for (int i = 0; i < nsegmentby_cols; i++)
	{
		/* new segment, need to do per-segment processing */
		segment_info_update(current_segment[i].segment_info, values[i], isnulls[i]);
	}
}

static bool
check_changed_group(CompressedSegmentInfo *current_segment, Datum *values, bool *isnulls,
					int nsegmentby_cols)
{
	for (int i = 0; i < nsegmentby_cols; i++)
	{
		if (!segment_info_datum_is_in_group(current_segment[i].segment_info, values[i], isnulls[i]))
		{
			return true;
		}
	}
	return false;
}

static void
init_scankey(ScanKey sk, AttrNumber attnum, Oid atttypid, Oid attcollid, StrategyNumber strategy)
{
	TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_BTREE_OPFAMILY);
	if (!OidIsValid(tce->btree_opf))
	{
		elog(ERROR, "no btree opfamily for type \"%s\"", format_type_be(atttypid));
	}

	Oid opr = get_opfamily_member(tce->btree_opf, atttypid, atttypid, strategy);

	/*
	 * Fall back to btree operator input type when it is binary compatible with
	 * the column type and no operator for column type could be found.
	 */
	if (!OidIsValid(opr) && IsBinaryCoercible(atttypid, tce->btree_opintype))
	{
		opr =
			get_opfamily_member(tce->btree_opf, tce->btree_opintype, tce->btree_opintype, strategy);
	}

	if (!OidIsValid(opr))
	{
		elog(ERROR, "no operator for type \"%s\"", format_type_be(atttypid));
	}

	opr = get_opcode(opr);
	if (!OidIsValid(opr))
	{
		elog(ERROR, "no opcode for type \"%s\"", format_type_be(atttypid));
	}

	ScanKeyEntryInitialize(sk,
						   0, /* flags */
						   attnum,
						   strategy,
						   InvalidOid, /* No strategy subtype. */
						   attcollid,
						   opr,
						   UnassignedDatum);
}

static void
create_segmentby_scankeys(CompressionSettings *settings, Relation index_rel,
						  Relation compressed_chunk_rel, ScanKeyData *index_scankeys)
{
	int num_segmentby = ts_array_length(settings->fd.segmentby);

	for (int i = 0; i < num_segmentby; i++)
	{
		AttrNumber idx_attnum = AttrOffsetGetAttrNumber(i);
		AttrNumber in_attnum = index_rel->rd_index->indkey.values[i];
		const NameData PG_USED_FOR_ASSERTS_ONLY *attname =
			attnumAttName(compressed_chunk_rel, in_attnum);
		Assert(strcmp(NameStr(*attname),
					  ts_array_get_element_text(settings->fd.segmentby, i + 1)) == 0);

		init_scankey(&index_scankeys[i],
					 idx_attnum,
					 attnumTypeId(index_rel, idx_attnum),
					 attnumCollationId(index_rel, idx_attnum),
					 BTEqualStrategyNumber);
	}
}

static void
create_orderby_scankeys(CompressionSettings *settings, Relation index_rel,
						Relation compressed_chunk_rel, ScanKeyData *orderby_scankeys)
{
	int position;
	int num_orderby = ts_array_length(settings->fd.orderby);
	/* Two scankeys per orderby column, one for the lower-bound metadata column
	 * and one for the upper-bound metadata column. */
	for (int i = 0; i < num_orderby * 2; i = i + 2)
	{
		position = (i / 2) + 1;
		AttrNumber lower_attno;
		AttrNumber upper_attno;
		orderby_sparse_metadata_attnos(settings,
									   compressed_chunk_rel->rd_id,
									   position,
									   &lower_attno,
									   &upper_attno);
		AttrNumber first_attno = lower_attno;
		StrategyNumber first_strategy = BTLessEqualStrategyNumber;
		AttrNumber second_attno = upper_attno;
		StrategyNumber second_strategy = BTGreaterEqualStrategyNumber;

		Assert(first_attno != InvalidAttrNumber);
		Assert(second_attno != InvalidAttrNumber);

		bool is_desc = ts_array_get_element_bool(settings->fd.orderby_desc, position);

		/* If we are using DESC order, swap the order of metadata scankeys
		 * since we rely on the order to determine whether a tuple is before or after
		 * the compressed batch and the index is also ordered in that way.
		 */
		if (is_desc)
		{
			AttrNumber temp_attno = first_attno;
			StrategyNumber temp_strategy = first_strategy;
			first_attno = second_attno;
			first_strategy = second_strategy;
			second_attno = temp_attno;
			second_strategy = temp_strategy;
		}
		init_scankey(&orderby_scankeys[i],
					 first_attno,
					 attnumTypeId(compressed_chunk_rel, first_attno),
					 attnumCollationId(compressed_chunk_rel, first_attno),
					 first_strategy);
		init_scankey(&orderby_scankeys[i + 1],
					 second_attno,
					 attnumTypeId(compressed_chunk_rel, second_attno),
					 attnumCollationId(compressed_chunk_rel, second_attno),
					 second_strategy);
	}
}

/* Deleting a tuple for recompression if we can.
 * If there is an unexpected result, we should just abort the operation completely.
 * There are potential optimizations that can be done here in certain scenarios.
 */
static bool
delete_tuple_for_recompression(Relation rel, ItemPointer tid, Snapshot snapshot)
{
	TM_Result result;
	TM_FailureData tmfd;

	result =
		table_tuple_delete(rel,
						   tid,
						   GetCurrentCommandId(true),
						   snapshot,
						   InvalidSnapshot,
						   true /* for now, just wait for commit/abort, that might let us proceed */
						   ,
						   &tmfd,
						   true /* changingPart */);

	return result == TM_Ok;
}

/* Check if we can update the chunk status to fully compressed after segmentwise recompression
 * We can only do this if there were no concurrent DML operations, so we check to see if there are
 * any uncompressed tuples in the chunk after compression.
 * If there aren't, we can update the chunk status
 *
 * Note: Caller is expected to have an ExclusiveLock on the uncompressed_chunk
 */
static void
try_updating_chunk_status(Chunk *uncompressed_chunk, Relation uncompressed_chunk_rel)
{
	PushActiveSnapshot(GetLatestSnapshot());
	TableScanDesc scan = table_beginscan(uncompressed_chunk_rel, GetActiveSnapshot(), 0, 0);
	ScanDirection scan_dir = BackwardScanDirection;
	TupleTableSlot *slot = table_slot_create(uncompressed_chunk_rel, NULL);

	/* Doing a backwards scan with assumption that newly inserted tuples
	 * are most likely at the end of the heap.
	 */
	bool has_tuples = false;
	if (table_scan_getnextslot(scan, scan_dir, slot))
	{
		has_tuples = true;
	}

	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);
	PopActiveSnapshot();

	if (!has_tuples)
	{
		/*
		 * Only clear PARTIAL. Segmentwise recompression only processes
		 * segments that have new uncompressed data, so segments without new
		 * data are left as-is. Any overlapping batches in those segments
		 * remain as is, so the UNORDERED flag must be preserved.
		 */
		if (ts_chunk_clear_status(uncompressed_chunk, CHUNK_STATUS_COMPRESSED_PARTIAL))
		{
			ereport(DEBUG1,
					(errmsg("cleared chunk status for recompression: \"%s.%s\"",
							NameStr(uncompressed_chunk->fd.schema_name),
							NameStr(uncompressed_chunk->fd.table_name))));
		}

		/* changed chunk status, so invalidate any plans involving this chunk */
		CacheInvalidateRelcacheByRelid(uncompressed_chunk->table_id);
	}
}

/*
 * Drop the physical metadata columns for a list of sparse index objects
 * from the compressed chunk table in a single ALTER TABLE.
 */
static void
drop_sparse_index_columns(Oid compressed_relid, List *index_objs)
{
	List *cmds = NIL;

	foreach_ptr(SparseIndexSettingsObject, index_obj, index_objs)
	{
		const char *type;
		List *columns;
		if (!ts_sparse_index_object_get_type_and_columns(index_obj, &type, &columns))
		{
			continue;
		}

		if (strcmp(type, ts_sparse_index_type_names[_SparseIndexTypeEnumMinmax]) == 0)
		{
			const char *colname = (const char *) lfirst(list_head(columns));
			static const char *minmax_prefixes[] = { "min", "max" };
			for (size_t i = 0; i < sizeof(minmax_prefixes) / sizeof(minmax_prefixes[0]); i++)
			{
				char *meta_name =
					compressed_column_metadata_name_v2(minmax_prefixes[i], &colname, 1);
				Assert(get_attnum(compressed_relid, meta_name) != InvalidAttrNumber);
				AlterTableCmd *cmd = makeNode(AlterTableCmd);
				cmd->subtype = AT_DropColumn;
				cmd->name = meta_name;
				cmd->missing_ok = true;
				cmds = lappend(cmds, cmd);
			}
		}
		else if (strcmp(type, ts_sparse_index_type_names[_SparseIndexTypeEnumFirstLast]) == 0)
		{
			const char *colname = (const char *) lfirst(list_head(columns));
			static const char *firstlast_prefixes[] = { "first", "last" };
			for (size_t i = 0; i < sizeof(firstlast_prefixes) / sizeof(firstlast_prefixes[0]); i++)
			{
				char *meta_name =
					compressed_column_metadata_name_v2(firstlast_prefixes[i], &colname, 1);
				Assert(get_attnum(compressed_relid, meta_name) != InvalidAttrNumber);
				AlterTableCmd *cmd = makeNode(AlterTableCmd);
				cmd->subtype = AT_DropColumn;
				cmd->name = meta_name;
				cmd->missing_ok = true;
				cmds = lappend(cmds, cmd);
			}
		}
		else if (strcmp(type, ts_sparse_index_type_names[_SparseIndexTypeEnumBloom]) == 0)
		{
			char *meta_name =
				compressed_column_metadata_name_list_v2(bloom1_column_prefix, columns);
			Assert(get_attnum(compressed_relid, meta_name) != InvalidAttrNumber);
			AlterTableCmd *cmd = makeNode(AlterTableCmd);
			cmd->subtype = AT_DropColumn;
			cmd->name = meta_name;
			cmd->missing_ok = true;
			cmds = lappend(cmds, cmd);
		}
	}

	if (cmds != NIL)
	{
		ts_alter_table_with_event_trigger(compressed_relid, NULL, cmds, true);
	}
}

/*
 * Add physical metadata columns for a list of sparse index objects
 * to the compressed chunk table in a single ALTER TABLE.
 */
static void
add_sparse_index_columns(Chunk *chunk, Oid compressed_relid, List *index_objs)
{
	List *col_defs = NIL;
	Relation uncompressed_rel = table_open(chunk->table_id, AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(uncompressed_rel);

	foreach_ptr(SparseIndexSettingsObject, index_obj, index_objs)
	{
		const char *type;
		List *columns;
		if (!ts_sparse_index_object_get_type_and_columns(index_obj, &type, &columns))
		{
			continue;
		}

		List *attrs = NIL;
		foreach_ptr(const char, colname, columns)
		{
			AttrNumber attno = get_attnum(chunk->table_id, colname);
			Ensure(AttributeNumberIsValid(attno),
				   "column \"%s\" not found on chunk \"%s.%s\"",
				   colname,
				   NameStr(chunk->fd.schema_name),
				   NameStr(chunk->fd.table_name));
			attrs = lappend(attrs, TupleDescAttr(tupdesc, attno - 1));
		}

		if (strcmp(type, ts_sparse_index_type_names[_SparseIndexTypeEnumMinmax]) == 0)
		{
			col_defs = lappend(col_defs, create_sparse_index_column_def(attrs, "min"));
			col_defs = lappend(col_defs, create_sparse_index_column_def(attrs, "max"));
		}
		else if (strcmp(type, ts_sparse_index_type_names[_SparseIndexTypeEnumFirstLast]) == 0)
		{
			col_defs = lappend(col_defs, create_sparse_index_column_def(attrs, "first"));
			col_defs = lappend(col_defs, create_sparse_index_column_def(attrs, "last"));
		}
		else if (strcmp(type, ts_sparse_index_type_names[_SparseIndexTypeEnumBloom]) == 0)
		{
			col_defs =
				lappend(col_defs, create_sparse_index_column_def(attrs, bloom1_column_prefix));
		}
	}

	table_close(uncompressed_rel, AccessShareLock);

	if (col_defs != NIL)
	{
		List *cmds = NIL;
		foreach_ptr(ColumnDef, coldef, col_defs)
		{
			AlterTableCmd *cmd = makeNode(AlterTableCmd);
			cmd->subtype = AT_AddColumn;
			cmd->def = (Node *) coldef;
			cmd->missing_ok = false;
			cmds = lappend(cmds, cmd);
		}
		ts_alter_table_with_event_trigger(compressed_relid, NULL, cmds, true);
	}
}

/*
 * Create BatchMetadataBuilders for the sparse index objects in to_add.
 * Must be called after add_sparse_index_columns so the compressed chunk
 * already has the metadata columns.
 */
static List *
create_sparse_index_builders(Relation uncompressed_rel, Oid compressed_relid, List *index_objs,
							 bool *repl)
{
	List *builders = NIL;
	TupleDesc tupdesc = RelationGetDescr(uncompressed_rel);
	Oid chunk_relid = RelationGetRelid(uncompressed_rel);

	foreach_ptr(SparseIndexSettingsObject, index_obj, index_objs)
	{
		const char *type;
		List *columns;
		if (!ts_sparse_index_object_get_type_and_columns(index_obj, &type, &columns))
		{
			continue;
		}

		if (strcmp(type, ts_sparse_index_type_names[_SparseIndexTypeEnumBloom]) == 0)
		{
			int num_columns = list_length(columns);
			Oid type_oids[MAX_BLOOM_FILTER_COLUMNS];
			AttrNumber attnums[MAX_BLOOM_FILTER_COLUMNS];
			int col_idx = 0;

			foreach_ptr(const char, colname, columns)
			{
				AttrNumber attno = get_attnum(chunk_relid, colname);
				attnums[col_idx] = attno;
				type_oids[col_idx] = TupleDescAttr(tupdesc, attno - 1)->atttypid;
				col_idx++;
			}

			char *meta_name =
				compressed_column_metadata_name_list_v2(bloom1_column_prefix, columns);
			AttrNumber compressed_attno = get_attnum(compressed_relid, meta_name);
			int bloom_offset = AttrNumberGetAttrOffset(compressed_attno);

			repl[bloom_offset] = true;

			builders = lappend(builders,
							   batch_metadata_builder_bloom1_create(num_columns,
																	type_oids,
																	attnums,
																	bloom_offset));
		}
		else if (strcmp(type, ts_sparse_index_type_names[_SparseIndexTypeEnumMinmax]) == 0)
		{
			const char *colname = (const char *) lfirst(list_head(columns));
			AttrNumber attno = get_attnum(chunk_relid, colname);
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attno - 1);

			char *min_name = compressed_column_metadata_name_v2("min", &colname, 1);
			char *max_name = compressed_column_metadata_name_v2("max", &colname, 1);
			int min_offset = AttrNumberGetAttrOffset(get_attnum(compressed_relid, min_name));
			int max_offset = AttrNumberGetAttrOffset(get_attnum(compressed_relid, max_name));

			repl[min_offset] = true;
			repl[max_offset] = true;

			builders = lappend(builders,
							   batch_metadata_builder_minmax_create(attr->atttypid,
																	attr->attcollation,
																	attno,
																	min_offset,
																	max_offset));
		}
		else if (strcmp(type, ts_sparse_index_type_names[_SparseIndexTypeEnumFirstLast]) == 0)
		{
			const char *colname = (const char *) lfirst(list_head(columns));
			AttrNumber attno = get_attnum(chunk_relid, colname);

			char *first_name = compressed_column_metadata_name_v2("first", &colname, 1);
			char *last_name = compressed_column_metadata_name_v2("last", &colname, 1);
			int first_offset = AttrNumberGetAttrOffset(get_attnum(compressed_relid, first_name));
			int last_offset = AttrNumberGetAttrOffset(get_attnum(compressed_relid, last_name));

			repl[first_offset] = true;
			repl[last_offset] = true;

			builders =
				lappend(builders,
						batch_metadata_builder_firstlast_create(TupleDescAttr(tupdesc, attno - 1)
																	->atttypid,
																attno,
																first_offset,
																last_offset));
		}
	}

	return builders;
}

static List *
modify_compressed_table(Chunk *chunk, bool force)
{
	CompressionSettings *chunk_settings = ts_compression_settings_get(chunk->table_id);
	CompressionSettings *ht_settings = ts_compression_settings_get(chunk->hypertable_relid);

	SparseIndexSettings *ht_index = ts_convert_to_sparse_index_settings(ht_settings->fd.index);
	SparseIndexSettings *chunk_index =
		ts_convert_to_sparse_index_settings(chunk_settings->fd.index);

	/* Step 1: collect sparse index objects to drop, then drop all at once */
	List *to_drop = NIL;
	foreach_ptr(SparseIndexSettingsObject, chunk_obj, chunk_index->objects)
	{
		/* ignore orderby sparse indexes */
		if (ts_sparse_index_is_orderby_source(chunk_obj))
		{
			continue;
		}

		if (force)
		{
			to_drop = lappend(to_drop, chunk_obj);
			continue;
		}

		bool found = false;
		foreach_ptr(SparseIndexSettingsObject, ht_obj, ht_index->objects)
		{
			if (ts_sparse_index_object_equal(chunk_obj, ht_obj))
			{
				found = true;
				break;
			}
		}
		if (!found)
		{
			to_drop = lappend(to_drop, chunk_obj);
		}
	}

	if (to_drop != NIL)
	{
		drop_sparse_index_columns(chunk_settings->fd.compress_relid, to_drop);
	}

	/* Step 2: collect sparse index objects to add, then add all at once */
	List *to_add = NIL;
	foreach_ptr(SparseIndexSettingsObject, ht_obj, ht_index->objects)
	{
		/* ignore orderby sparse indexes */
		if (ts_sparse_index_is_orderby_source(ht_obj))
		{
			continue;
		}

		if (force)
		{
			to_add = lappend(to_add, ht_obj);
			continue;
		}

		bool found = false;
		foreach_ptr(SparseIndexSettingsObject, chunk_obj, chunk_index->objects)
		{
			if (ts_sparse_index_object_equal(ht_obj, chunk_obj))
			{
				found = true;
				break;
			}
		}
		if (!found)
		{
			to_add = lappend(to_add, ht_obj);
		}
	}

	if (to_add != NIL)
	{
		add_sparse_index_columns(chunk, chunk_settings->fd.compress_relid, to_add);
	}

	/* Update the chunk's compression settings to match the hypertable */
	chunk_settings->fd.index = ht_settings->fd.index;
	ts_compression_settings_update(chunk_settings);

	return to_add;
}

/*
 * Scan every compressed batch, decompress it, feed the rows through the
 * builders, and update the compressed tuple with the computed sparse index
 * values.
 */
static void
populate_sparse_index_columns(Relation compressed_rel, RowDecompressor *decompressor,
							  List *builders, bool *repl)
{
	TupleDesc compressed_desc = RelationGetDescr(compressed_rel);
	TableScanDesc scan = table_beginscan(compressed_rel, GetActiveSnapshot(), 0, NULL);
	TupleTableSlot *scan_slot = table_slot_create(compressed_rel, NULL);
	TupleTableSlot *update_slot = MakeSingleTupleTableSlot(compressed_desc, &TTSOpsHeapTuple);

	while (table_scan_getnextslot(scan, ForwardScanDirection, scan_slot))
	{
		CHECK_FOR_INTERRUPTS();

		bool should_free;
		HeapTuple compressed_tuple = ExecFetchSlotHeapTuple(scan_slot, false, &should_free);

		heap_deform_tuple(compressed_tuple,
						  compressed_desc,
						  decompressor->compressed_datums,
						  decompressor->compressed_is_nulls);

		int n_batch_rows = decompress_batch(decompressor);

		/* Feed each decompressed row through the builders */
		for (int i = 0; i < n_batch_rows; i++)
		{
			foreach_ptr(BatchMetadataBuilder, builder, builders)
			{
				builder->update_row(builder, decompressor->decompressed_slots[i]);
			}
		}

		/* Write computed sparse index values into the datum arrays */
		foreach_ptr(BatchMetadataBuilder, builder, builders)
		{
			builder->insert_to_compressed_row(builder,
											  decompressor->compressed_datums,
											  decompressor->compressed_is_nulls);
		}

		/* Update the compressed tuple in-place */
		ItemPointerData tid = scan_slot->tts_tid;
		HeapTuple new_tuple = heap_modify_tuple(compressed_tuple,
												compressed_desc,
												decompressor->compressed_datums,
												decompressor->compressed_is_nulls,
												repl);
		ExecStoreHeapTuple(new_tuple, update_slot, false);

		/*
		 * Sparse index metadata columns are not covered by any index.
		 * If indexes on metadata columns are added in the future,
		 * this will need to handle index updates via update_indexes.
		 */
		TU_UpdateIndexes update_indexes;
		simple_table_tuple_update(compressed_rel,
								  &tid,
								  update_slot,
								  GetActiveSnapshot(),
								  &update_indexes);
		ExecClearTuple(update_slot);

		/* Reset */
		foreach_ptr(BatchMetadataBuilder, builder, builders)
		{
			builder->reset(builder,
						   decompressor->compressed_datums,
						   decompressor->compressed_is_nulls);
		}

		row_decompressor_reset(decompressor);

		if (should_free)
		{
			heap_freetuple(compressed_tuple);
		}
	}

	ExecDropSingleTupleTableSlot(update_slot);
	ExecDropSingleTupleTableSlot(scan_slot);
	table_endscan(scan);
}

void
rebuild_sparse_index_impl(Chunk *uncompressed_chunk, bool force)
{
	Hypertable *ht = ts_hypertable_get_by_id(uncompressed_chunk->fd.hypertable_id);
	Hypertable *compress_ht = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);

	LockRelationOid(ht->main_table_relid, AccessShareLock);
	LockRelationOid(compress_ht->main_table_relid, AccessShareLock);
	LockRelationOid(uncompressed_chunk->table_id, ShareUpdateExclusiveLock);

	/* Re-read chunk state after locks — another process may have changed it */
	uncompressed_chunk = ts_chunk_get_by_relid(uncompressed_chunk->table_id, true);
	if (!ts_chunk_is_compressed(uncompressed_chunk) || ts_chunk_is_frozen(uncompressed_chunk))
	{
		ereport(NOTICE,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("chunk \"%s.%s\" is no longer compressed or is frozen, skipping",
						NameStr(uncompressed_chunk->fd.schema_name),
						NameStr(uncompressed_chunk->fd.table_name))));
		return;
	}

	CompressionSettings *chunk_settings = ts_compression_settings_get(uncompressed_chunk->table_id);
	CompressionSettings *ht_settings =
		ts_compression_settings_get(uncompressed_chunk->hypertable_relid);

	/* Orderby changes require recompression, not sparse index rebuild */
	if (!ts_array_equal(chunk_settings->fd.orderby, ht_settings->fd.orderby))
	{
		ereport(NOTICE,
				(errmsg("orderby settings for chunk \"%s.%s\" differ from hypertable \"%s\"",
						NameStr(uncompressed_chunk->fd.schema_name),
						NameStr(uncompressed_chunk->fd.table_name),
						get_rel_name(uncompressed_chunk->hypertable_relid)),
				 errhint("Use compress_chunk(chunk, recompress => true) to recompress.")));
		return;
	}

	if (!force && ts_sparse_index_equal(chunk_settings->fd.index, ht_settings->fd.index))
	{
		ereport(NOTICE,
				(errmsg("sparse index settings for chunk \"%s.%s\" already match hypertable "
						"after acquiring locks, skipping",
						NameStr(uncompressed_chunk->fd.schema_name),
						NameStr(uncompressed_chunk->fd.table_name))));
		return;
	}

	/* Step 1: drop old columns, add new ones, update compression settings */
	List *added_indexes = modify_compressed_table(uncompressed_chunk, force);

	if (added_indexes == NIL)
	{
		return;
	}

	Oid compressed_relid = chunk_settings->fd.compress_relid;

	/* Step 2: initialize builders and decompressor */
	Relation compressed_rel = table_open(compressed_relid, RowExclusiveLock);
	Relation uncompressed_rel = table_open(uncompressed_chunk->table_id, AccessShareLock);
	TupleDesc compressed_desc = RelationGetDescr(compressed_rel);

	bool *repl = palloc0(sizeof(bool) * compressed_desc->natts);
	List *builders =
		create_sparse_index_builders(uncompressed_rel, compressed_relid, added_indexes, repl);

	RowDecompressor decompressor = build_decompressor(compressed_desc,
													  RelationGetDescr(uncompressed_rel),
													  compressed_relid,
													  uncompressed_chunk->table_id);

	/* Step 3: scan, decompress, populate, update */
	populate_sparse_index_columns(compressed_rel, &decompressor, builders, repl);

	row_decompressor_close(&decompressor);
	table_close(uncompressed_rel, AccessShareLock);
	table_close(compressed_rel, RowExclusiveLock);
}
