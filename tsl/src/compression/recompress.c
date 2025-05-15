/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include "debug_point.h"
#include <parser/parse_coerce.h>
#include <parser/parse_relation.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/snapmgr.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "compression.h"
#include "compression_dml.h"
#include "create.h"
#include "debug_assert.h"
#include "guc.h"
#include "hypercore/hypercore_handler.h"
#include "hypercore/utils.h"
#include "indexing.h"
#include "recompress.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/chunk_column_stats.h"
#include "ts_catalog/compression_settings.h"

/*
 * Timing parameters for spin locking heuristics.
 * These are the same as used by Postgres for truncate locking during lazy vacuum.
 * https://github.com/postgres/postgres/blob/4a0650d359c5981270039eeb634c3b7427aa0af5/src/backend/access/heap/vacuumlazy.c#L82
 */
#define RECOMPRESS_EXCLUSIVE_LOCK_WAIT_INTERVAL 50 /* ms */
#define RECOMPRESS_EXCLUSIVE_LOCK_TIMEOUT 5000	   /* ms */

static bool fetch_uncompressed_chunk_into_tuplesort(Tuplesortstate *tuplesortstate,
													Relation uncompressed_chunk_rel,
													Snapshot snapshot);
static bool delete_tuple_for_recompression(Relation rel, ItemPointer tid, Snapshot snapshot);
static void update_current_segment(CompressedSegmentInfo *current_segment, TupleTableSlot *slot,
								   int nsegmentby_cols);
static void create_segmentby_scankeys(CompressionSettings *settings, Relation index_rel,
									  Relation compressed_chunk_rel, ScanKeyData *index_scankeys);
static void create_orderby_scankeys(CompressionSettings *settings, Relation index_rel,
									Relation compressed_chunk_rel, ScanKeyData *orderby_scankeys);
static void update_segmentby_scankeys(TupleTableSlot *uncompressed_slot,
									  CompressedSegmentInfo *current_segment, int num_segmentby,
									  ScanKey index_scankeys);
static void update_orderby_scankeys(TupleTableSlot *uncompressed_slot,
									CompressedSegmentInfo *current_segment, int num_segmentby,
									int num_orderby, ScanKey orderby_scankeys);
static enum Batch_match_result match_tuple_batch(TupleTableSlot *compressed_slot, int num_orderby,
												 ScanKey orderby_scankeys, bool *nulls_first);
static bool check_changed_group(CompressedSegmentInfo *current_segment, TupleTableSlot *slot,
								int nsegmentby_cols);
static void recompress_segment(Tuplesortstate *tuplesortstate, Relation compressed_chunk_rel,
							   RowCompressor *row_compressor);
static void try_updating_chunk_status(Chunk *uncompressed_chunk, Relation uncompressed_chunk_rel);

/*
 * Recompress an existing chunk by decompressing the batches
 * that are affected by the addition of newer data. The existing
 * compressed chunk will not be recreated but modified in place.
 *
 * 0 uncompressed_chunk_id REGCLASS
 * 1 if_not_compressed BOOL = false
 */
Datum
tsl_recompress_chunk_segmentwise(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);
	bool if_not_compressed = PG_ARGISNULL(1) ? true : PG_GETARG_BOOL(1);

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

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
		CompressionSettings *settings = ts_compression_settings_get(uncompressed_chunk_id);
		if (!settings->fd.orderby)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("segmentwise recompression cannot be applied for "
							"compression with no "
							"order by")));
		}
		uncompressed_chunk_id = recompress_chunk_segmentwise_impl(chunk);
	}

	PG_RETURN_OID(uncompressed_chunk_id);
}

Oid
recompress_chunk_segmentwise_impl(Chunk *uncompressed_chunk)
{
	Oid uncompressed_chunk_id = uncompressed_chunk->table_id;

	/*
	 * only proceed if status in (3, 9, 11)
	 * 1: compressed
	 * 2: compressed_unordered
	 * 4: frozen
	 * 8: compressed_partial
	 */
	if (!ts_chunk_is_compressed(uncompressed_chunk) && ts_chunk_is_partial(uncompressed_chunk))
		elog(ERROR,
			 "unexpected chunk status %d in chunk %s.%s",
			 uncompressed_chunk->fd.status,
			 NameStr(uncompressed_chunk->fd.schema_name),
			 NameStr(uncompressed_chunk->fd.table_name));

	/* need it to find the segby cols from the catalog */
	Chunk *compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);
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
	Relation compressed_chunk_rel = table_open(compressed_chunk->table_id, recompression_lockmode);

	bool has_unique_constraints =
		ts_indexing_relation_has_primary_or_unique_index(uncompressed_chunk_rel);
	int count;
	LOCKTAG locktag;
	SET_LOCKTAG_RELATION(locktag, MyDatabaseId, uncompressed_chunk_id);

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

			PG_RETURN_OID(uncompressed_chunk_id);
		}
	}

	/*
	 * Calculate and add the column dimension ranges for the src chunk used by chunk skipping
	 * feature. This has to be done before the compression. In case of recompression, the logic will
	 * get the min/max entries for the uncompressed portion and reconcile and update the existing
	 * entry for ht/chunk/column combination. This case handles:
	 *
	 * * INSERTs into uncompressed chunk
	 * * UPDATEs into uncompressed chunk
	 *
	 * In case of DELETEs, the entries won't exist in the uncompressed chunk, but since
	 * we are deleting, we will stay within the earlier computed max/min range. This
	 * means that the chunk will not get pruned for a larger range of values. This will
	 * work ok enough if only a few of the compressed chunks get DELETEs down the line.
	 * In the future, we can look at computing min/max entries in the compressed chunk
	 * using the batch metadata and then recompute the range to handle DELETE cases.
	 */
	Hypertable *ht = ts_hypertable_get_by_id(uncompressed_chunk->fd.hypertable_id);
	if (ht->range_space)
		ts_chunk_column_stats_calculate(ht, uncompressed_chunk);

	TupleDesc compressed_rel_tupdesc = RelationGetDescr(compressed_chunk_rel);
	TupleDesc uncompressed_rel_tupdesc = RelationGetDescr(uncompressed_chunk_rel);

	int num_segmentby = ts_array_length(settings->fd.segmentby);
	int num_orderby = ts_array_length(settings->fd.orderby);
	int n_keys = num_segmentby + num_orderby;

	AttrNumber *sort_keys = palloc(sizeof(*sort_keys) * n_keys);
	Oid *sort_operators = palloc(sizeof(*sort_operators) * n_keys);
	Oid *sort_collations = palloc(sizeof(*sort_collations) * n_keys);
	bool *nulls_first = palloc(sizeof(*nulls_first) * n_keys);

	CompressedSegmentInfo *current_segment = palloc0(sizeof(CompressedSegmentInfo) * n_keys);

	for (int n = 0; n < n_keys; n++)
	{
		const char *attname;
		int position;
		if (n < num_segmentby)
		{
			position = n + 1;
			attname = ts_array_get_element_text(settings->fd.segmentby, position);
		}
		else
		{
			position = n - num_segmentby + 1;
			attname = ts_array_get_element_text(settings->fd.orderby, position);
		}

		AttrNumber col_attno = get_attnum(uncompressed_chunk_rel->rd_id, attname);
		current_segment[n].decompressed_chunk_offset = AttrNumberGetAttrOffset(col_attno);
		current_segment[n].segment_info = segment_info_new(
			TupleDescAttr(uncompressed_rel_tupdesc, current_segment[n].decompressed_chunk_offset));

		compress_chunk_populate_sort_info_for_column(settings,
													 RelationGetRelid(uncompressed_chunk_rel),
													 attname,
													 &sort_keys[n],
													 &sort_operators[n],
													 &sort_collations[n],
													 &nulls_first[n]);
	}

	/******************** row decompressor **************/

	RowDecompressor decompressor = build_decompressor(compressed_chunk_rel, uncompressed_chunk_rel);
	/********** row compressor *******************/
	RowCompressor row_compressor;
	row_compressor_init(settings,
						&row_compressor,
						RelationGetDescr(uncompressed_chunk_rel),
						compressed_chunk_rel,
						true /*need_bistate*/,
						0 /*insert options*/);

	/* For chunks with no segmentby settings, we can still do segmentwise recompression
	 * The entire chunk is treated as a single segment
	 */
	elog(ts_guc_debug_compression_path_info ? INFO : DEBUG1,
		 "Using index \"%s\" for recompression",
		 get_rel_name(row_compressor.index_oid));

	LOCKMODE index_lockmode =
		ts_guc_enable_exclusive_locking_recompression ? ExclusiveLock : RowExclusiveLock;
	Relation index_rel = index_open(row_compressor.index_oid, index_lockmode);
	ereport(DEBUG1,
			(errmsg("locks acquired for recompression: \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));

	/* Setting up scankeys */
	ScanKeyData *index_scankeys = palloc(sizeof(ScanKeyData) * num_segmentby);
	ScanKeyData *orderby_scankeys = palloc(sizeof(ScanKeyData) * num_orderby * 2);
	create_segmentby_scankeys(settings, index_rel, compressed_chunk_rel, index_scankeys);
	create_orderby_scankeys(settings, index_rel, compressed_chunk_rel, orderby_scankeys);

	/* Used for sorting and iterating over all the uncompressed tuples that have
	 * to be recompressed. These tuples are sorted based on the segmentby and
	 * orderby settings.
	 */
	Tuplesortstate *input_tuplesortstate = tuplesort_begin_heap(uncompressed_rel_tupdesc,
																n_keys,
																sort_keys,
																sort_operators,
																sort_collations,
																nulls_first,
																maintenance_work_mem,
																NULL,
																false);

	/* Used for gathering and resorting the tuples that should be recompressed together.
	 * Since we are working on a per-segment level here, we only need to sort them
	 * based on the orderby settings.
	 */
	Tuplesortstate *recompress_tuplesortstate =
		tuplesort_begin_heap(uncompressed_rel_tupdesc,
							 num_orderby,
							 &sort_keys[num_segmentby],
							 &sort_operators[num_segmentby],
							 &sort_collations[num_segmentby],
							 &nulls_first[num_segmentby],
							 maintenance_work_mem,
							 NULL,
							 false);

	/************** snapshot ****************************/
	Snapshot snapshot = RegisterSnapshot(GetTransactionSnapshot());

	TupleTableSlot *uncompressed_slot =
		MakeTupleTableSlot(uncompressed_rel_tupdesc, &TTSOpsMinimalTuple);
	TupleTableSlot *compressed_slot = table_slot_create(compressed_chunk_rel, NULL);

	HeapTuple compressed_tuple;
	IndexScanDesc index_scan =
		index_beginscan(compressed_chunk_rel, index_rel, snapshot, num_segmentby, 0);

	bool found_tuple = fetch_uncompressed_chunk_into_tuplesort(input_tuplesortstate,
															   uncompressed_chunk_rel,
															   snapshot);
	if (!found_tuple)
		goto finish;
	tuplesort_performsort(input_tuplesortstate);

	for (found_tuple = tuplesort_gettupleslot(input_tuplesortstate,
											  true /*=forward*/,
											  false /*=copy*/,
											  uncompressed_slot,
											  NULL /*=abbrev*/);
		 found_tuple;)
	{
		CHECK_FOR_INTERRUPTS();

		update_current_segment(current_segment, uncompressed_slot, num_segmentby);

		/* Build scankeys based on uncompressed tuple values */
		update_segmentby_scankeys(uncompressed_slot,
								  current_segment,
								  num_segmentby,
								  index_scankeys);

		update_orderby_scankeys(uncompressed_slot,
								current_segment,
								num_segmentby,
								num_orderby,
								orderby_scankeys);

		index_rescan(index_scan, index_scankeys, num_segmentby, NULL, 0);

		bool done_with_segment = false;
		bool tuples_for_recompression = false;
		enum Batch_match_result result;

		while (index_getnext_slot(index_scan, ForwardScanDirection, compressed_slot))
		{
			/* Check if the uncompressed tuple is before, inside, or after the compressed batch */
			result = match_tuple_batch(compressed_slot,
									   num_orderby,
									   orderby_scankeys,
									   &nulls_first[num_segmentby]);

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
				found_tuple = tuplesort_gettupleslot(input_tuplesortstate,
													 true /*=forward*/,
													 false /*=copy*/,
													 uncompressed_slot,
													 NULL /*=abbrev*/);
				/* If we happen to hit the end of uncompressed tuples or tuple changed segment group
				 * we are done with the segment group
				 */
				if (!found_tuple ||
					check_changed_group(current_segment, uncompressed_slot, num_segmentby))
				{
					done_with_segment = true;
					break;
				}

				slot_getallattrs(uncompressed_slot);

				update_orderby_scankeys(uncompressed_slot,
										current_segment,
										num_segmentby,
										num_orderby,
										orderby_scankeys);
				result = match_tuple_batch(compressed_slot,
										   num_orderby,
										   orderby_scankeys,
										   &nulls_first[num_segmentby]);
			}

			/* If we are done with segment, recompress everything we have so far
			 * and break out of this segment index scan
			 */
			if (done_with_segment)
			{
				tuples_for_recompression = false;
				recompress_segment(recompress_tuplesortstate,
								   uncompressed_chunk_rel,
								   &row_compressor);
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
					ereport(ERROR,
							(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							 errmsg("aborting recompression due to concurrent updates on "
									"compressed data, retrying with next policy run")));
				CommandCounterIncrement();

				if (should_free)
					heap_freetuple(compressed_tuple);

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
								   &row_compressor);
			}
		}

		/* End if we are finished with all uncompressed tuples */
		if (!found_tuple)
		{
			break;
		}

		/* Reset index scan if we are done with with this segment */
		if (done_with_segment)
		{
			continue;
		}

		/* We are done with existing batches for this segment group
		 * Everything after this point goes into new batches
		 * until we hit a new segment group or exhaust the uncompressed tuples
		 */
		while (!check_changed_group(current_segment, uncompressed_slot, num_segmentby))
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
								   &row_compressor);
				break;
			}

			slot_getallattrs(uncompressed_slot);
		}

		if (tuples_for_recompression)
		{
			recompress_segment(recompress_tuplesortstate, uncompressed_chunk_rel, &row_compressor);
		}
	}

finish:
	row_compressor_close(&row_compressor);
	ExecDropSingleTupleTableSlot(uncompressed_slot);
	ExecDropSingleTupleTableSlot(compressed_slot);
	index_endscan(index_scan);
	UnregisterSnapshot(snapshot);
	index_close(index_rel, NoLock);
	row_decompressor_close(&decompressor);

	tuplesort_end(input_tuplesortstate);
	tuplesort_end(recompress_tuplesortstate);

	pfree(current_segment);
	pfree(index_scankeys);
	pfree(orderby_scankeys);

	/* Need to rebuild indexes if the relation is using hypercore
	 * TAM. Alternatively, we could insert into indexes when inserting into
	 * the compressed rel. */
	if (uncompressed_chunk_rel->rd_tableam == hypercore_routine())
	{
		ReindexParams params = {
			.options = 0,
			.tablespaceOid = InvalidOid,
		};

#if PG17_GE
		reindex_relation(NULL, RelationGetRelid(uncompressed_chunk_rel), 0, &params);
#else
		reindex_relation(RelationGetRelid(uncompressed_chunk_rel), 0, &params);
#endif
	}

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

	PG_RETURN_OID(uncompressed_chunk_id);
}

static void
update_segmentby_scankeys(TupleTableSlot *uncompressed_slot, CompressedSegmentInfo *current_segment,
						  int num_segmentby, ScanKey index_scankeys)
{
	Datum val;
	bool is_null;
	for (int i = 0; i < num_segmentby; i++)
	{
		AttrNumber in_attnum =
			AttrOffsetGetAttrNumber(current_segment[i].decompressed_chunk_offset);
		val = slot_getattr(uncompressed_slot, in_attnum, &is_null);
		index_scankeys[i].sk_flags = is_null ? SK_ISNULL | SK_SEARCHNULL : 0;
		index_scankeys[i].sk_argument = val;
	}
}

static void
update_orderby_scankeys(TupleTableSlot *uncompressed_slot, CompressedSegmentInfo *current_segment,
						int num_segmentby, int num_orderby, ScanKey orderby_scankeys)
{
	int min_index, max_index;
	Datum val;
	bool is_null;
	for (int i = 0; i < num_orderby; i++)
	{
		AttrNumber in_attnum =
			AttrOffsetGetAttrNumber(current_segment[num_segmentby + i].decompressed_chunk_offset);
		val = slot_getattr(uncompressed_slot, in_attnum, &is_null);
		min_index = i * 2;
		max_index = min_index + 1;
		orderby_scankeys[min_index].sk_flags = is_null ? SK_ISNULL : 0;
		orderby_scankeys[min_index].sk_argument = val;
		orderby_scankeys[max_index].sk_flags = is_null ? SK_ISNULL : 0;
		orderby_scankeys[max_index].sk_argument = val;
	}
}

static enum Batch_match_result
handle_null_scan(int key_flags, bool nulls_first, enum Batch_match_result result)
{
	if (key_flags & SK_ISNULL)
		return nulls_first ? Tuple_before : Tuple_after;

	return result;
}

static enum Batch_match_result
match_tuple_batch(TupleTableSlot *compressed_slot, int num_orderby, ScanKey orderby_scankeys,
				  bool *nulls_first)
{
	ScanKey key;
	for (int i = 0; i < num_orderby; i++)
	{
		key = &orderby_scankeys[i * 2];
		if (!slot_key_test(compressed_slot, key))
			return handle_null_scan(key->sk_flags, nulls_first[i], Tuple_before);

		key = &orderby_scankeys[i * 2 + 1];
		if (!slot_key_test(compressed_slot, key))
			return handle_null_scan(key->sk_flags, nulls_first[i], Tuple_after);
	}

	return Tuple_match;
}

static bool
fetch_uncompressed_chunk_into_tuplesort(Tuplesortstate *tuplesortstate,
										Relation uncompressed_chunk_rel, Snapshot snapshot)
{
	bool matching_exist = false;
	/* Let compression TAM know it should only return tuples from the
	 * non-compressed relation. */

	TableScanDesc scan = table_beginscan(uncompressed_chunk_rel, snapshot, 0, 0);
	hypercore_scan_set_skip_compressed(scan, true);
	TupleTableSlot *slot = table_slot_create(uncompressed_chunk_rel, NULL);

	while (table_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		matching_exist = true;
		slot_getallattrs(slot);
		tuplesort_puttupleslot(tuplesortstate, slot);
		if (!delete_tuple_for_recompression(uncompressed_chunk_rel, &slot->tts_tid, snapshot))
			ereport(ERROR,
					(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
					 errmsg("aborting recompression due to concurrent updates on "
							"uncompressed data, retrying with next policy run")));
	}
	ExecDropSingleTupleTableSlot(slot);
	table_endscan(scan);

	return matching_exist;
}

/* Sort the tuples and recompress them */
static void
recompress_segment(Tuplesortstate *tuplesortstate, Relation compressed_chunk_rel,
				   RowCompressor *row_compressor)
{
	tuplesort_performsort(tuplesortstate);
	row_compressor_reset(row_compressor);
	row_compressor_append_sorted_rows(row_compressor,
									  tuplesortstate,
									  RelationGetDescr(compressed_chunk_rel),
									  compressed_chunk_rel);
	tuplesort_reset(tuplesortstate);
	CommandCounterIncrement();
}

static void
update_current_segment(CompressedSegmentInfo *current_segment, TupleTableSlot *slot,
					   int nsegmentby_cols)
{
	Datum val;
	bool is_null;
	CompressedSegmentInfo curr;
	for (int i = 0; i < nsegmentby_cols; i++)
	{
		curr = current_segment[i];
		val = slot_getattr(slot, AttrOffsetGetAttrNumber(curr.decompressed_chunk_offset), &is_null);
		/* new segment, need to do per-segment processing */
		segment_info_update(curr.segment_info, val, is_null);
	}
}

static bool
check_changed_group(CompressedSegmentInfo *current_segment, TupleTableSlot *slot,
					int nsegmentby_cols)
{
	Datum val;
	bool is_null;
	bool changed_segment = false;
	CompressedSegmentInfo curr;
	for (int i = 0; i < nsegmentby_cols; i++)
	{
		curr = current_segment[i];
		val = slot_getattr(slot, AttrOffsetGetAttrNumber(curr.decompressed_chunk_offset), &is_null);
		if (!segment_info_datum_is_in_group(curr.segment_info, val, is_null))
		{
			changed_segment = true;
			break;
		}
	}
	return changed_segment;
}

static void
init_scankey(ScanKey sk, AttrNumber attnum, Oid atttypid, Oid attcollid, StrategyNumber strategy)
{
	TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_BTREE_OPFAMILY);
	if (!OidIsValid(tce->btree_opf))
		elog(ERROR, "no btree opfamily for type \"%s\"", format_type_be(atttypid));

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
		elog(ERROR, "no operator for type \"%s\"", format_type_be(atttypid));

	opr = get_opcode(opr);
	if (!OidIsValid(opr))
		elog(ERROR, "no opcode for type \"%s\"", format_type_be(atttypid));

	ScanKeyEntryInitialize(sk,
						   0, /* flags */
						   attnum,
						   strategy,
						   InvalidOid, /* No strategy subtype. */
						   attcollid,
						   opr,
						   (Datum) 0);
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
	/* Create two scankeys per orderby column, for min and max metadata columns respectively */
	for (int i = 0; i < num_orderby * 2; i = i + 2)
	{
		position = (i / 2) + 1;
		AttrNumber first_attno =
			get_attnum(compressed_chunk_rel->rd_id, column_segment_min_name(position));
		StrategyNumber first_strategy = BTLessEqualStrategyNumber;
		AttrNumber second_attno =
			get_attnum(compressed_chunk_rel->rd_id, column_segment_max_name(position));
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
	TableScanDesc scan = table_beginscan(uncompressed_chunk_rel, GetLatestSnapshot(), 0, 0);
	hypercore_scan_set_skip_compressed(scan, true);
	ScanDirection scan_dir = uncompressed_chunk_rel->rd_tableam == hypercore_routine() ?
								 ForwardScanDirection :
								 BackwardScanDirection;
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

	if (!has_tuples)
	{
		if (ts_chunk_clear_status(uncompressed_chunk,
								  CHUNK_STATUS_COMPRESSED_UNORDERED |
									  CHUNK_STATUS_COMPRESSED_PARTIAL))
			ereport(DEBUG1,
					(errmsg("cleared chunk status for recompression: \"%s.%s\"",
							NameStr(uncompressed_chunk->fd.schema_name),
							NameStr(uncompressed_chunk->fd.table_name))));

		/* changed chunk status, so invalidate any plans involving this chunk */
		CacheInvalidateRelcacheByRelid(uncompressed_chunk->table_id);
	}
}
