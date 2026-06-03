/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include "debug_point.h"
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
#include "compression.h"
#include "compression_dml.h"
#include "create.h"
#include "debug_assert.h"
#include "guc.h"
#include "hypertable.h"
#include "indexing.h"
#include "recompress.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/chunk_column_stats.h"
#include "ts_catalog/compression_chunk_size.h"
#include "ts_catalog/compression_settings.h"
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

/*
 * Scan state saved by compact_chunk_find_overlapping_batches.  The caller can
 * inspect the result and then pass the same state to
 * compact_chunk_recompress_overlapping_batches to continue without restarting
 * the scan.
 */
typedef struct CompactChunkScanState
{
	ItemPointerData previous_tid;	   /* TID of the batch processed just before the current one */
	ItemPointerData first_overlap_tid; /* TID of the first overlapping batch */

	/* Segmentby key values of the current batch, for segment-group detection. */
	Datum *seg_values;
	bool *seg_isnull;

	/* First-row and last-row orderby tuples of the current batch, read straight
	 * from the index. */
	Datum *curr_first;
	bool *curr_first_isnull;
	Datum *curr_last;
	bool *curr_last_isnull;

	/* Last-row orderby tuple of the batch processed just before the current
	 * one. Holds copies so it survives advancing the index scan. */
	Datum *prev_last;
	bool *prev_last_isnull;

	bool *isdesc; /* orderby column DESC settings */
} CompactChunkScanState;

static CompactChunkScanState *
compact_chunk_scan_state_init(RecompressContext *recompress_ctx, CompressionSettings *settings)
{
	CompactChunkScanState *state = palloc(sizeof(CompactChunkScanState));
	ItemPointerSetInvalid(&state->previous_tid);
	ItemPointerSetInvalid(&state->first_overlap_tid);
	state->seg_values = palloc(sizeof(Datum) * recompress_ctx->num_segmentby);
	state->seg_isnull = palloc(sizeof(bool) * recompress_ctx->num_segmentby);
	state->curr_first = palloc(sizeof(Datum) * recompress_ctx->num_orderby);
	state->curr_first_isnull = palloc(sizeof(bool) * recompress_ctx->num_orderby);
	state->curr_last = palloc(sizeof(Datum) * recompress_ctx->num_orderby);
	state->curr_last_isnull = palloc(sizeof(bool) * recompress_ctx->num_orderby);
	state->prev_last = palloc0(sizeof(Datum) * recompress_ctx->num_orderby);
	state->prev_last_isnull = palloc(sizeof(bool) * recompress_ctx->num_orderby);
	state->isdesc = palloc(sizeof(bool) * recompress_ctx->num_orderby);
	for (int i = 0; i < recompress_ctx->num_orderby; i++)
	{
		state->prev_last_isnull[i] = true;
		state->isdesc[i] = ts_array_get_element_bool(settings->fd.orderby_desc, i + 1);
	}
	return state;
}

static void
compact_chunk_scan_state_reset(CompactChunkScanState *state, RecompressContext *recompress_ctx)
{
	ItemPointerSetInvalid(&state->previous_tid);
	ItemPointerSetInvalid(&state->first_overlap_tid);
	for (int i = 0; i < recompress_ctx->num_orderby; i++)
	{
		int key = recompress_ctx->num_segmentby + i;
		if (!state->prev_last_isnull[i] && !recompress_ctx->key_byval[key] &&
			PointerIsValid(DatumGetPointer(state->prev_last[i])))
		{
			pfree(DatumGetPointer(state->prev_last[i]));
		}
		state->prev_last[i] = (Datum) 0;
		state->prev_last_isnull[i] = true;
	}
}

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
static IndexScanDesc compact_chunk_begin_index_scan(Relation compressed_chunk_rel,
													Relation index_rel, Snapshot snapshot);
static void read_batch_firstlast(IndexScanDesc index_scan, RecompressContext *recompress_ctx,
								 CompactChunkScanState *state);
static void save_prev_last(CompactChunkScanState *state, RecompressContext *recompress_ctx);
static bool batches_overlap_firstlast(RecompressContext *recompress_ctx, Datum *prev_last,
									  bool *prev_last_isnull, Datum *curr_first,
									  bool *curr_first_isnull);
static void decompress_batch_to_tuplesort(TupleTableSlot *slot, TupleDesc tupdesc,
										  RowDecompressor *decompressor,
										  Tuplesortstate *recompress_tuplesortstate,
										  Tuplesortstate *null_tuplesortstate,
										  Relation compressed_chunk_rel, Snapshot snapshot,
										  AttrNumber first_orderby_attno);
static bool compact_chunk_find_overlapping_batches(Relation compressed_chunk_rel,
												   IndexScanDesc index_scan,
												   RecompressContext *recompress_ctx,
												   CompactChunkScanState *state);
static bool compact_chunk_recompress_overlapping_batches(
	Relation compressed_chunk_rel, IndexScanDesc index_scan, Snapshot snapshot,
	RecompressContext *recompress_ctx, CompactChunkScanState *state, RowCompressor *compressor,
	RowDecompressor *decompressor, Tuplesortstate *recompress_tuplesortstate,
	Tuplesortstate *null_tuplesortstate, BulkWriter *writer, AttrNumber first_orderby_attno);
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
		CompressionSettings *settings = ts_compression_settings_get(uncompressed_chunk_id);
		if (!settings->fd.orderby)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("segmentwise recompression cannot be applied for "
							"compression with no "
							"order by")));
		}
		bool nullable_orderby = !is_chunk_orderby_nonnullable(settings);
		if (nullable_orderby)
		{
			elog(ts_guc_debug_compression_path_info ? INFO : DEBUG1,
				 "in-memory recompression is disabled due to nullable order by, "
				 "performing segmentwise decompress/compress on chunk \"%s.%s\"",
				 NameStr(chunk->fd.schema_name),
				 NameStr(chunk->fd.table_name));
		}
		uncompressed_chunk_id = recompress_chunk_segmentwise_impl(chunk, nullable_orderby);
	}

	PG_RETURN_OID(uncompressed_chunk_id);
}

/*
 * Compact a chunk by recombining overlapping batches
 *
 * 0 uncompressed_chunk_id REGCLASS
 */
Datum
tsl_compact_chunk(PG_FUNCTION_ARGS)
{
	Oid uncompressed_chunk_id = PG_ARGISNULL(0) ? InvalidOid : PG_GETARG_OID(0);

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	TS_PREVENT_FUNC_IF_READ_ONLY();
	Chunk *chunk = ts_chunk_get_by_relid(uncompressed_chunk_id, true);

	if (!ts_chunk_is_compressed(chunk))
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("trying to compact an uncompressed chunk %s.%s",
						NameStr(chunk->fd.schema_name),
						NameStr(chunk->fd.table_name))));
	}

	if (ts_chunk_is_partial(chunk))
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("trying to compact a partially compressed chunk %s.%s",
						NameStr(chunk->fd.schema_name),
						NameStr(chunk->fd.table_name))));
	}

	uncompressed_chunk_id = compact_chunk_impl(chunk);

	PG_RETURN_OID(uncompressed_chunk_id);
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

	/* Cache the sort operator function for each orderby column, used to compare
	 * batch boundary values during compaction. */
	recompress_ctx->orderby_sort_fmgrs = palloc(sizeof(FmgrInfo) * recompress_ctx->num_orderby);
	for (int i = 0; i < recompress_ctx->num_orderby; i++)
	{
		fmgr_info(get_opcode(recompress_ctx->sort_operators[recompress_ctx->num_segmentby + i]),
				  &recompress_ctx->orderby_sort_fmgrs[i]);
	}

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

Oid
recompress_chunk_segmentwise_impl(Chunk *uncompressed_chunk,
								  bool fullrecompress /* do full decompress/compress segmentwise */)
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
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unexpected chunk status %d in chunk %s.%s",
						uncompressed_chunk->fd.status,
						NameStr(uncompressed_chunk->fd.schema_name),
						NameStr(uncompressed_chunk->fd.table_name))));
	}

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

	PG_RETURN_OID(uncompressed_chunk_id);
}

static IndexScanDesc
compact_chunk_begin_index_scan(Relation compressed_chunk_rel, Relation index_rel, Snapshot snapshot)
{
	IndexScanDesc index_scan =
		index_beginscan_compat(compressed_chunk_rel, index_rel, snapshot, NULL, 0, 0);
	/* We use index tuples directly to fetch the values */
	index_scan->xs_want_itup = true;
	index_rescan(index_scan, NULL, 0, NULL, 0);
	return index_scan;
}

/*
 * Read the current batch's segmentby key values and the first-row / last-row
 * orderby tuples from the index tuple into the scan state.
 *
 * Index key order is [segby1, ...segbyN, orderby_lower_1, orderby_upper_1, ...].
 * The index stores a (lower, upper) metadata pair per orderby column. With
 * firstlast metadata the lower/upper columns are the values of that column in
 * the batch's boundary rows: ascending stores (first, last), descending stores
 * (last, first). So curr_first/curr_last are exactly the orderby values in the
 * batch's first and last rows.
 */
static void
read_batch_firstlast(IndexScanDesc index_scan, RecompressContext *recompress_ctx,
					 CompactChunkScanState *state)
{
	for (int i = 0; i < recompress_ctx->num_segmentby; i++)
	{
		state->seg_values[i] = index_getattr(index_scan->xs_itup,
											 AttrOffsetGetAttrNumber(i),
											 index_scan->xs_itupdesc,
											 &state->seg_isnull[i]);
	}

	for (int i = 0; i < recompress_ctx->num_orderby; i++)
	{
		AttrNumber lower = AttrOffsetGetAttrNumber(recompress_ctx->num_segmentby + i * 2);
		AttrNumber upper = lower + 1;
		AttrNumber first_attno = state->isdesc[i] ? upper : lower;
		AttrNumber last_attno = state->isdesc[i] ? lower : upper;

		state->curr_first[i] = index_getattr(index_scan->xs_itup,
											 first_attno,
											 index_scan->xs_itupdesc,
											 &state->curr_first_isnull[i]);
		state->curr_last[i] = index_getattr(index_scan->xs_itup,
											last_attno,
											index_scan->xs_itupdesc,
											&state->curr_last_isnull[i]);
	}
}

/*
 * Remember the current batch's last-row orderby tuple as the predecessor for
 * the next batch. The index tuple is only valid for the current scan position,
 * so pass-by-reference values are deep-copied to survive advancing the scan.
 */
static void
save_prev_last(CompactChunkScanState *state, RecompressContext *recompress_ctx)
{
	for (int i = 0; i < recompress_ctx->num_orderby; i++)
	{
		int key = recompress_ctx->num_segmentby + i;

		if (!state->prev_last_isnull[i] && !recompress_ctx->key_byval[key] &&
			PointerIsValid(DatumGetPointer(state->prev_last[i])))
		{
			pfree(DatumGetPointer(state->prev_last[i]));
		}

		state->prev_last_isnull[i] = state->curr_last_isnull[i];
		state->prev_last[i] = state->curr_last_isnull[i] ?
								  (Datum) 0 :
								  datumCopy(state->curr_last[i],
											recompress_ctx->key_byval[key],
											recompress_ctx->key_typlen[key]);
	}
}

/*
 * Decide whether two adjacent batches overlap from their boundary metadata.
 *
 * Batches are scanned in sort order, so the previous batch's last row and the
 * current batch's first row are the touching boundaries. They overlap when the
 * current batch's first row sorts strictly before the previous batch's last
 * row. A boundary touch (the tuples are equal) is not an overlap.
 *
 * The btree sort operators don't handle NULLs, so NULLs are compared explicitly
 * using each column's NULLS FIRST/LAST setting.
 */
static bool
batches_overlap_firstlast(RecompressContext *recompress_ctx, Datum *prev_last,
						  bool *prev_last_isnull, Datum *curr_first, bool *curr_first_isnull)
{
	for (int i = 0; i < recompress_ctx->num_orderby; i++)
	{
		int key = recompress_ctx->num_segmentby + i;
		bool nulls_first = recompress_ctx->nulls_first[key];
		Oid collation = recompress_ctx->sort_collations[key];
		FmgrInfo *sort_fmgr = &recompress_ctx->orderby_sort_fmgrs[i];

		bool prev_isnull = prev_last_isnull[i];
		bool curr_isnull = curr_first_isnull[i];

		/* Equal on this column, move on to the next one. */
		if (prev_isnull && curr_isnull)
		{
			continue;
		}

		/* prev.last is NULL, curr.first is not. NULLS FIRST puts the NULL before
		 * the non-null (prev before curr, no overlap); NULLS LAST puts it after
		 * (prev after curr, overlap). */
		if (prev_isnull)
		{
			return !nulls_first;
		}

		/* curr.first is NULL, prev.last is not. Mirror of the case above. */
		if (curr_isnull)
		{
			return nulls_first;
		}

		/* Both non-null: compare with the sort operator (< for ASC, > for DESC). */
		if (DatumGetBool(FunctionCall2Coll(sort_fmgr, collation, prev_last[i], curr_first[i])))
		{
			return false; /* prev sorts before curr: no overlap */
		}

		if (DatumGetBool(FunctionCall2Coll(sort_fmgr, collation, curr_first[i], prev_last[i])))
		{
			return true; /* curr sorts before prev: overlap */
		}

		/* Equal on this column, move on to the next one. */
	}

	/* The boundary tuples are equal across all columns: a touch, not an overlap. */
	return false;
}

/*
 * Decompress a compressed batch into tuplesort(s) and delete the original.
 *
 * When null_tuplesortstate is not NULL, rows with NULL in the first orderby
 * column (first_orderby_attno) are routed to null_tuplesortstate so they
 * can be recompressed into a separate batch.  All other rows go to
 * recompress_tuplesortstate.
 *
 * When null_tuplesortstate is NULL, all rows go to recompress_tuplesortstate.
 */
static void
decompress_batch_to_tuplesort(TupleTableSlot *slot, TupleDesc tupdesc,
							  RowDecompressor *decompressor,
							  Tuplesortstate *recompress_tuplesortstate,
							  Tuplesortstate *null_tuplesortstate, Relation compressed_chunk_rel,
							  Snapshot snapshot, AttrNumber first_orderby_attno)
{
	bool should_free;
	HeapTuple compressed_tuple = ExecFetchSlotHeapTuple(slot, false, &should_free);

	heap_deform_tuple(compressed_tuple,
					  tupdesc,
					  decompressor->compressed_datums,
					  decompressor->compressed_is_nulls);

	int n_rows = decompress_batch(decompressor);

	for (int i = 0; i < n_rows; i++)
	{
		TupleTableSlot *row = decompressor->decompressed_slots[i];
		if (null_tuplesortstate)
		{
			bool isnull;
			slot_getattr(row, first_orderby_attno, &isnull);
			if (isnull)
			{
				tuplesort_puttupleslot(null_tuplesortstate, row);
				continue;
			}
		}
		tuplesort_puttupleslot(recompress_tuplesortstate, row);
	}

	row_decompressor_reset(decompressor);

	if (!delete_tuple_for_recompression(compressed_chunk_rel, &slot->tts_tid, snapshot))
	{
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("aborting compaction due to concurrent updates on "
						"compressed data, retrying with next policy run")));
	}

	if (should_free)
	{
		heap_freetuple(compressed_tuple);
	}
}

/*
 * Scan the compressed chunk index in order, looking for the first batch that
 * overlaps its predecessor in the same segment group.
 *
 * Returns true on the first overlap found without modifying the heap, recording
 * the overlapping pair (previous_tid and first_overlap_tid) and the current
 * batch's boundary tuples in state so the recompress pass can resume from here.
 */
static bool
compact_chunk_find_overlapping_batches(Relation compressed_chunk_rel, IndexScanDesc index_scan,
									   RecompressContext *recompress_ctx,
									   CompactChunkScanState *state)
{
	TupleTableSlot *compressed_slot = table_slot_create(compressed_chunk_rel, NULL);

	while (index_getnext_slot(index_scan, ForwardScanDirection, compressed_slot))
	{
		read_batch_firstlast(index_scan, recompress_ctx, state);

		/* First batch overall or first batch of a new segment group: nothing to
		 * compare against, just remember it as the predecessor. */
		if (!ItemPointerIsValid(&state->previous_tid) ||
			check_changed_group(recompress_ctx->current_segment,
								state->seg_values,
								state->seg_isnull,
								recompress_ctx->num_segmentby))
		{
			ItemPointerCopy(&index_scan->xs_heaptid, &state->previous_tid);
			update_current_segment(recompress_ctx->current_segment,
								   state->seg_values,
								   state->seg_isnull,
								   recompress_ctx->num_segmentby);
			save_prev_last(state, recompress_ctx);
			continue;
		}

		if (batches_overlap_firstlast(recompress_ctx,
									  state->prev_last,
									  state->prev_last_isnull,
									  state->curr_first,
									  state->curr_first_isnull))
		{
			ItemPointerCopy(&index_scan->xs_heaptid, &state->first_overlap_tid);
			ExecDropSingleTupleTableSlot(compressed_slot);
			return true;
		}

		/* No overlap: this batch becomes the predecessor for the next one. */
		ItemPointerCopy(&index_scan->xs_heaptid, &state->previous_tid);
		save_prev_last(state, recompress_ctx);
	}

	ExecDropSingleTupleTableSlot(compressed_slot);
	return false;
}

/*
 * Recompress all overlapping batches in the compressed chunk.
 *
 * The caller runs a find pass first, so state->first_overlap_tid and
 * state->previous_tid identify the first overlapping pair. They are fetched by
 * TID and merged, then the main scan loop continues from where the find pass
 * stopped, absorbing further overlapping batches in the same group.
 *
 * Overlapping batches are decompressed into recompress_tuplesortstate and
 * re-compressed once the group ends. When the first orderby column is nullable,
 * NULL rows are routed to null_tuplesortstate so they end up in their own batch
 * (the index orders that batch correctly per NULLS FIRST/LAST).
 *
 * Returns true if any overlapping batches were found and recompressed.
 */
static bool
compact_chunk_recompress_overlapping_batches(
	Relation compressed_chunk_rel, IndexScanDesc index_scan, Snapshot snapshot,
	RecompressContext *recompress_ctx, CompactChunkScanState *state, RowCompressor *compressor,
	RowDecompressor *decompressor, Tuplesortstate *recompress_tuplesortstate,
	Tuplesortstate *null_tuplesortstate, BulkWriter *writer, AttrNumber first_orderby_attno)
{
	TupleTableSlot *previous_compressed_slot = table_slot_create(compressed_chunk_rel, NULL);
	TupleTableSlot *compressed_slot = table_slot_create(compressed_chunk_rel, NULL);

	TupleDesc compressed_rel_tupdesc = RelationGetDescr(compressed_chunk_rel);
	bool has_nullable_orderby = (null_tuplesortstate != NULL);
	bool overlapping = false;
	bool found_overlaps = false;
	bool has_null_rows = false;

	/*
	 * The find pass identified the first overlapping pair. Fetch both batches by
	 * TID and decompress them into the shared tuplesort to start a merge group.
	 */
	if (ItemPointerIsValid(&state->first_overlap_tid))
	{
		bool found pg_attribute_unused();
		bool call_again = false;
		bool all_dead = false;

		found = table_index_fetch_tuple(index_scan->xs_heapfetch,
										&state->first_overlap_tid,
										index_scan->xs_snapshot,
										previous_compressed_slot,
										&call_again,
										&all_dead);
		Assert(found);
		decompress_batch_to_tuplesort(previous_compressed_slot,
									  compressed_rel_tupdesc,
									  decompressor,
									  recompress_tuplesortstate,
									  null_tuplesortstate,
									  compressed_chunk_rel,
									  snapshot,
									  first_orderby_attno);

		found = table_index_fetch_tuple(index_scan->xs_heapfetch,
										&state->previous_tid,
										index_scan->xs_snapshot,
										previous_compressed_slot,
										&call_again,
										&all_dead);
		Assert(found);
		decompress_batch_to_tuplesort(previous_compressed_slot,
									  compressed_rel_tupdesc,
									  decompressor,
									  recompress_tuplesortstate,
									  null_tuplesortstate,
									  compressed_chunk_rel,
									  snapshot,
									  first_orderby_attno);

		if (has_nullable_orderby)
		{
			has_null_rows = true;
		}
		overlapping = true;
		found_overlaps = true;
		CommandCounterIncrement();

		/* The overlapping batch becomes the predecessor for the scan loop. */
		ItemPointerCopy(&state->first_overlap_tid, &state->previous_tid);
		update_current_segment(recompress_ctx->current_segment,
							   state->seg_values,
							   state->seg_isnull,
							   recompress_ctx->num_segmentby);
		save_prev_last(state, recompress_ctx);
	}

	while (index_getnext_slot(index_scan, ForwardScanDirection, compressed_slot))
	{
		read_batch_firstlast(index_scan, recompress_ctx, state);

		/* First batch of a new segment group: close any open merge group and
		 * flush accumulated null rows before starting fresh. */
		if (!ItemPointerIsValid(&state->previous_tid) ||
			check_changed_group(recompress_ctx->current_segment,
								state->seg_values,
								state->seg_isnull,
								recompress_ctx->num_segmentby))
		{
			if (overlapping)
			{
				recompress_segment(recompress_tuplesortstate,
								   compressed_chunk_rel,
								   compressor,
								   writer);
				overlapping = false;
			}
			if (has_null_rows)
			{
				recompress_segment(null_tuplesortstate, compressed_chunk_rel, compressor, writer);
				has_null_rows = false;
			}

			ItemPointerCopy(&index_scan->xs_heaptid, &state->previous_tid);
			update_current_segment(recompress_ctx->current_segment,
								   state->seg_values,
								   state->seg_isnull,
								   recompress_ctx->num_segmentby);
			save_prev_last(state, recompress_ctx);
			continue;
		}

		/* A batch joins the current group when it overlaps its predecessor; the
		 * first batch that no longer overlaps closes the group. */
		bool batch_overlaps = batches_overlap_firstlast(recompress_ctx,
														state->prev_last,
														state->prev_last_isnull,
														state->curr_first,
														state->curr_first_isnull);

		if (batch_overlaps)
		{
			if (!overlapping)
			{
				bool found pg_attribute_unused() =
					table_index_fetch_tuple(index_scan->xs_heapfetch,
											&state->previous_tid,
											index_scan->xs_snapshot,
											previous_compressed_slot,
											&index_scan->xs_heap_continue,
											NULL);
				Assert(found);

				decompress_batch_to_tuplesort(previous_compressed_slot,
											  compressed_rel_tupdesc,
											  decompressor,
											  recompress_tuplesortstate,
											  null_tuplesortstate,
											  compressed_chunk_rel,
											  snapshot,
											  first_orderby_attno);

				if (has_nullable_orderby)
				{
					has_null_rows = true;
				}
				overlapping = true;
				found_overlaps = true;
			}

			decompress_batch_to_tuplesort(compressed_slot,
										  compressed_rel_tupdesc,
										  decompressor,
										  recompress_tuplesortstate,
										  null_tuplesortstate,
										  compressed_chunk_rel,
										  snapshot,
										  first_orderby_attno);

			CommandCounterIncrement();
		}
		else if (overlapping)
		{
			/* This batch no longer overlaps the group; recompress what we
			 * gathered and let it start a fresh comparison. */
			recompress_segment(recompress_tuplesortstate, compressed_chunk_rel, compressor, writer);
			overlapping = false;
			CommandCounterIncrement();
		}

		ItemPointerCopy(&index_scan->xs_heaptid, &state->previous_tid);
		save_prev_last(state, recompress_ctx);
	}

	if (overlapping)
	{
		recompress_segment(recompress_tuplesortstate, compressed_chunk_rel, compressor, writer);
	}

	/* Flush any remaining null rows gathered from overlap merges. */
	if (has_null_rows)
	{
		recompress_segment(null_tuplesortstate, compressed_chunk_rel, compressor, writer);
	}

	ExecDropSingleTupleTableSlot(previous_compressed_slot);
	ExecDropSingleTupleTableSlot(compressed_slot);

	return found_overlaps;
}

Oid
compact_chunk_impl(Chunk *uncompressed_chunk)
{
	Oid uncompressed_chunk_id = uncompressed_chunk->table_id;

	if (!ts_chunk_is_compressed(uncompressed_chunk))
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unexpected chunk status %d in chunk %s.%s",
						uncompressed_chunk->fd.status,
						NameStr(uncompressed_chunk->fd.schema_name),
						NameStr(uncompressed_chunk->fd.table_name))));
	}

	Chunk *compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);
	Ensure(compressed_chunk != NULL,
		   "compressed chunk not found for chunk \"%s\"",
		   get_rel_name(uncompressed_chunk->table_id));

	ereport(DEBUG1,
			(errmsg("acquiring locks for recompression: \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));

	/* Taking a ShareExclusiveLock on compressed chunk mostly to block DDL,
	 * this could potentially be a RowExclusiveLock with enough testing.
	 *
	 * For uncompressed chunk, we just need to read so AccessShareLock is fine.
	 */
	Relation uncompressed_chunk_rel = table_open(uncompressed_chunk->table_id, AccessShareLock);
	Relation compressed_chunk_rel =
		table_open(compressed_chunk->table_id, ShareUpdateExclusiveLock);

	int count;
	LOCKTAG locktag;
	SET_LOCKTAG_RELATION(locktag, MyDatabaseId, uncompressed_chunk_id);

	/* Check if any backends currently hold locks on the uncompressed chunk
	 * that would conflict with ExclusiveLock. This detects concurrent DML
	 * (which holds RowExclusiveLock) without actually acquiring ExclusiveLock
	 * ourselves. If conflicts exist, we skip compaction to avoid blocking. */
	GetLockConflicts(&locktag, ExclusiveLock, &count);

	if (count > 0)
	{
		elog(WARNING,
			 "delaying compaction on chunk %s.%s due to concurrent DML",
			 NameStr(uncompressed_chunk->fd.schema_name),
			 NameStr(uncompressed_chunk->fd.table_name));

		/* Safe to drop the lock, we didn't change anything */
		table_close(uncompressed_chunk_rel, NoLock);
		table_close(compressed_chunk_rel, NoLock);

		return uncompressed_chunk_id;
	}

	TupleDesc uncompressed_rel_tupdesc = RelationGetDescr(uncompressed_chunk_rel);
	CompressionSettings *settings = ts_compression_settings_get(uncompressed_chunk->table_id);

	/*
	 * Check if first orderby column is nullable. We need
	 * additional null-handling logic during compaction if so.
	 */
	int num_orderby = ts_array_length(settings->fd.orderby);
	AttrNumber first_orderby_attno = InvalidAttrNumber;
	bool has_nullable_orderby = false;

	if (num_orderby > 0)
	{
		first_orderby_attno = get_attnum(uncompressed_chunk_rel->rd_id,
										 ts_array_get_element_text(settings->fd.orderby, 1));
		has_nullable_orderby =
			!TupleDescAttr(uncompressed_rel_tupdesc, AttrNumberGetAttrOffset(first_orderby_attno))
				 ->attnotnull;
	}

	/*
	 * Compaction reads each batch's exact boundary rows from the firstlast
	 * sparse metadata. Skip chunks whose orderby columns lack it, which is only
	 * possible with a legacy sparse index configuration.
	 */
	for (int pos = 1; pos <= num_orderby; pos++)
	{
		if (orderby_sparse_kind(settings, pos) != ORDERBY_SPARSE_FIRSTLAST)
		{
			ereport(WARNING,
					(errmsg("skipping compaction on chunk %s.%s",
							NameStr(uncompressed_chunk->fd.schema_name),
							NameStr(uncompressed_chunk->fd.table_name)),
					 errdetail("Orderby column \"%s\" has no firstlast sparse index.",
							   ts_array_get_element_text(settings->fd.orderby, pos)),
					 errhint("Recompress the chunk to add firstlast sparse index metadata for its "
							 "orderby columns.")));
			table_close(uncompressed_chunk_rel, NoLock);
			table_close(compressed_chunk_rel, NoLock);
			return uncompressed_chunk_id;
		}
	}

	BulkWriter writer = bulk_writer_build(compressed_chunk_rel, 0);
	Oid index_oid = get_compressed_chunk_index(writer.indexstate, settings);
	Relation index_rel = index_open(index_oid, RowExclusiveLock);
	ereport(DEBUG1,
			(errmsg("locks acquired for compaction: \"%s.%s\"",
					NameStr(uncompressed_chunk->fd.schema_name),
					NameStr(uncompressed_chunk->fd.table_name))));

	RecompressContext *recompress_ctx =
		compress_chunk_populate_recompress_ctx(settings,
											   uncompressed_chunk_rel,
											   compressed_chunk_rel,
											   index_rel,
											   true);

	Snapshot snapshot = RegisterSnapshot(GetTransactionSnapshot());
	IndexScanDesc index_scan =
		compact_chunk_begin_index_scan(compressed_chunk_rel, index_rel, snapshot);

	CompactChunkScanState *state = compact_chunk_scan_state_init(recompress_ctx, settings);

	bool found_overlaps = compact_chunk_find_overlapping_batches(compressed_chunk_rel,
																 index_scan,
																 recompress_ctx,
																 state);

	if (found_overlaps)
	{
		/* Recompress the overlaps */
		RowCompressor compressor;
		RowDecompressor decompressor;
		Tuplesortstate *recompress_tuplesortstate;
		Tuplesortstate *null_tuplesortstate = NULL;

		row_compressor_init(&compressor,
							settings,
							RelationGetDescr(uncompressed_chunk_rel),
							RelationGetDescr(compressed_chunk_rel));
		decompressor = build_decompressor(RelationGetDescr(compressed_chunk_rel),
										  RelationGetDescr(uncompressed_chunk_rel),
										  RelationGetRelid(compressed_chunk_rel),
										  RelationGetRelid(uncompressed_chunk_rel));
		/* Used for gathering and resorting the tuples that should be recompressed together.
		 * Since we are working on a per-segment level here, we only need to sort them
		 * based on the orderby settings.
		 */
		recompress_tuplesortstate =
			tuplesort_begin_heap(uncompressed_rel_tupdesc,
								 recompress_ctx->num_orderby,
								 &recompress_ctx->sort_keys[recompress_ctx->num_segmentby],
								 &recompress_ctx->sort_operators[recompress_ctx->num_segmentby],
								 &recompress_ctx->sort_collations[recompress_ctx->num_segmentby],
								 &recompress_ctx->nulls_first[recompress_ctx->num_segmentby],
								 maintenance_work_mem,
								 NULL,
								 false);

		if (has_nullable_orderby)
		{
			null_tuplesortstate =
				tuplesort_begin_heap(uncompressed_rel_tupdesc,
									 recompress_ctx->num_orderby,
									 &recompress_ctx->sort_keys[recompress_ctx->num_segmentby],
									 &recompress_ctx->sort_operators[recompress_ctx->num_segmentby],
									 &recompress_ctx
										  ->sort_collations[recompress_ctx->num_segmentby],
									 &recompress_ctx->nulls_first[recompress_ctx->num_segmentby],
									 maintenance_work_mem,
									 NULL,
									 false);
		}

		compact_chunk_recompress_overlapping_batches(compressed_chunk_rel,
													 index_scan,
													 snapshot,
													 recompress_ctx,
													 state,
													 &compressor,
													 &decompressor,
													 recompress_tuplesortstate,
													 null_tuplesortstate,
													 &writer,
													 first_orderby_attno);
		row_compressor_close(&compressor);
		row_decompressor_close(&decompressor);
		tuplesort_end(recompress_tuplesortstate);
		if (null_tuplesortstate)
		{
			tuplesort_end(null_tuplesortstate);
		}
	}

	/* At this point, we have resolved all the overlaps.
	 * Try to switch the chunk status if we can get the exclusive lock
	 */
	if (ConditionalLockRelation(compressed_chunk_rel, ExclusiveLock))
	{
		/*
		 * Use a fresh snapshot for the verification scan. If recompression
		 * happened, the original snapshot predates the CommandCounterIncrement()
		 * calls made during recompression, so it would still see the deleted
		 * batches and miss the newly inserted ones. A fresh snapshot correctly
		 * reflects the post-recompression state.
		 */
		index_endscan(index_scan);
		UnregisterSnapshot(snapshot);
		snapshot = RegisterSnapshot(GetTransactionSnapshot());
		index_scan = compact_chunk_begin_index_scan(compressed_chunk_rel, index_rel, snapshot);
		compact_chunk_scan_state_reset(state, recompress_ctx);
		found_overlaps = compact_chunk_find_overlapping_batches(compressed_chunk_rel,
																index_scan,
																recompress_ctx,
																state);
		if (!found_overlaps)
		{
			/*
			 * Only clear UNORDERED status from chunk.
			 */
			if (ts_chunk_clear_status(uncompressed_chunk, CHUNK_STATUS_COMPRESSED_UNORDERED))
			{
				ereport(DEBUG1,
						(errmsg("cleared unordered chunk status for compaction: \"%s.%s\"",
								NameStr(uncompressed_chunk->fd.schema_name),
								NameStr(uncompressed_chunk->fd.table_name))));
			}

			/* changed chunk status, so invalidate any plans involving this chunk */
			CacheInvalidateRelcacheByRelid(uncompressed_chunk->table_id);
		}
	}

	index_endscan(index_scan);
	UnregisterSnapshot(snapshot);
	index_close(index_rel, NoLock);

	bulk_writer_close(&writer);

	free_chunk_recompress_ctx(recompress_ctx);

	table_close(uncompressed_chunk_rel, NoLock);
	table_close(compressed_chunk_rel, NoLock);

	return uncompressed_chunk_id;
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

	Chunk *compressed_chunk = ts_chunk_get_by_id(uncompressed_chunk->fd.compressed_chunk_id, true);
	Ensure(compressed_chunk != NULL,
		   "compressed chunk not found for chunk \"%s\"",
		   get_rel_name(uncompressed_chunk->table_id));

	CompressionSettings *settings = ts_compression_settings_get(uncompressed_chunk->table_id);
	Ensure(settings != NULL,
		   "compression settings not found for chunk \"%s\"",
		   get_rel_name(uncompressed_chunk->table_id));

	Ensure(settings->fd.orderby, "empty order by, cannot recompress in-memory");

	LOCKMODE lockmode = ExclusiveLock;
	Relation uncompressed_chunk_rel = table_open(uncompressed_chunk->table_id, lockmode);
	Relation compressed_chunk_rel = table_open(compressed_chunk->table_id, lockmode);

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
	LockRelationOid(compressed_chunk->table_id, AccessExclusiveLock);
	ts_chunk_drop(compressed_chunk, DROP_RESTRICT, -1);
	if (ts_chunk_clear_status(uncompressed_chunk, CHUNK_STATUS_COMPRESSED_UNORDERED))
	{
		ereport(DEBUG1,
				(errmsg("cleared chunk status for recompression: \"%s.%s\"",
						NameStr(uncompressed_chunk->fd.schema_name),
						NameStr(uncompressed_chunk->fd.table_name))));
	}
	ts_chunk_set_compressed_chunk(uncompressed_chunk, new_compressed_chunk->fd.id);

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
		if (!slot_key_test(compressed_slot, key))
		{
			return handle_null_scan(key->sk_flags, nulls_first[0], Tuple_before);
		}

		key = &orderby_scankeys[1];
		if (!slot_key_test(compressed_slot, key))
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
