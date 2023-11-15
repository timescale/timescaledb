/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/heapam.h>
#include <access/hio.h>
#include <access/skey.h>
#include <access/tableam.h>
#include <catalog/index.h>
#include <catalog/pg_attribute.h>
#include <catalog/storage.h>
#include <commands/vacuum.h>
#include <executor/tuptable.h>
#include <nodes/bitmapset.h>
#include <nodes/execnodes.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>
#include <optimizer/pathnode.h>
#include <parser/parsetree.h>
#include <pgstat.h>
#include <postgres_ext.h>
#include <storage/buf.h>
#include <storage/bufmgr.h>
#include <storage/lockdefs.h>
#include <storage/off.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/typcache.h>

#include "compression.h"
#include "compression/api.h"
#include "compression/arrow_tts.h"
#include "compressionam_handler.h"
#include "create.h"
#include "trigger.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_settings.h"

static const TableAmRoutine compressionam_methods;
static void compressionam_handler_end_conversion(Oid relid);

static Oid
get_compressed_chunk_relid(Oid chunk_relid)
{
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	return ts_chunk_get_relid(chunk->fd.compressed_chunk_id, false);
}

/* ------------------------------------------------------------------------
 * Slot related callbacks for compression AM
 * ------------------------------------------------------------------------
 */
static const TupleTableSlotOps *
compressionam_slot_callbacks(Relation relation)
{
	return &TTSOpsArrowTuple;
}

#define FEATURE_NOT_SUPPORTED                                                                      \
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("%s not supported", __func__)))

#define FUNCTION_DOES_NOTHING                                                                      \
	ereport(WARNING,                                                                               \
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),                                               \
			 errmsg("%s does not do anything yet", __func__)))

#define pgstat_count_compression_scan(rel) pgstat_count_heap_scan(rel)

#define pgstat_count_compression_getnext(rel) pgstat_count_heap_getnext(rel)

typedef struct CompressionInfoData
{
	CompressionSettings *settings;
	int16 *column_offsets;
	Bitmapset *segmentby_cols;
	Bitmapset *orderby_cols;
	int hypertable_id;
	int num_columns;
	int num_segmentby;
	int num_orderby;
	int num_keys;
} CompressionInfoData;

static CompressionInfoData *
build_compression_info_data(Relation rel)
{
	TupleDesc tupdesc = RelationGetDescr(rel);
	CompressionInfoData *cdata = palloc0(sizeof(CompressionInfoData));
	CompressionSettings *settings;
	int32 hyper_id = ts_chunk_get_hypertable_id_by_reloid(rel->rd_id);
	Oid hyper_relid = ts_hypertable_id_to_relid(hyper_id, false);

	settings = ts_compression_settings_get(hyper_relid);
	Ensure(settings,
		   "no compression settings for relation %s",
		   get_rel_name(RelationGetRelid(rel)));

	cdata->num_columns = tupdesc->natts;
	cdata->column_offsets = palloc0(sizeof(int) * tupdesc->natts);
	cdata->hypertable_id = hyper_id;
	cdata->settings = settings;

	for (int i = 0; i < cdata->num_columns; i++)
	{
		const Form_pg_attribute attr = &tupdesc->attrs[i];
		bool is_segmentby = ts_array_is_member(settings->fd.segmentby, NameStr(attr->attname));
		bool is_orderby = ts_array_is_member(settings->fd.orderby, NameStr(attr->attname));

		cdata->column_offsets[i] = AttrNumberGetAttrOffset(attr->attnum);

		if (is_segmentby)
		{
			cdata->num_segmentby += 1;
			cdata->segmentby_cols = bms_add_member(cdata->segmentby_cols, attr->attnum);
		}

		if (is_orderby)
		{
			cdata->num_orderby += 1;
			cdata->orderby_cols = bms_add_member(cdata->orderby_cols, attr->attnum);
		}

		if (is_segmentby || is_orderby)
			cdata->num_keys += 1;
	}

	Assert(cdata->num_segmentby == ts_array_length(settings->fd.segmentby));
	Assert(cdata->num_orderby == ts_array_length(settings->fd.orderby));

	return cdata;
}

typedef struct CompressionScanDescData
{
	TableScanDescData rs_base;
	TableScanDesc heap_scan;
	Relation compressed_rel;
	TupleTableSlot *compressed_slot;
	TableScanDesc compressed_scan_desc;
	uint16 compressed_tuple_index;
	int64 returned_row_count;
	int32 compressed_row_count;
	AttrNumber count_colattno;
	CompressionInfoData *cdata;
	int16 *attrs_map;
	bool compressed_read_done;
} CompressionScanDescData;

typedef struct CompressionScanDescData *CompressionScanDesc;

/*
 * Initialization common for beginscan and rescan.
 */
static void
initscan(CompressionScanDesc scan, ScanKey key)
{
	if (key != NULL && scan->rs_base.rs_nkeys > 0)
		memcpy(scan->rs_base.rs_key, key, scan->rs_base.rs_nkeys * sizeof(ScanKeyData));

	if (scan->rs_base.rs_flags & SO_TYPE_SEQSCAN)
		pgstat_count_compression_scan(scan->rs_base.rs_rd);
}

static TableScanDesc
compressionam_beginscan(Relation relation, Snapshot snapshot, int nkeys, ScanKey key,
						ParallelTableScanDesc parallel_scan, uint32 flags)
{
	CompressionScanDesc scan;
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();

	RelationIncrementReferenceCount(relation);

	scan = palloc0(sizeof(CompressionScanDescData));
	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;

	Chunk *chunk = ts_chunk_get_by_relid(RelationGetRelid(relation), true);
	Chunk *c_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);

	scan->compressed_rel = table_open(c_chunk->table_id, AccessShareLock);
	scan->compressed_tuple_index = 1;
	scan->compressed_slot = table_slot_create(scan->compressed_rel, NULL);
	scan->returned_row_count = 0;
	scan->compressed_row_count = 0;

	TupleDesc tupdesc = RelationGetDescr(relation);
	TupleDesc c_tupdesc = RelationGetDescr(scan->compressed_rel);

	scan->attrs_map = build_attribute_offset_map(tupdesc, c_tupdesc, &scan->count_colattno);
	scan->cdata = build_compression_info_data(relation);

	if (nkeys > 0)
		scan->rs_base.rs_key = (ScanKey) palloc(sizeof(ScanKeyData) * nkeys);
	else
		scan->rs_base.rs_key = NULL;

	initscan(scan, key);

	if (flags & SO_TYPE_ANALYZE)
		scan->compressed_scan_desc = table_beginscan_analyze(scan->compressed_rel);
	else
		scan->compressed_scan_desc = table_beginscan(scan->compressed_rel, snapshot, 0, NULL);

	scan->heap_scan = heapam->scan_begin(relation, snapshot, nkeys, key, parallel_scan, flags);

	return &scan->rs_base;
}

static void
compressionam_rescan(TableScanDesc sscan, ScanKey key, bool set_params, bool allow_strat,
					 bool allow_sync, bool allow_pagemode)
{
	CompressionScanDesc scan = (CompressionScanDesc) sscan;
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();

	initscan(scan, key);
	scan->compressed_tuple_index = 1;
	table_rescan(scan->compressed_scan_desc, NULL);
	heapam->scan_rescan(scan->heap_scan, key, set_params, allow_strat, allow_sync, allow_pagemode);
}

static void
compressionam_endscan(TableScanDesc sscan)
{
	CompressionScanDesc scan = (CompressionScanDesc) sscan;
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();

	RelationDecrementReferenceCount(sscan->rs_rd);
	ExecDropSingleTupleTableSlot(scan->compressed_slot);
	table_endscan(scan->compressed_scan_desc);
	table_close(scan->compressed_rel, AccessShareLock);
	heapam->scan_end(scan->heap_scan);

	if (scan->rs_base.rs_key)
		pfree(scan->rs_base.rs_key);

	pfree(scan->attrs_map);
	pfree(scan);
}

static bool
compressionam_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	CompressionScanDesc scan = (CompressionScanDesc) sscan;

	if (scan->compressed_read_done)
	{
		const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
		return heapam->scan_getnextslot(scan->heap_scan, direction, slot);
	}

	Assert(scan->compressed_tuple_index == 1 ||
		   scan->compressed_tuple_index <= scan->compressed_row_count);

	if (TupIsNull(scan->compressed_slot) ||
		(scan->compressed_tuple_index == scan->compressed_row_count))
	{
		if (!table_scan_getnextslot(scan->compressed_scan_desc, direction, scan->compressed_slot))
		{
			ExecClearTuple(slot);
			tts_arrow_set_heaptuple_mode(slot);
			scan->compressed_read_done = true;
			return compressionam_getnextslot(sscan, direction, slot);
		}

		bool isnull;
		Datum count = slot_getattr(scan->compressed_slot, scan->count_colattno, &isnull);

		Assert(!isnull);
		scan->compressed_row_count = DatumGetInt32(count);
		scan->compressed_tuple_index = 1;
		ExecStoreArrowTuple(slot, scan->compressed_slot, scan->compressed_tuple_index);
	}
	else
	{
		scan->compressed_tuple_index++;
		ExecStoreArrowTupleExisting(slot, scan->compressed_tuple_index);
	}

	Assert(!TTS_EMPTY(scan->compressed_slot));
	pgstat_count_compression_getnext(sscan->rs_rd);

	return true;
}

static void
compressionam_get_latest_tid(TableScanDesc sscan, ItemPointer tid)
{
	FEATURE_NOT_SUPPORTED;
}

static void
compressionam_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples, CommandId cid,
						   int options, BulkInsertStateData *bistate)
{
	FEATURE_NOT_SUPPORTED;
}

typedef struct IndexFetchComprData
{
	IndexFetchTableData h_base; /* AM independent part of the descriptor */
	IndexFetchTableData *compr_hscan;
	Relation compr_rel;
	TupleTableSlot *compressed_slot;
	ItemPointerData tid;
	int64 num_decompressions;
} IndexFetchComprData;

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for compression AM
 * ------------------------------------------------------------------------
 */
static IndexFetchTableData *
compressionam_index_fetch_begin(Relation rel)
{
	IndexFetchComprData *cscan = palloc0(sizeof(IndexFetchComprData));
	Oid cchunk_relid = get_compressed_chunk_relid(RelationGetRelid(rel));
	Relation crel = table_open(cchunk_relid, AccessShareLock);

	cscan->h_base.rel = rel;
	cscan->compr_rel = crel;
	cscan->compressed_slot = table_slot_create(crel, NULL);
	cscan->compr_hscan = crel->rd_tableam->index_fetch_begin(crel);
	ItemPointerSetInvalid(&cscan->tid);

	return &cscan->h_base;
}

static void
compressionam_index_fetch_reset(IndexFetchTableData *scan)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;

	ItemPointerSetInvalid(&cscan->tid);
	cscan->compr_rel->rd_tableam->index_fetch_reset(cscan->compr_hscan);
}

static void
compressionam_index_fetch_end(IndexFetchTableData *scan)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;
	Relation crel = cscan->compr_rel;

	crel->rd_tableam->index_fetch_end(cscan->compr_hscan);
	ExecDropSingleTupleTableSlot(cscan->compressed_slot);
	table_close(crel, AccessShareLock);
	pfree(cscan);
}

static bool
compressionam_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid,
								Snapshot snapshot, TupleTableSlot *slot, bool *call_again,
								bool *all_dead)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;
	Relation crel = cscan->compr_rel;
	ItemPointerData orig_tid;

	/* Recreate the original TID for the compressed table */
	uint16 tuple_index = compressed_tid_to_tid(&orig_tid, tid);

	/*
	 * Avoid decompression if the new TID from the index points to the same
	 * compressed tuple as the previous call to this function.
	 *
	 * There are cases, however, we're the index scan jumps between the same
	 * compressed tuples to get the right order, which will lead to
	 * decompressing the same compressed tuple multiple times. This happens,
	 * for example, when there's a segmentby column and orderby on
	 * time. Returning data in time order requires interleaving rows from two
	 * or more compressed tuples with different segmenby values. It is
	 * possible to optimize that case further by retaining a window/cache of
	 * decompressed tuples, keyed on TID.
	 */
	if (!TTS_EMPTY(slot) && ItemPointerEquals(&cscan->tid, &orig_tid))
	{
		/* Still in the same compressed tuple, so just update tuple index and
		 * return the same Arrow slot */
		ExecStoreArrowTupleExisting(slot, tuple_index);
		ItemPointerCopy(tid, &slot->tts_tid);
		return true;
	}

	bool result = crel->rd_tableam->index_fetch_tuple(cscan->compr_hscan,
													  &orig_tid,
													  snapshot,
													  cscan->compressed_slot,
													  call_again,
													  all_dead);

	if (result)
	{
		ExecStoreArrowTuple(slot, cscan->compressed_slot, tuple_index);
		/* Save the current compressed TID */
		ItemPointerCopy(&orig_tid, &cscan->tid);
		cscan->num_decompressions++;
	}

	return result;
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for compression AM
 * ------------------------------------------------------------------------
 */

static bool
compressionam_fetch_row_version(Relation relation, ItemPointer tid, Snapshot snapshot,
								TupleTableSlot *slot)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

static bool
compressionam_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

static bool
compressionam_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

#if PG14_GE
static TransactionId
compressionam_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	FEATURE_NOT_SUPPORTED;
	return 0;
}
#endif

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for compression AM.
 * ----------------------------------------------------------------------------
 */

typedef struct ConversionState
{
	Oid relid;
	Tuplesortstate *tuplesortstate;
	CompressionInfoData *cdata;
} ConversionState;

static ConversionState *conversionstate = NULL;

static void
compressionam_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid, int options,
						   BulkInsertStateData *bistate)
{
	if (conversionstate)
		tuplesort_puttupleslot(conversionstate->tuplesortstate, slot);
	else
	{
		const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
		heapam->tuple_insert(relation, slot, cid, options, bistate);
	}
}

static void
compressionam_tuple_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
									   int options, BulkInsertStateData *bistate, uint32 specToken)
{
	FEATURE_NOT_SUPPORTED;
}

static void
compressionam_tuple_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 specToken,
										 bool succeeded)
{
	FEATURE_NOT_SUPPORTED;
}

static TM_Result
compressionam_tuple_delete(Relation relation, ItemPointer tid, CommandId cid, Snapshot snapshot,
						   Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool changingPart)
{
	FEATURE_NOT_SUPPORTED;
	return TM_Ok;
}

#if PG16_LT
typedef bool TU_UpdateIndexes;
#endif

static TM_Result
compressionam_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot, CommandId cid,
						   Snapshot snapshot, Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
						   LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	FEATURE_NOT_SUPPORTED;
	return TM_Ok;
}

static TM_Result
compressionam_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
						 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
						 LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd)
{
	FEATURE_NOT_SUPPORTED;
	return TM_Ok;
}

static void
compressionam_finish_bulk_insert(Relation rel, int options)
{
	if (conversionstate)
		compressionam_handler_end_conversion(rel->rd_id);
}

/* ------------------------------------------------------------------------
 * DDL related callbacks for compression AM.
 * ------------------------------------------------------------------------
 */

#if PG16_LT
/* Account for API differences in pre-PG16 versions */
typedef RelFileNode RelFileLocator;
#define relation_set_new_filelocator relation_set_new_filenode
#endif

static void
compressionam_relation_set_new_filelocator(Relation rel, const RelFileLocator *newrlocator,
										   char persistence, TransactionId *freezeXid,
										   MultiXactId *minmulti)
{
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();

	heapam->relation_set_new_filelocator(rel, newrlocator, persistence, freezeXid, minmulti);
}

static void
compressionam_relation_nontransactional_truncate(Relation rel)
{
	RelationTruncate(rel, 0);
}

static void
compressionam_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	FEATURE_NOT_SUPPORTED;
}

static void
compressionam_relation_copy_for_cluster(Relation OldCompression, Relation NewCompression,
										Relation OldIndex, bool use_sort, TransactionId OldestXmin,
										TransactionId *xid_cutoff, MultiXactId *multi_cutoff,
										double *num_tuples, double *tups_vacuumed,
										double *tups_recently_dead)
{
	FEATURE_NOT_SUPPORTED;
}

static void
compressionam_vacuum_rel(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy)
{
	Oid cchunk_relid = get_compressed_chunk_relid(RelationGetRelid(rel));
	LOCKMODE lmode =
		(params->options & VACOPT_FULL) ? AccessExclusiveLock : ShareUpdateExclusiveLock;

	FEATURE_NOT_SUPPORTED;

	Relation crel = vacuum_open_relation(cchunk_relid,
										 NULL,
										 params->options,
										 params->log_min_duration >= 0,
										 lmode);

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM, RelationGetRelid(rel));

	/* Vacuum the uncompressed relation */
	// const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
	// heapam->relation_vacuum(rel, params, bstrategy);

	/* The compressed relation can be vacuumed too, but might not need it
	 * unless we do a lot of insert/deletes of compressed rows */
	crel->rd_tableam->relation_vacuum(crel, params, bstrategy);
	table_close(crel, NoLock);
}

static bool
compressionam_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
									  BufferAccessStrategy bstrategy)
{
	CompressionScanDescData *cscan = (CompressionScanDescData *) scan;

	FEATURE_NOT_SUPPORTED;

	return cscan->compressed_rel->rd_tableam->scan_analyze_next_block(cscan->compressed_scan_desc,
																	  blockno,
																	  bstrategy);
}

/*
 * Sample from the compressed chunk.
 *
 * TODO: this needs more work and it is not clear that this is the best way to
 * analyze.
 */
static bool
compressionam_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
									  double *liverows, double *deadrows, TupleTableSlot *slot)
{
	CompressionScanDescData *cscan = (CompressionScanDescData *) scan;

	FEATURE_NOT_SUPPORTED;

	bool result =
		cscan->compressed_rel->rd_tableam->scan_analyze_next_tuple(cscan->compressed_scan_desc,
																   OldestXmin,
																   liverows,
																   deadrows,
																   cscan->compressed_slot);

	if (result)
	{
		ExecStoreArrowTuple(slot, cscan->compressed_slot, 0);
	}
	else
	{
		ExecClearTuple(slot);
	}

	return result;
}

typedef struct IndexCallbackState
{
	IndexBuildCallback callback;
	CompressionInfoData *cdata;
	IndexInfo *index_info;
	EState *estate;
	void *orig_state;
	int16 tuple_index;
	double ntuples;
	Datum *values;
	bool *isnull;
	MemoryContext decompression_mcxt;
	ArrowArray **arrow_columns;
	int16 *attrs_map;
	AttrNumber count_cattno;
} IndexCallbackState;

/*
 * TODO: need to rerun filters on uncompressed tuples.
 */
static void
compression_index_build_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull,
								 bool tupleIsAlive, void *state)
{
	IndexCallbackState *icstate = state;
	// bool checking_uniqueness = (callback_state->index_info->ii_Unique ||
	//							callback_state->index_info->ii_ExclusionOps != NULL);
	int num_rows = -1;
	TupleDesc idesc = RelationGetDescr(index);

	for (int i = 0; i < icstate->index_info->ii_NumIndexAttrs; i++)
	{
		const AttrNumber attno = icstate->index_info->ii_IndexAttrNumbers[i];

		if (bms_is_member(attno, icstate->cdata->segmentby_cols))
		{
			// Segment by column, nothing to decompress. Just return the value
			// from the compressed chunk since it is the same for every row in
			// the compressed tuple.
			int countoff = icstate->index_info->ii_NumIndexAttrs;
			Assert(num_rows == -1 || num_rows == DatumGetInt32(values[countoff]));
			num_rows = DatumGetInt32(values[countoff]);
		}
		else
		{
			if (isnull[i])
			{
				// do nothing
			}
			else
			{
				const Form_pg_attribute attr = &idesc->attrs[i];
				const CompressedDataHeader *header =
					(CompressedDataHeader *) PG_DETOAST_DATUM(values[i]);
				DecompressAllFunction decompress_all =
					tsl_get_decompress_all_function(header->compression_algorithm, attr->atttypid);
				Assert(decompress_all != NULL);
				MemoryContext oldcxt = MemoryContextSwitchTo(icstate->decompression_mcxt);
				icstate->arrow_columns[i] =
					decompress_all(PointerGetDatum(header), idesc->attrs[i].atttypid, oldcxt);
				Assert(num_rows == -1 || num_rows == icstate->arrow_columns[i]->length);
				num_rows = icstate->arrow_columns[i]->length;
				MemoryContextReset(icstate->decompression_mcxt);
				MemoryContextSwitchTo(oldcxt);
			}
		}
	}

	Assert(num_rows > 0);

	for (int rownum = 0; rownum < num_rows; rownum++)
	{
		for (int colnum = 0; colnum < icstate->index_info->ii_NumIndexAttrs; colnum++)
		{
			const AttrNumber table_attno = icstate->index_info->ii_IndexAttrNumbers[colnum];

			if (bms_is_member(table_attno, icstate->cdata->segmentby_cols))
			{
				// Segment by column
			}
			else
			{
				const char *restrict arrow_values = icstate->arrow_columns[colnum]->buffers[1];
				const uint64 *restrict validity = icstate->arrow_columns[colnum]->buffers[0];
				int16 value_bytes = get_typlen(idesc->attrs[colnum].atttypid);

				/*
				 * The conversion of Datum to more narrow types will truncate
				 * the higher bytes, so we don't care if we read some garbage
				 * into them, and can always read 8 bytes. These are unaligned
				 * reads, so technically we have to do memcpy.
				 */
				uint64 value;
				memcpy(&value, &arrow_values[value_bytes * rownum], 8);

#ifdef USE_FLOAT8_BYVAL
				Datum datum = Int64GetDatum(value);
#else
				/*
				 * On 32-bit systems, the data larger than 4 bytes go by
				 * reference, so we have to jump through these hoops.
				 */
				Datum datum;
				if (value_bytes <= 4)
				{
					datum = Int32GetDatum((uint32) value);
				}
				else
				{
					datum = Int64GetDatum(value);
				}
#endif
				values[colnum] = datum;
				isnull[colnum] = !arrow_row_is_valid(validity, rownum);
			}
		}

		ItemPointerData index_tid;
		tid_to_compressed_tid(&index_tid, tid, rownum + 1);
		icstate->callback(index, &index_tid, values, isnull, tupleIsAlive, icstate->orig_state);
	}
}

static double
compressionam_index_build_range_scan(Relation relation, Relation indexRelation,
									 IndexInfo *indexInfo, bool allow_sync, bool anyvisible,
									 bool progress, BlockNumber start_blockno,
									 BlockNumber numblocks, IndexBuildCallback callback,
									 void *callback_state, TableScanDesc scan)
{
	Oid cchunk_relid = get_compressed_chunk_relid(RelationGetRelid(relation));
	Relation crel = table_open(cchunk_relid, AccessShareLock);
	AttrNumber count_cattno = InvalidAttrNumber;
	IndexCallbackState icstate = {
		.callback = callback,
		.orig_state = callback_state,
		.estate = CreateExecutorState(),
		.index_info = indexInfo,
		.tuple_index = -1,
		.ntuples = 0,
		.decompression_mcxt = AllocSetContextCreate(CurrentMemoryContext,
													"bulk decompression",
													/* minContextSize = */ 0,
													/* initBlockSize = */ 64 * 1024,
													/* maxBlockSize = */ 64 * 1024),
		.cdata = build_compression_info_data(relation),
		.attrs_map = build_attribute_offset_map(RelationGetDescr(relation),
												RelationGetDescr(crel),
												&count_cattno),
		.arrow_columns = palloc(sizeof(ArrowArray *) * indexInfo->ii_NumIndexAttrs),
	};
	IndexInfo iinfo = *indexInfo;

	/* Translate index attribute numbers for the compressed relation */
	for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
	{
		const AttrNumber attno = indexInfo->ii_IndexAttrNumbers[i];
		const int16 cattoff = icstate.attrs_map[AttrNumberGetAttrOffset(attno)];
		const AttrNumber cattno = AttrOffsetGetAttrNumber(cattoff);

		iinfo.ii_IndexAttrNumbers[i] = cattno;
		icstate.arrow_columns[i] = NULL;
	}

	/* Include the count column in the callback. It is needed to know the
	 * uncompressed tuple count in case of building an index on the segmentby
	 * column. */
	iinfo.ii_IndexAttrNumbers[iinfo.ii_NumIndexAttrs++] = count_cattno;

	/* Check uniqueness on compressed */
	iinfo.ii_Unique = false;
	iinfo.ii_ExclusionOps = NULL;
	iinfo.ii_Predicate = NULL;

	/* TODO: special case for segmentby column */
	crel->rd_tableam->index_build_range_scan(crel,
											 indexRelation,
											 &iinfo,
											 allow_sync,
											 anyvisible,
											 progress,
											 start_blockno,
											 numblocks,
											 compression_index_build_callback,
											 &icstate,
											 scan);

	table_close(crel, NoLock);
	FreeExecutorState(icstate.estate);
	MemoryContextDelete(icstate.decompression_mcxt);
	pfree(icstate.attrs_map);
	pfree(icstate.arrow_columns);

	return icstate.ntuples;
}

static void
compressionam_index_validate_scan(Relation compressionRelation, Relation indexRelation,
								  IndexInfo *indexInfo, Snapshot snapshot,
								  ValidateIndexState *state)
{
	FEATURE_NOT_SUPPORTED;
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the compression AM
 * ------------------------------------------------------------------------
 */
static bool
compressionam_relation_needs_toast_table(Relation rel)
{
	return false;
}

static Oid
compressionam_relation_toast_am(Relation rel)
{
	FEATURE_NOT_SUPPORTED;
	return InvalidOid;
}

/* ------------------------------------------------------------------------
 * Planner related callbacks for the compression AM
 * ------------------------------------------------------------------------
 */

static uint64
compressionam_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64 size = table_block_relation_size(rel, forkNumber);
	/*
	Oid cchunk_relid = get_compressed_chunk_relid(RelationGetRelid(rel));
	Relation crel = try_relation_open(cchunk_relid, AccessShareLock);

	if (crel == NULL)
		return 0;

	uint64 size = table_block_relation_size(rel, forkNumber);

	size += crel->rd_tableam->relation_size(crel, forkNumber);
	relation_close(crel, AccessShareLock);
	*/
	return size;
}

static void
compressionam_relation_estimate_size(Relation rel, int32 *attr_widths, BlockNumber *pages,
									 double *tuples, double *allvisfrac)
{
	Oid cchunk_relid = get_compressed_chunk_relid(RelationGetRelid(rel));

	if (!OidIsValid(cchunk_relid))
		return;

	Relation crel = table_open(cchunk_relid, AccessShareLock);

	/* Cannot pass on attr_widths since they are cached widths for the
	 * non-compressed relation which also doesn't have the same number of
	 * attribute (columns). If we pass NULL it will use an estimate
	 * instead. */
	crel->rd_tableam->relation_estimate_size(crel, NULL, pages, tuples, allvisfrac);

	*tuples *= GLOBAL_MAX_ROWS_PER_COMPRESSION;

	// TODO: merge with uncompressed rel size
	table_close(crel, AccessShareLock);
}

static void
compressionam_fetch_toast_slice(Relation toastrel, Oid valueid, int32 attrsize, int32 sliceoffset,
								int32 slicelength, struct varlena *result)
{
	FEATURE_NOT_SUPPORTED;
}

/* ------------------------------------------------------------------------
 * Executor related callbacks for the compression AM
 * ------------------------------------------------------------------------
 */

static bool
compressionam_scan_bitmap_next_block(TableScanDesc scan, TBMIterateResult *tbmres)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

static bool
compressionam_scan_bitmap_next_tuple(TableScanDesc scan, TBMIterateResult *tbmres,
									 TupleTableSlot *slot)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

static bool
compressionam_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

static bool
compressionam_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
									 TupleTableSlot *slot)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

void
compressionam_handler_start_conversion(Oid relid)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(CurTransactionContext);
	ConversionState *state = palloc0(sizeof(ConversionState));
	Relation relation = table_open(relid, AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(relation);
	CompressionInfoData *cdata = build_compression_info_data(relation);
	AttrNumber *sort_keys = palloc0(sizeof(*sort_keys) * cdata->num_keys);
	Oid *sort_operators = palloc0(sizeof(*sort_operators) * cdata->num_keys);
	Oid *sort_collations = palloc0(sizeof(*sort_collations) * cdata->num_keys);
	bool *nulls_first = palloc0(sizeof(*nulls_first) * cdata->num_keys);
	const CompressionSettings *settings = cdata->settings;

	Assert(cdata->num_segmentby == ts_array_length(settings->fd.segmentby));

	state->relid = relid;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		const Form_pg_attribute attr = &tupdesc->attrs[i];
		const char *attname = NameStr(attr->attname);
		int segmentby_pos = ts_array_position(settings->fd.segmentby, attname);
		int orderby_pos = ts_array_position(settings->fd.orderby, attname);

		if (segmentby_pos > 0 || orderby_pos > 0)
		{
			TypeCacheEntry *tentry;
			int sort_index = -1;
			Oid sort_op = InvalidOid;

			tentry = lookup_type_cache(attr->atttypid, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

			if (segmentby_pos > 0)
			{
				sort_index = segmentby_pos - 1;
				sort_op = tentry->lt_opr;
			}
			else if (orderby_pos > 0)
			{
				bool orderby_asc =
					!ts_array_get_element_bool(settings->fd.orderby_desc, orderby_pos);
				sort_index = cdata->num_segmentby + (orderby_pos - 1);
				sort_op = orderby_asc ? tentry->lt_opr : tentry->gt_opr;
			}

			if (!OidIsValid(sort_op))
				elog(ERROR,
					 "no valid sort operator for column \"%s\" of type \"%s\"",
					 attname,
					 format_type_be(attr->atttypid));

			sort_keys[sort_index] = attr->attnum;
			sort_operators[sort_index] = sort_op;
		}
	}

	state->cdata = cdata;
	state->tuplesortstate = tuplesort_begin_heap(tupdesc,
												 cdata->num_keys,
												 sort_keys,
												 sort_operators,
												 sort_collations,
												 nulls_first,
												 maintenance_work_mem,
												 NULL,
												 false /*=randomAccess*/);

	relation_close(relation, AccessShareLock);
	conversionstate = state;
	MemoryContextSwitchTo(oldcxt);
}

void
compressionam_handler_end_conversion(Oid relid)
{
	Chunk *chunk = ts_chunk_get_by_relid(conversionstate->relid, true);
	Relation relation = table_open(conversionstate->relid, AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(relation);

	if (!chunk)
		elog(ERROR, "could not find uncompressed chunk for relation %s", get_rel_name(relid));
	Hypertable *ht = ts_hypertable_get_by_id(chunk->fd.hypertable_id);
	Hypertable *ht_compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);

	tuplesort_performsort(conversionstate->tuplesortstate);

	Chunk *c_chunk = create_compress_chunk(ht_compressed, chunk, InvalidOid);
	Relation compressed_rel = table_open(c_chunk->table_id, RowExclusiveLock);
	RowCompressor row_compressor;

	row_compressor_init(conversionstate->cdata->settings,
						&row_compressor,
						relation,
						compressed_rel,
						RelationGetDescr(compressed_rel)->natts,
						true /*need_bistate*/,
						HEAP_INSERT_FROZEN);

	row_compressor_append_sorted_rows(&row_compressor,
									  conversionstate->tuplesortstate,
									  tupdesc,
									  compressed_rel);

	row_compressor_close(&row_compressor);
	tuplesort_end(conversionstate->tuplesortstate);

	/* Update compression statistics */
	RelationSize before_size = ts_relation_size_impl(chunk->table_id);
	RelationSize after_size = ts_relation_size_impl(c_chunk->table_id);
	compression_chunk_size_catalog_insert(chunk->fd.id,
										  &before_size,
										  c_chunk->fd.id,
										  &after_size,
										  row_compressor.rowcnt_pre_compression,
										  row_compressor.num_compressed_rows,
										  row_compressor.num_compressed_rows);

	/* Copy chunk constraints (including fkey) to compressed chunk.
	 * Do this after compressing the chunk to avoid holding strong, unnecessary locks on the
	 * referenced table during compression.
	 */
	ts_chunk_constraints_create(ht_compressed, c_chunk);
	ts_trigger_create_all_on_chunk(c_chunk);
	ts_chunk_set_compressed_chunk(chunk, c_chunk->fd.id);

	table_close(relation, NoLock);
	table_close(compressed_rel, NoLock);
	conversionstate = NULL;
}

/* ------------------------------------------------------------------------
 * Definition of the compression table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine compressionam_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = compressionam_slot_callbacks,

	.scan_begin = compressionam_beginscan,
	.scan_end = compressionam_endscan,
	.scan_rescan = compressionam_rescan,
	.scan_getnextslot = compressionam_getnextslot,
#if PG14_GE
	/*-----------
	 * Optional functions to provide scanning for ranges of ItemPointers.
	 * Implementations must either provide both of these functions, or neither
	 * of them.
	 */
	.scan_set_tidrange = NULL,
	.scan_getnextslot_tidrange = NULL,
#endif
	/* ------------------------------------------------------------------------
	 * Parallel table scan related functions.
	 * ------------------------------------------------------------------------
	 */
	.parallelscan_estimate = table_block_parallelscan_estimate,
	.parallelscan_initialize = table_block_parallelscan_initialize,
	.parallelscan_reinitialize = table_block_parallelscan_reinitialize,

	/* ------------------------------------------------------------------------
	 * Index Scan Callbacks
	 * ------------------------------------------------------------------------
	 */
	.index_fetch_begin = compressionam_index_fetch_begin,
	.index_fetch_reset = compressionam_index_fetch_reset,
	.index_fetch_end = compressionam_index_fetch_end,
	.index_fetch_tuple = compressionam_index_fetch_tuple,

	/* ------------------------------------------------------------------------
	 * Manipulations of physical tuples.
	 * ------------------------------------------------------------------------
	 */
	.tuple_insert = compressionam_tuple_insert,
	.tuple_insert_speculative = compressionam_tuple_insert_speculative,
	.tuple_complete_speculative = compressionam_tuple_complete_speculative,
	.multi_insert = compressionam_multi_insert,
	.tuple_delete = compressionam_tuple_delete,
	.tuple_update = compressionam_tuple_update,
	.tuple_lock = compressionam_tuple_lock,

	.finish_bulk_insert = compressionam_finish_bulk_insert,

	/* ------------------------------------------------------------------------
	 * Callbacks for non-modifying operations on individual tuples
	 * ------------------------------------------------------------------------
	 */
	.tuple_fetch_row_version = compressionam_fetch_row_version,

	.tuple_get_latest_tid = compressionam_get_latest_tid,
	.tuple_tid_valid = compressionam_tuple_tid_valid,
	.tuple_satisfies_snapshot = compressionam_tuple_satisfies_snapshot,
#if PG14_GE
	.index_delete_tuples = compressionam_index_delete_tuples,
#endif

/* ------------------------------------------------------------------------
 * DDL related functionality.
 * ------------------------------------------------------------------------
 */
#if PG16_GE
	.relation_set_new_filelocator = compressionam_relation_set_new_filelocator,
#else
	.relation_set_new_filenode = compressionam_relation_set_new_filelocator,
#endif
	.relation_nontransactional_truncate = compressionam_relation_nontransactional_truncate,
	.relation_copy_data = compressionam_relation_copy_data,
	.relation_copy_for_cluster = compressionam_relation_copy_for_cluster,
	.relation_vacuum = compressionam_vacuum_rel,
	.scan_analyze_next_block = compressionam_scan_analyze_next_block,
	.scan_analyze_next_tuple = compressionam_scan_analyze_next_tuple,
	.index_build_range_scan = compressionam_index_build_range_scan,
	.index_validate_scan = compressionam_index_validate_scan,

	/* ------------------------------------------------------------------------
	 * Miscellaneous functions.
	 * ------------------------------------------------------------------------
	 */
	.relation_size = compressionam_relation_size,
	.relation_needs_toast_table = compressionam_relation_needs_toast_table,
	.relation_toast_am = compressionam_relation_toast_am,
	.relation_fetch_toast_slice = compressionam_fetch_toast_slice,

	/* ------------------------------------------------------------------------
	 * Planner related functions.
	 * ------------------------------------------------------------------------
	 */
	.relation_estimate_size = compressionam_relation_estimate_size,

	/* ------------------------------------------------------------------------
	 * Executor related functions.
	 * ------------------------------------------------------------------------
	 */
	.scan_bitmap_next_block = compressionam_scan_bitmap_next_block,
	.scan_bitmap_next_tuple = compressionam_scan_bitmap_next_tuple,
	.scan_sample_next_block = compressionam_scan_sample_next_block,
	.scan_sample_next_tuple = compressionam_scan_sample_next_tuple,
};

const TableAmRoutine *
compressionam_routine(void)
{
	return &compressionam_methods;
}

Datum
compressionam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&compressionam_methods);
}
