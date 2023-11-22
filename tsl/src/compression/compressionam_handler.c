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
#include <access/transam.h>
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
#include <storage/block.h>
#include <storage/buf.h>
#include <storage/bufmgr.h>
#include <storage/itemptr.h>
#include <storage/lockdefs.h>
#include <storage/off.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/hsearch.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/typcache.h>

#include "compression.h"
#include "compression/api.h"
#include "compression/arrow_tts.h"
#include "compressionam_handler.h"
#include "create.h"
#include "guc.h"
#include "trigger.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_settings.h"

static const TableAmRoutine compressionam_methods;
static void compressionam_handler_end_conversion(Oid relid);

typedef struct ColumnCompressionSettings
{
	NameData attname;
	AttrNumber attnum;
	bool is_orderby;
	bool is_segmentby;
	bool orderby_desc;
	bool nulls_first;
} ColumnCompressionSettings;

/*
 * Compression info cache struct for access method.
 *
 * This struct is cached in a relcache entry's rd_amcache pointer and needs to
 * have a structure that can be palloc'ed in a single memory chunk.
 */
typedef struct CompressionAmInfo
{
	int32 hypertable_id;		  /* TimescaleDB ID of parent hypertable */
	int32 relation_id;			  /* TimescaleDB ID of relation (chunk ID) */
	int32 compressed_relation_id; /* TimescaleDB ID of compressed relation (chunk ID) */
	Oid compressed_relid;		  /* Relid of compressed relation */
	int num_columns;
	int num_segmentby;
	int num_orderby;
	int num_keys;
	/* Per-column information follows. */
	ColumnCompressionSettings columns[FLEXIBLE_ARRAY_MEMBER];
} CompressionAmInfo;

#define COMPRESSION_AM_INFO_SIZE(natts)                                                            \
	(sizeof(CompressionAmInfo) + sizeof(ColumnCompressionSettings) * (natts))

static int32
get_compressed_chunk_id(Oid chunk_relid)
{
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	return chunk->fd.compressed_chunk_id;
}

static int32
get_chunk_id_from_relid(Oid relid)
{
	int32 chunk_id;
	Oid nspid = get_rel_namespace(relid);
	const char *schema = get_namespace_name(nspid);
	const char *relname = get_rel_name(relid);
	ts_chunk_get_id(schema, relname, &chunk_id, false);

	return chunk_id;
}

static CompressionAmInfo *
lazy_build_compressionam_info_cache(Relation rel, bool include_compressed_relinfo)
{
	CompressionAmInfo *caminfo = rel->rd_amcache;

	if (NULL == caminfo)
	{
		TupleDesc tupdesc = RelationGetDescr(rel);
		int32 hyper_id = ts_chunk_get_hypertable_id_by_reloid(rel->rd_id);
		Oid hyper_relid = ts_hypertable_id_to_relid(hyper_id, false);
		CompressionSettings *settings = ts_compression_settings_get(hyper_relid);

		Ensure(settings,
			   "no compression settings for relation %s",
			   get_rel_name(RelationGetRelid(rel)));

		/* Anything put in rel->rd_amcache must be a single memory chunk
		 * palloc'd in CacheMemoryContext since PostgreSQL expects to be able
		 * to free it with a single pfree(). */
		caminfo =
			MemoryContextAllocZero(CacheMemoryContext, COMPRESSION_AM_INFO_SIZE(tupdesc->natts));
		caminfo->compressed_relid = InvalidOid;
		caminfo->num_columns = tupdesc->natts;
		caminfo->hypertable_id = hyper_id;

		for (int i = 0; i < caminfo->num_columns; i++)
		{
			const Form_pg_attribute attr = &tupdesc->attrs[i];
			ColumnCompressionSettings *colsettings = &caminfo->columns[i];
			const char *attname = NameStr(attr->attname);
			int segmentby_pos = ts_array_position(settings->fd.segmentby, attname);
			int orderby_pos = ts_array_position(settings->fd.orderby, attname);

			namestrcpy(&colsettings->attname, attname);
			colsettings->attnum = attr->attnum;
			colsettings->is_segmentby = segmentby_pos > 0;
			colsettings->is_orderby = orderby_pos > 0;

			if (colsettings->is_segmentby)
				caminfo->num_segmentby += 1;

			if (colsettings->is_orderby)
			{
				caminfo->num_orderby += 1;
				colsettings->orderby_desc =
					ts_array_get_element_bool(settings->fd.orderby_desc, orderby_pos);
				colsettings->nulls_first =
					ts_array_get_element_bool(settings->fd.orderby_nullsfirst, orderby_pos);
			}

			if (colsettings->is_segmentby || colsettings->is_orderby)
				caminfo->num_keys += 1;
		}

		Assert(caminfo->num_segmentby == ts_array_length(settings->fd.segmentby));
		Assert(caminfo->num_orderby == ts_array_length(settings->fd.orderby));
	}

	/* Only optionally include information about the compressed chunk because
	 * it might not exist when this cache is built. The information will be
	 * added the next time the function is called instead. */
	if (!OidIsValid(caminfo->compressed_relid) && include_compressed_relinfo)
	{
		caminfo->relation_id = get_chunk_id_from_relid(rel->rd_id);
		caminfo->compressed_relation_id = get_compressed_chunk_id(rel->rd_id);
		caminfo->compressed_relid = ts_chunk_get_relid(caminfo->compressed_relation_id, false);
	}

	return caminfo;
}

static inline CompressionAmInfo *
RelationGetCompressionAmInfo(Relation rel)
{
	return lazy_build_compressionam_info_cache(rel, true);
}

static void
build_segment_and_orderby_bms(const CompressionAmInfo *caminfo, Bitmapset **segmentby,
							  Bitmapset **orderby)
{
	*segmentby = NULL;
	*orderby = NULL;

	for (int i = 0; i < caminfo->num_columns; i++)
	{
		const ColumnCompressionSettings *colsettings = &caminfo->columns[i];

		if (colsettings->is_segmentby)
			*segmentby = bms_add_member(*segmentby, colsettings->attnum);

		if (colsettings->is_orderby)
			*orderby = bms_add_member(*orderby, colsettings->attnum);
	}
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

typedef struct CompressionScanDescData
{
	TableScanDescData rs_base;
	TableScanDesc heap_scan;
	Relation compressed_rel;
	TableScanDesc compressed_scan_desc;
	uint16 compressed_tuple_index;
	int64 returned_row_count;
	int32 compressed_row_count;
	AttrNumber count_colattno;
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
	scan->returned_row_count = 0;
	scan->compressed_row_count = 0;

	/* Disable reading of compressed data if transparent decompression is
	 * enabled, since that that scan node reads compressed data directly form
	 * the compressed chunk. If we also read the compressed data via this TAM,
	 * then the compressed data will be returned twice. */
	scan->compressed_read_done = ts_guc_enable_transparent_decompression;

	TupleDesc tupdesc = RelationGetDescr(relation);
	TupleDesc c_tupdesc = RelationGetDescr(scan->compressed_rel);

	scan->attrs_map = build_attribute_offset_map(tupdesc, c_tupdesc, &scan->count_colattno);

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
	TupleTableSlot *child_slot;

	if (scan->compressed_read_done)
	{
		/* All the compressed data has been returned, so now return tuples
		 * from the non-compressed data */
		const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
		child_slot = arrow_slot_get_noncompressed_slot(slot);

		bool result = heapam->scan_getnextslot(scan->heap_scan, direction, child_slot);

		if (result)
			ExecStoreArrowTuple(slot, InvalidTupleIndex);

		return result;
	}

	Assert(scan->compressed_tuple_index == 1 ||
		   scan->compressed_tuple_index <= scan->compressed_row_count);

	child_slot = arrow_slot_get_compressed_slot(slot, RelationGetDescr(scan->compressed_rel));

	if (TupIsNull(child_slot) || (scan->compressed_tuple_index == scan->compressed_row_count))
	{
		if (!table_scan_getnextslot(scan->compressed_scan_desc, direction, child_slot))
		{
			ExecClearTuple(slot);
			scan->compressed_read_done = true;
			return compressionam_getnextslot(sscan, direction, slot);
		}

		Assert(ItemPointerIsValid(&child_slot->tts_tid));

		bool isnull;
		Datum count = slot_getattr(child_slot, scan->count_colattno, &isnull);

		Assert(!isnull);
		scan->compressed_row_count = DatumGetInt32(count);
		scan->compressed_tuple_index = 1;
		ExecStoreArrowTuple(slot, scan->compressed_tuple_index);
	}
	else
	{
		scan->compressed_tuple_index++;
		ExecStoreArrowTuple(slot, scan->compressed_tuple_index);
	}

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
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
	/* Inserts only supported in non-compressed relation, so simply forward to the heap AM */
	heapam->multi_insert(relation, slots, ntuples, cid, options, bistate);
}

typedef struct IndexFetchComprData
{
	IndexFetchTableData h_base; /* AM independent part of the descriptor */
	IndexFetchTableData *compr_hscan;
	IndexFetchTableData *uncompr_hscan;
	Relation compr_rel;
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
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
	IndexFetchComprData *cscan = palloc0(sizeof(IndexFetchComprData));
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);

	Relation crel = table_open(caminfo->compressed_relid, AccessShareLock);
	cscan->h_base.rel = rel;
	cscan->compr_rel = crel;
	cscan->compr_hscan = crel->rd_tableam->index_fetch_begin(crel);
	cscan->uncompr_hscan = heapam->index_fetch_begin(rel);
	ItemPointerSetInvalid(&cscan->tid);

	return &cscan->h_base;
}

static void
compressionam_index_fetch_reset(IndexFetchTableData *scan)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();

	ItemPointerSetInvalid(&cscan->tid);
	cscan->compr_rel->rd_tableam->index_fetch_reset(cscan->compr_hscan);
	heapam->index_fetch_reset(cscan->uncompr_hscan);
}

static void
compressionam_index_fetch_end(IndexFetchTableData *scan)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
	Relation crel = cscan->compr_rel;

	crel->rd_tableam->index_fetch_end(cscan->compr_hscan);
	heapam->index_fetch_end(cscan->uncompr_hscan);
	table_close(crel, AccessShareLock);
	pfree(cscan);
}

/*
 * Return tuple for given TID via index scan.
 */
static bool
compressionam_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid,
								Snapshot snapshot, TupleTableSlot *slot, bool *call_again,
								bool *all_dead)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;
	TupleTableSlot *child_slot;
	Relation crel = cscan->compr_rel;
	ItemPointerData decoded_tid;

	if (!is_compressed_tid(tid))
	{
		const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
		child_slot = arrow_slot_get_noncompressed_slot(slot);
		bool result = heapam->index_fetch_tuple(cscan->uncompr_hscan,
												tid,
												snapshot,
												child_slot,
												call_again,
												all_dead);

		if (result)
			ExecStoreArrowTuple(slot, InvalidTupleIndex);

		return result;
	}

	/* Compressed tuples not visible through this TAM when scanned by
	 * transparent decompression enabled since DecompressChunk already scanned
	 * that data. */
	if (ts_guc_enable_transparent_decompression)
		return false;

	/* Recreate the original TID for the compressed table */
	uint16 tuple_index = compressed_tid_to_tid(&decoded_tid, tid);
	Assert(tuple_index != InvalidTupleIndex);
	child_slot = arrow_slot_get_compressed_slot(slot, RelationGetDescr(cscan->compr_rel));

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
	if (!TTS_EMPTY(child_slot) && !TTS_EMPTY(slot) && ItemPointerIsValid(&cscan->tid) &&
		ItemPointerEquals(&cscan->tid, &decoded_tid))
	{
		/* Still in the same compressed tuple, so just update tuple index and
		 * return the same Arrow slot */
		ExecStoreArrowTuple(slot, tuple_index);
		return true;
	}

	bool result = crel->rd_tableam->index_fetch_tuple(cscan->compr_hscan,
													  &decoded_tid,
													  snapshot,
													  child_slot,
													  call_again,
													  all_dead);

	if (result)
	{
		ExecStoreArrowTuple(slot, tuple_index);
		/* Save the current compressed TID */
		ItemPointerCopy(&decoded_tid, &cscan->tid);
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
	if (!is_compressed_tid(tid))
	{
		/* Just pass this on to regular heap AM */
		const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
		TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
		bool result = heapam->tuple_fetch_row_version(relation, tid, snapshot, child_slot);

		if (result)
			ExecStoreArrowTuple(slot, InvalidTupleIndex);

		return result;
	}

	FEATURE_NOT_SUPPORTED;
	/*
	ItemPointerData decoded_tid;

	uint16 tuple_index = compressed_tid_to_tid(&decoded_tid, tid);
	Oid compr_relid = get_compressed_table_relid(RelationGetRelid(relation));
	Relation compr_rel = table_open(compr_relid, AccessShareLock);
	TupleTableSlot *compr_slot = table_slot_create(compr_rel, NULL);

	compr_rel->rd_tableam->tuple_fetch_row_version(compr_rel, &decoded_tid, snapshot, compr_slot);
	table_close(compr_rel, NoLock);
	*/
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
	return false;
}

/*
 * Determine which index tuples are safe to delete.
 *
 * The Index AM asks the Table AM about which given index tuples (as
 * referenced by TID) are safe to delete. Given that the array of TIDs to
 * delete ("delTIDs") may reference either the compressed or non-compressed
 * relation within the compression TAM, it is necessary to split the
 * information in the TM_IndexDeleteOp in two: one for each relation. Then the
 * operation can be relayed to the standard heapAM method to do the heavy
 * lifting for each relation.
 *
 * In order to call the heapAM method on the compressed relation, it is
 * necessary to first "decode" the compressed TIDs to "normal" TIDs that
 * reference compressed tuples. A complication, however, is that multiple
 * distinct "compressed" TIDs may decode to the same TID, i.e., they reference
 * the same compressed tuple in the TAM's compressed relation, and the heapAM
 * method for index_delete_tuples() expects only unique TIDs. Therefore, it is
 * necesssary to deduplicate TIDs before calling the heapAM method on the
 * compressed relation and then restore the result array of decoded delTIDs
 * after the method returns. Note that the returned delTID array might be
 * smaller than the input delTID array since only the TIDs that are safe to
 * delete should remain. Thus, if a decoded TID is not safe to delete, then
 * all compressed TIDs that reference that compressed tuple are also not safe
 * to delete.
 */
static TransactionId
compressionam_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	TM_IndexDeleteOp noncompr_delstate = *delstate;
	TM_IndexDeleteOp compr_delstate = *delstate;
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);
	/* Hash table setup for TID deduplication */
	typedef struct TidEntry
	{
		ItemPointerData tid;
		List *tuple_indexes;
		List *status_indexes;
	} TidEntry;
	struct HASHCTL hctl = {
		.keysize = sizeof(ItemPointerData),
		.entrysize = sizeof(TidEntry),
		.hcxt = CurrentMemoryContext,
	};
	unsigned int total_knowndeletable_compressed = 0;
	unsigned int total_knowndeletable_non_compressed = 0;

	/*
	 * Setup separate TM_IndexDeleteOPs for the compressed and non-compressed
	 * relations. Note that it is OK to reference the original status array
	 * because it is accessed via the "id" index in the TM_IndexDelete struct,
	 * so it doesn't need the same length and order as the deltids array. This
	 * is because the deltids array is going to be sorted during processing
	 * anyway so the "same-array-index" mappings for the status and deltids
	 * arrays will be lost in any case.
	 */
	noncompr_delstate.deltids = palloc(sizeof(TM_IndexDelete) * delstate->ndeltids);
	noncompr_delstate.ndeltids = 0;
	compr_delstate.deltids = palloc(sizeof(TM_IndexDelete) * delstate->ndeltids);
	compr_delstate.ndeltids = 0;

	/* Hash table to deduplicate compressed TIDs that point to the same
	 * compressed tuple */
	HTAB *tidhash = hash_create("IndexDelete deduplication",
								delstate->ndeltids,
								&hctl,
								HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	/*
	 * Stage 1: preparation.
	 *
	 * Split the deltids array based on the two relations and deduplicate
	 * compressed TIDs at the same time. When deduplicating, it is necessary
	 * to "remember" the lost information when decoding (e.g., index into a
	 * compressed tuple).
	 */
	for (int i = 0; i < delstate->ndeltids; i++)
	{
		const TM_IndexDelete *deltid = &delstate->deltids[i];
		const TM_IndexStatus *status = &delstate->status[deltid->id];

		/* If this is a compressed TID, decode and deduplicate
		 * first. Otherwise just add to the non-compressed deltids array */
		if (is_compressed_tid(&deltid->tid))
		{
			TM_IndexDelete *deltid_compr = &compr_delstate.deltids[compr_delstate.ndeltids];
			ItemPointerData decoded_tid;
			bool found;
			TidEntry *tidentry;
			uint16 tuple_index;

			tuple_index = compressed_tid_to_tid(&decoded_tid, &deltid->tid);
			tidentry = hash_search(tidhash, &decoded_tid, HASH_ENTER, &found);

			if (status->knowndeletable)
				total_knowndeletable_compressed++;

			if (!found)
			{
				/* Add to compressed IndexDelete array */
				deltid_compr->id = deltid->id;
				ItemPointerCopy(&decoded_tid, &deltid_compr->tid);

				/* Remember the information for the compressed TID so that the
				 * deltids array can be restored later */
				tidentry->tuple_indexes = list_make1_int(tuple_index);
				tidentry->status_indexes = list_make1_int(deltid->id);
				compr_delstate.ndeltids++;
			}
			else
			{
				/* Duplicate TID, so just append info that needs to be remembered */
				tidentry->tuple_indexes = lappend_int(tidentry->tuple_indexes, tuple_index);
				tidentry->status_indexes = lappend_int(tidentry->status_indexes, deltid->id);
			}
		}
		else
		{
			TM_IndexDelete *deltid_noncompr =
				&noncompr_delstate.deltids[noncompr_delstate.ndeltids];

			*deltid_noncompr = *deltid;
			noncompr_delstate.ndeltids++;

			if (status->knowndeletable)
				total_knowndeletable_non_compressed++;
		}
	}

	Assert((total_knowndeletable_non_compressed + total_knowndeletable_compressed) > 0 ||
		   delstate->bottomup);

	/*
	 * Stage 2: call heapAM method for each relation and recreate the deltids
	 * array with the result.
	 *
	 * The heapAM method implements various assumptions and asserts around the
	 * contents of the deltids array depending on whether the index AM is
	 * doing simple index tuple deletion or bottom up deletion (as indicated
	 * by delstate->bottomup). For example, in the simple index deletion case,
	 * it seems the deltids array should have at least have one known
	 * deletable entry or otherwise the heapAM might prune the array to zero
	 * length which leads to an assertion failure because it can only be zero
	 * length in the bottomup case. Since we split the original deltids array
	 * across the compressed and non-compressed relations, we might end up in
	 * a situation where we call one relation without any knowndeletable TIDs
	 * in the simple deletion case, leading to an assertion
	 * failure. Therefore, only call heapAM if there is at least one
	 * knowndeletable or we are doing bottomup deletion.
	 *
	 * Note, also, that the function should return latestRemovedXid
	 * transaction ID, so need to remember those for each call and then return
	 * the latest removed of those.
	 */
	TransactionId xid_noncompr = InvalidTransactionId;
	TransactionId xid_compr = InvalidTransactionId;
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();

	/* Reset the deltids array before recreating it with the result */
	delstate->ndeltids = 0;

	if (noncompr_delstate.ndeltids > 0 &&
		(total_knowndeletable_non_compressed > 0 || delstate->bottomup))
	{
		xid_noncompr = heapam->index_delete_tuples(rel, &noncompr_delstate);
		memcpy(delstate->deltids,
			   noncompr_delstate.deltids,
			   noncompr_delstate.ndeltids * sizeof(TM_IndexDelete));
		delstate->ndeltids = noncompr_delstate.ndeltids;
	}

	if (compr_delstate.ndeltids > 0 && (total_knowndeletable_compressed > 0 || delstate->bottomup))
	{
		/* Assume RowExclusivelock since this involves deleting tuples */
		Relation compr_rel = table_open(caminfo->compressed_relid, RowExclusiveLock);

		xid_compr = heapam->index_delete_tuples(compr_rel, &compr_delstate);

		for (int i = 0; i < compr_delstate.ndeltids; i++)
		{
			const TM_IndexDelete *deltid_compr = &compr_delstate.deltids[i];
			const TM_IndexStatus *status_compr = &delstate->status[deltid_compr->id];
			ListCell *lc_id, *lc_tupindex;
			TidEntry *tidentry;
			bool found;

			tidentry = hash_search(tidhash, &deltid_compr->tid, HASH_FIND, &found);

			Assert(found);

			forboth (lc_id, tidentry->status_indexes, lc_tupindex, tidentry->tuple_indexes)
			{
				int id = lfirst_int(lc_id);
				uint16 tuple_index = lfirst_int(lc_tupindex);
				TM_IndexDelete *deltid = &delstate->deltids[delstate->ndeltids];
				TM_IndexStatus *status = &delstate->status[deltid->id];

				deltid->id = id;
				/* Assume that all index tuples pointing to the same heap
				 * compressed tuple are deletable if one is
				 * deletable. Otherwise leave status as before. */
				if (status_compr->knowndeletable)
					status->knowndeletable = true;

				tid_to_compressed_tid(&deltid->tid, &deltid_compr->tid, tuple_index);
				delstate->ndeltids++;
			}
		}

		table_close(compr_rel, NoLock);
	}

	hash_destroy(tidhash);
	pfree(compr_delstate.deltids);
	pfree(noncompr_delstate.deltids);

#ifdef USE_ASSERT_CHECKING
	do
	{
		int ndeletable = 0;

		for (int i = 0; i < delstate->ndeltids; i++)
		{
			const TM_IndexDelete *deltid = &delstate->deltids[i];
			const TM_IndexStatus *status = &delstate->status[deltid->id];

			if (status->knowndeletable)
				ndeletable++;
		}

		Assert(ndeletable > 0 || delstate->ndeltids == 0);
	} while (0);
#endif

	/* Return the latestremovedXid. TransactionIdFollows can handle
	 * InvalidTransactionid. */
	return TransactionIdFollows(xid_noncompr, xid_compr) ? xid_noncompr : xid_compr;
}

/* ----------------------------------------------------------------------------
 *  Functions for manipulations of physical tuples for compression AM.
 * ----------------------------------------------------------------------------
 */

typedef struct ConversionState
{
	Oid relid;
	Tuplesortstate *tuplesortstate;
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
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
	heapam->tuple_insert_speculative(relation, slot, cid, options, bistate, specToken);
}

static void
compressionam_tuple_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 specToken,
										 bool succeeded)
{
	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
	heapam->tuple_complete_speculative(relation, slot, specToken, succeeded);
}

static TM_Result
compressionam_tuple_delete(Relation relation, ItemPointer tid, CommandId cid, Snapshot snapshot,
						   Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool changingPart)
{
	if (!is_compressed_tid(tid))
	{
		/* Just pass this on to regular heap AM */
		const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
		return heapam
			->tuple_delete(relation, tid, cid, snapshot, crosscheck, wait, tmfd, changingPart);
	}

	/* This shouldn't happen because hypertable_modify should have
	 * decompressed the data to be deleted already. It can happen, however, if
	 * DELETE is run directly on a hypertable chunk, because that case isn't
	 * handled in the current code for DML on compressed chunks. */
	elog(ERROR, "cannot delete compressed tuple");

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
	if (!is_compressed_tid(otid))
	{
		/* Just pass this on to regular heap AM */
		const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
		return heapam->tuple_update(relation,
									otid,
									slot,
									cid,
									snapshot,
									crosscheck,
									wait,
									tmfd,
									lockmode,
									update_indexes);
	}

	/* This shouldn't happen because hypertable_modify should have
	 * decompressed the data to be deleted already. It can happen, however, if
	 * UPDATE is run directly on a hypertable chunk, because that case isn't
	 * handled in the current code for DML on compressed chunks. */
	elog(ERROR, "cannot update compressed tuple");

	return TM_Ok;
}

static TM_Result
compressionam_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot,
						 TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
						 LockWaitPolicy wait_policy, uint8 flags, TM_FailureData *tmfd)
{
	TM_Result result;

	if (is_compressed_tid(tid))
	{
		CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(relation);
		/* SELECT FOR UPDATE takes RowShareLock, so assume this
		 * lockmode. Another option to consider is take same lock as currently
		 * held on the non-compressed relation */
		Relation crel = table_open(caminfo->compressed_relid, RowShareLock);
		TupleTableSlot *child_slot = arrow_slot_get_compressed_slot(slot, RelationGetDescr(crel));
		ItemPointerData decoded_tid;

		uint16 tuple_index = compressed_tid_to_tid(&decoded_tid, tid);
		result = crel->rd_tableam->tuple_lock(crel,
											  &decoded_tid,
											  snapshot,
											  child_slot,
											  cid,
											  mode,
											  wait_policy,
											  flags,
											  tmfd);

		if (result == TM_Ok)
			ExecStoreArrowTuple(slot, tuple_index);

		table_close(crel, NoLock);
	}
	else
	{
		const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
		TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
		result = heapam->tuple_lock(relation,
									tid,
									snapshot,
									child_slot,
									cid,
									mode,
									wait_policy,
									flags,
									tmfd);

		if (result == TM_Ok)
			ExecStoreArrowTuple(slot, InvalidTupleIndex);
	}

	return result;
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
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);
	LOCKMODE lmode =
		(params->options & VACOPT_FULL) ? AccessExclusiveLock : ShareUpdateExclusiveLock;

	FEATURE_NOT_SUPPORTED;

	Relation crel = vacuum_open_relation(caminfo->compressed_relid,
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
	TupleTableSlot *child_slot =
		arrow_slot_get_compressed_slot(slot, RelationGetDescr(cscan->compressed_rel));
	FEATURE_NOT_SUPPORTED;

	bool result =
		cscan->compressed_rel->rd_tableam->scan_analyze_next_tuple(cscan->compressed_scan_desc,
																   OldestXmin,
																   liverows,
																   deadrows,
																   child_slot);

	if (result)
		ExecStoreArrowTuple(slot, 1);
	else
		ExecClearTuple(slot);

	return result;
}

typedef struct IndexCallbackState
{
	IndexBuildCallback callback;
	Relation rel;
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
	int32 num_rows = -1;
	TupleDesc idesc = RelationGetDescr(index);
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(icstate->rel);
	Bitmapset *segmentby_cols = NULL;
	Bitmapset *orderby_cols = NULL;

	build_segment_and_orderby_bms(caminfo, &segmentby_cols, &orderby_cols);

	for (int i = 0; i < icstate->index_info->ii_NumIndexAttrs; i++)
	{
		const AttrNumber attno = icstate->index_info->ii_IndexAttrNumbers[i];

		if (bms_is_member(attno, segmentby_cols))
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
			const AttrNumber attno = icstate->index_info->ii_IndexAttrNumbers[colnum];

			if (bms_is_member(attno, segmentby_cols))
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

	bms_free(segmentby_cols);
	bms_free(orderby_cols);
}

/*
 * Build index.
 *
 * TODO: Explore potential to reduce index size by only having one TID for
 * each compressed tuple (instead of a TID per value) and use the call_again
 * functionality in the index scan callbacks to return all values.
 */
static double
compressionam_index_build_range_scan(Relation relation, Relation indexRelation,
									 IndexInfo *indexInfo, bool allow_sync, bool anyvisible,
									 bool progress, BlockNumber start_blockno,
									 BlockNumber numblocks, IndexBuildCallback callback,
									 void *callback_state, TableScanDesc scan)
{
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(relation);
	Relation crel = table_open(caminfo->compressed_relid, AccessShareLock);
	AttrNumber count_cattno = InvalidAttrNumber;
	IndexCallbackState icstate = {
		.callback = callback,
		.orig_state = callback_state,
		.rel = relation,
		.estate = CreateExecutorState(),
		.index_info = indexInfo,
		.tuple_index = -1,
		.ntuples = 0,
		.decompression_mcxt = AllocSetContextCreate(CurrentMemoryContext,
													"bulk decompression",
													/* minContextSize = */ 0,
													/* initBlockSize = */ 64 * 1024,
													/* maxBlockSize = */ 64 * 1024),
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

	const TableAmRoutine *heapam = GetHeapamTableAmRoutine();
	const TableAmRoutine *oldam = relation->rd_tableam;
	relation->rd_tableam = heapam;
	icstate.ntuples += heapam->index_build_range_scan(relation,
													  indexRelation,
													  indexInfo,
													  allow_sync,
													  anyvisible,
													  progress,
													  start_blockno,
													  numblocks,
													  callback,
													  callback_state,
													  scan);
	relation->rd_tableam = oldam;

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
	  CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);
	Relation crel = try_relation_open(caminfo->compressed_relid, AccessShareLock);

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
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);

	if (!OidIsValid(caminfo->compressed_relid))
		return;

	Relation crel = table_open(caminfo->compressed_relid, AccessShareLock);

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
	CompressionAmInfo *caminfo = lazy_build_compressionam_info_cache(relation, false);
	AttrNumber *sort_keys = palloc0(sizeof(*sort_keys) * caminfo->num_keys);
	Oid *sort_operators = palloc0(sizeof(*sort_operators) * caminfo->num_keys);
	Oid *sort_collations = palloc0(sizeof(*sort_collations) * caminfo->num_keys);
	bool *nulls_first = palloc0(sizeof(*nulls_first) * caminfo->num_keys);
	int segmentby_pos = 0;
	int orderby_pos = 0;

	state->relid = relid;

	for (int i = 0; i < caminfo->num_columns; i++)
	{
		const ColumnCompressionSettings *column = &caminfo->columns[i];
		const Form_pg_attribute attr = &tupdesc->attrs[i];

		Assert(attr->attnum == column->attnum);

		if (column->is_segmentby || column->is_orderby)
		{
			TypeCacheEntry *tentry;
			int sort_index = -1;
			Oid sort_op = InvalidOid;

			tentry = lookup_type_cache(attr->atttypid, TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

			if (column->is_segmentby)
			{
				sort_index = segmentby_pos++;
				sort_op = tentry->lt_opr;
			}
			else if (column->is_orderby)
			{
				sort_index = caminfo->num_segmentby + orderby_pos++;
				sort_op = !column->orderby_desc ? tentry->lt_opr : tentry->gt_opr;
			}

			if (!OidIsValid(sort_op))
				elog(ERROR,
					 "no valid sort operator for column \"%s\" of type \"%s\"",
					 NameStr(column->attname),
					 format_type_be(attr->atttypid));

			sort_keys[sort_index] = column->attnum;
			sort_operators[sort_index] = sort_op;
			nulls_first[sort_index] = column->nulls_first;
		}
	}

	state->tuplesortstate = tuplesort_begin_heap(tupdesc,
												 caminfo->num_keys,
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
	CompressionSettings *settings = ts_compression_settings_get(ht->main_table_relid);
	RowCompressor row_compressor;

	row_compressor_init(settings,
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
