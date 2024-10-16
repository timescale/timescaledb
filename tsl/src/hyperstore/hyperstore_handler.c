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
#include <access/xact.h>
#include <catalog/heap.h>
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

#include "arrow_tts.h"
#include "compression/api.h"
#include "compression/compression.h"
#include "compression/create.h"
#include "debug_assert.h"
#include "guc.h"
#include "hyperstore_handler.h"
#include "trigger.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_chunk_size.h"
#include "ts_catalog/compression_settings.h"

static const TableAmRoutine compressionam_methods;
static void convert_to_compressionam_finish(Oid relid);
static List *partially_compressed_relids = NIL; /* Relids that needs to have
												 * updated status set at end of
												 * transcation */

#define COMPRESSION_AM_INFO_SIZE(natts)                                                            \
	(sizeof(CompressionAmInfo) + (sizeof(ColumnCompressionSettings) * (natts)))

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

static const TableAmRoutine *
switch_to_heapam(Relation rel)
{
	const TableAmRoutine *tableam = rel->rd_tableam;
	Assert(tableam == compressionam_routine());
	rel->rd_tableam = GetHeapamTableAmRoutine();
	return tableam;
}

static CompressionAmInfo *
lazy_build_compressionam_info_cache(Relation rel, bool missing_compressed_ok,
									bool *compressed_relation_created)
{
	Assert(OidIsValid(rel->rd_id) && !ts_is_hypertable(rel->rd_id));

	CompressionAmInfo *caminfo;
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
	caminfo = MemoryContextAllocZero(CacheMemoryContext, COMPRESSION_AM_INFO_SIZE(tupdesc->natts));
	caminfo->relation_id = get_chunk_id_from_relid(rel->rd_id);
	caminfo->compressed_relid = InvalidOid;
	caminfo->num_columns = tupdesc->natts;
	caminfo->hypertable_id = hyper_id;

	/* Only optionally include information about the compressed chunk because
	 * it might not exist when this cache is built. The information will be
	 * added the next time the function is called instead. */
	FormData_chunk form = ts_chunk_get_formdata(caminfo->relation_id);
	caminfo->compressed_relation_id = form.compressed_chunk_id;

	/* Create compressed chunk and set the created flag if it does not
	 * exist. */
	if (compressed_relation_created)
		*compressed_relation_created = (caminfo->compressed_relation_id == 0);
	if (caminfo->compressed_relation_id == 0)
	{
		/* Consider if we want to make it simpler to create the compressed
		 * table by just considering a normal side-relation with no strong
		 * connection to the original chunk. We do not need constraints,
		 * foreign keys, or any other things on this table since it never
		 * participate in any plans. */

		Chunk *chunk = ts_chunk_get_by_relid(rel->rd_id, true);
		Ensure(chunk, "\"%s\" is not a chunk", get_rel_name(rel->rd_id));
		Hypertable *ht = ts_hypertable_get_by_id(chunk->fd.hypertable_id);
		Hypertable *ht_compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
		Chunk *c_chunk = create_compress_chunk(ht_compressed, chunk, InvalidOid);
		ts_chunk_constraints_create(ht_compressed, c_chunk);
		ts_trigger_create_all_on_chunk(c_chunk);
		ts_chunk_set_compressed_chunk(chunk, c_chunk->fd.id);

		caminfo->compressed_relation_id = c_chunk->fd.id;
	}

	caminfo->compressed_relid = ts_chunk_get_relid(caminfo->compressed_relation_id, false);
	caminfo->count_cattno =
		get_attnum(caminfo->compressed_relid, COMPRESSION_COLUMN_METADATA_COUNT_NAME);

	Assert(caminfo->compressed_relation_id > 0 && OidIsValid(caminfo->compressed_relid));
	Assert(caminfo->count_cattno != InvalidAttrNumber);

	for (int i = 0; i < caminfo->num_columns; i++)
	{
		const Form_pg_attribute attr = &tupdesc->attrs[i];
		ColumnCompressionSettings *colsettings = &caminfo->columns[i];

		if (attr->attisdropped)
		{
			colsettings->attnum = InvalidAttrNumber;
			colsettings->cattnum = InvalidAttrNumber;
			colsettings->is_dropped = true;
			continue;
		}

		const char *attname = NameStr(attr->attname);
		int segmentby_pos = ts_array_position(settings->fd.segmentby, attname);
		int orderby_pos = ts_array_position(settings->fd.orderby, attname);

		namestrcpy(&colsettings->attname, attname);
		colsettings->attnum = attr->attnum;
		colsettings->typid = attr->atttypid;
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

		if (OidIsValid(caminfo->compressed_relid))
			colsettings->cattnum = get_attnum(caminfo->compressed_relid, attname);
		else
			colsettings->cattnum = InvalidAttrNumber;
	}

	Assert(caminfo->num_segmentby == ts_array_length(settings->fd.segmentby));
	Assert(caminfo->num_orderby == ts_array_length(settings->fd.orderby));
	Ensure(caminfo->relation_id > 0, "invalid chunk ID");

	return caminfo;
}

CompressionAmInfo *
RelationGetCompressionAmInfo(Relation rel)
{
	if (NULL == rel->rd_amcache)
		rel->rd_amcache = lazy_build_compressionam_info_cache(rel, false, NULL);

	Assert(rel->rd_amcache &&
		   OidIsValid(((CompressionAmInfo *) rel->rd_amcache)->compressed_relid));

	return rel->rd_amcache;
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

typedef struct CompressionParallelScanDescData
{
	ParallelBlockTableScanDescData pscandesc;
	ParallelBlockTableScanDescData cpscandesc;
} CompressionParallelScanDescData;

typedef struct CompressionParallelScanDescData *CompressionParallelScanDesc;

typedef struct CompressionScanDescData
{
	TableScanDescData rs_base;
	TableScanDesc uscan_desc; /* scan descriptor for non-compressed relation */
	Relation compressed_rel;
	TableScanDesc cscan_desc; /* scan descriptor for compressed relation */
	int64 returned_noncompressed_count;
	int64 returned_compressed_count;
	int32 compressed_row_count;
	bool compressed_read_done;
	bool reset;
} CompressionScanDescData;

typedef struct CompressionScanDescData *CompressionScanDesc;

/*
 * Initialization common for beginscan and rescan.
 */
static void
initscan(CompressionScanDesc scan, ScanKey keys, int nkeys)
{
	int nvalidkeys = 0;

	/*
	 * Translate any scankeys to the corresponding scankeys on the compressed
	 * relation.
	 *
	 * It is only possible to use scankeys in the following two cases:
	 *
	 * 1. The scankey is for a segment_by column
	 * 2. The scankey is for a column that has min/max metadata (i.e., order_by column).
	 *
	 * TODO: Implement support for (2) above, which involves transforming a
	 * scankey to the corresponding min/max scankeys.
	 */
	if (NULL != keys && nkeys > 0)
	{
		const CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(scan->rs_base.rs_rd);

		for (int i = 0; i < nkeys; i++)
		{
			const ScanKey key = &keys[i];

			for (int j = 0; j < caminfo->num_columns; j++)
			{
				const ColumnCompressionSettings *column = &caminfo->columns[j];

				if (column->is_segmentby && key->sk_attno == column->attnum)
				{
					scan->rs_base.rs_key[nvalidkeys] = *key;
					/* Remap the attribute number to the corresponding
					 * compressed rel attribute number */
					scan->rs_base.rs_key[nvalidkeys].sk_attno = column->cattnum;
					nvalidkeys++;
					break;
				}
			}
		}
	}

	scan->rs_base.rs_nkeys = nvalidkeys;

	/* Use the TableScanDescData's scankeys to store the transformed compression scan keys */
	if (scan->rs_base.rs_flags & SO_TYPE_SEQSCAN)
		pgstat_count_compression_scan(scan->rs_base.rs_rd);
}

static const char *
get_scan_type(uint32 flags)
{
	if (flags & SO_TYPE_TIDSCAN)
		return "TID";
#if PG14_GE
	if (flags & SO_TYPE_TIDRANGESCAN)
		return "TID range";
#endif
	if (flags & SO_TYPE_BITMAPSCAN)
		return "bitmap";
	if (flags & SO_TYPE_SAMPLESCAN)
		return "sample";
	if (flags & SO_TYPE_ANALYZE)
		return "analyze";
	if (flags & SO_TYPE_SEQSCAN)
		return "sequence";
	return "unknown";
}

static TableScanDesc
compressionam_beginscan(Relation relation, Snapshot snapshot, int nkeys, ScanKey keys,
						ParallelTableScanDesc parallel_scan, uint32 flags)
{
	CompressionScanDesc scan;
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(relation);
	CompressionParallelScanDesc cpscan = (CompressionParallelScanDesc) parallel_scan;

	RelationIncrementReferenceCount(relation);

	elog(DEBUG2,
		 "%d starting %s scan of relation %s parallel_scan=%p",
#if PG17_GE
		 MyProcNumber,
#else
		 MyBackendId,
#endif
		 get_scan_type(flags),
		 RelationGetRelationName(relation),
		 parallel_scan);

	scan = palloc0(sizeof(CompressionScanDescData));
	scan->rs_base.rs_rd = relation;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_key = nkeys > 0 ? palloc0(sizeof(ScanKeyData) * nkeys) : NULL;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = parallel_scan;
	scan->compressed_rel = table_open(caminfo->compressed_relid, AccessShareLock);
	scan->returned_noncompressed_count = 0;
	scan->returned_compressed_count = 0;
	scan->compressed_row_count = 0;
	scan->reset = true;

	/*
	 * Don't read compressed data if transparent decompression is enabled or
	 * it is requested by the scan.
	 *
	 * Transparent decompression reads compressed data itself, directly from
	 * the compressed chunk, so avoid reading it again here.
	 */
	scan->compressed_read_done = (ts_guc_enable_transparent_decompression == 2) ||
								 (keys && keys->sk_flags & SK_NO_COMPRESSED);

	initscan(scan, keys, nkeys);

	ParallelTableScanDesc ptscan =
		parallel_scan ? (ParallelTableScanDesc) &cpscan->pscandesc : NULL;

	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	scan->uscan_desc =
		relation->rd_tableam->scan_begin(relation, snapshot, nkeys, keys, ptscan, flags);
	relation->rd_tableam = oldtam;

	if (parallel_scan)
	{
		/* Parallel workers use a serialized snapshot that they get from the
		 * coordinator. The snapshot will be marked as a temp snapshot so that
		 * endscan() knows to deregister it. However, if we pass the snapshot
		 * to both sub-scans marked as a temp snapshot it will be deregistered
		 * twice. Therefore remove the temp flag for the second scan. */
		flags &= ~SO_TEMP_SNAPSHOT;
	}

	ParallelTableScanDesc cptscan =
		parallel_scan ? (ParallelTableScanDesc) &cpscan->cpscandesc : NULL;
	Relation crel = scan->compressed_rel;
	scan->cscan_desc = crel->rd_tableam->scan_begin(scan->compressed_rel,
													snapshot,
													scan->rs_base.rs_nkeys,
													scan->rs_base.rs_key,
													cptscan,
													flags);

	return &scan->rs_base;
}

static void
compressionam_rescan(TableScanDesc sscan, ScanKey key, bool set_params, bool allow_strat,
					 bool allow_sync, bool allow_pagemode)
{
	CompressionScanDesc scan = (CompressionScanDesc) sscan;

	initscan(scan, key, scan->rs_base.rs_nkeys);
	scan->reset = true;
	scan->compressed_read_done = false;

	table_rescan(scan->cscan_desc, key);

	Relation relation = scan->uscan_desc->rs_rd;
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam
		->scan_rescan(scan->uscan_desc, key, set_params, allow_strat, allow_sync, allow_pagemode);
	relation->rd_tableam = oldtam;
}

static void
compressionam_endscan(TableScanDesc sscan)
{
	CompressionScanDesc scan = (CompressionScanDesc) sscan;

	RelationDecrementReferenceCount(sscan->rs_rd);
	table_endscan(scan->cscan_desc);
	table_close(scan->compressed_rel, AccessShareLock);

	Relation relation = sscan->rs_rd;
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam->scan_end(scan->uscan_desc);
	relation->rd_tableam = oldtam;

	elog(DEBUG2,
		 "scanned " INT64_FORMAT " tuples (" INT64_FORMAT " compressed, " INT64_FORMAT
		 " noncompressed) in rel %s",
		 scan->returned_compressed_count + scan->returned_noncompressed_count,
		 scan->returned_compressed_count,
		 scan->returned_noncompressed_count,
		 RelationGetRelationName(sscan->rs_rd));

	if (scan->rs_base.rs_key)
		pfree(scan->rs_base.rs_key);

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
		child_slot = arrow_slot_get_noncompressed_slot(slot);

		Relation relation = sscan->rs_rd;
		const TableAmRoutine *oldtam = switch_to_heapam(relation);
		bool result =
			relation->rd_tableam->scan_getnextslot(scan->uscan_desc, direction, child_slot);
		relation->rd_tableam = oldtam;

		if (result)
		{
			scan->returned_noncompressed_count++;
			ExecStoreArrowTuple(slot, InvalidTupleIndex);
		}

		return result;
	}

	child_slot = arrow_slot_get_compressed_slot(slot, RelationGetDescr(scan->compressed_rel));

	if (scan->reset || arrow_slot_is_last(slot) || arrow_slot_is_consumed(slot))
	{
		scan->reset = false;

		if (!table_scan_getnextslot(scan->cscan_desc, direction, child_slot))
		{
			ExecClearTuple(slot);
			scan->compressed_read_done = true;
			return compressionam_getnextslot(sscan, direction, slot);
		}

		Assert(ItemPointerIsValid(&child_slot->tts_tid));
		ExecStoreArrowTuple(slot, 1);
		scan->compressed_row_count = arrow_slot_total_row_count(slot);
	}
	else
	{
		ExecStoreNextArrowTuple(slot);
	}

	scan->returned_compressed_count++;
	pgstat_count_compression_getnext(sscan->rs_rd);

	return true;
}

static Size
compressionam_parallelscan_estimate(Relation rel)
{
	return sizeof(CompressionParallelScanDescData);
}

/*
 * Initialize ParallelTableScanDesc for a parallel scan of this relation.
 * `pscan` will be sized according to parallelscan_estimate() for the same
 * relation.
 */
static Size
compressionam_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	CompressionParallelScanDesc cpscan = (CompressionParallelScanDesc) pscan;
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	table_block_parallelscan_initialize(rel, (ParallelTableScanDesc) &cpscan->pscandesc);
	rel->rd_tableam = oldtam;

	Relation crel = table_open(caminfo->compressed_relid, AccessShareLock);
	table_block_parallelscan_initialize(crel, (ParallelTableScanDesc) &cpscan->cpscandesc);
	table_close(crel, NoLock);

	return sizeof(CompressionParallelScanDescData);
}

/*
 * Reinitialize `pscan` for a new scan. `rel` will be the same relation as
 * when `pscan` was initialized by parallelscan_initialize.
 */
static void
compressionam_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	CompressionParallelScanDesc cpscan = (CompressionParallelScanDesc) pscan;
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	table_block_parallelscan_reinitialize(rel, (ParallelTableScanDesc) &cpscan->pscandesc);
	rel->rd_tableam = oldtam;

	Relation crel = table_open(caminfo->compressed_relid, AccessShareLock);
	table_block_parallelscan_reinitialize(crel, (ParallelTableScanDesc) &cpscan->cpscandesc);
	table_close(crel, NoLock);
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
	/* Inserts only supported in non-compressed relation, so simply forward to the heap AM */
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam->multi_insert(relation, slots, ntuples, cid, options, bistate);
	relation->rd_tableam = oldtam;

	TS_WITH_MEMORY_CONTEXT(CurTransactionContext, {
		partially_compressed_relids =
			list_append_unique_oid(partially_compressed_relids, RelationGetRelid(relation));
	});
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
	IndexFetchComprData *cscan = palloc0(sizeof(IndexFetchComprData));
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);

	Relation crel = table_open(caminfo->compressed_relid, AccessShareLock);
	cscan->h_base.rel = rel;
	cscan->compr_rel = crel;
	cscan->compr_hscan = crel->rd_tableam->index_fetch_begin(crel);

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	cscan->uncompr_hscan = rel->rd_tableam->index_fetch_begin(rel);
	rel->rd_tableam = oldtam;

	ItemPointerSetInvalid(&cscan->tid);

	return &cscan->h_base;
}

static void
compressionam_index_fetch_reset(IndexFetchTableData *scan)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;
	Relation rel = scan->rel;

	ItemPointerSetInvalid(&cscan->tid);
	cscan->compr_rel->rd_tableam->index_fetch_reset(cscan->compr_hscan);

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	rel->rd_tableam->index_fetch_reset(cscan->uncompr_hscan);
	rel->rd_tableam = oldtam;
}

static void
compressionam_index_fetch_end(IndexFetchTableData *scan)
{
	IndexFetchComprData *cscan = (IndexFetchComprData *) scan;
	Relation rel = scan->rel;

	Relation crel = cscan->compr_rel;
	crel->rd_tableam->index_fetch_end(cscan->compr_hscan);
	table_close(crel, AccessShareLock);

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	rel->rd_tableam->index_fetch_end(cscan->uncompr_hscan);
	rel->rd_tableam = oldtam;
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
	Relation rel = scan->rel;
	Relation crel = cscan->compr_rel;

	ItemPointerData decoded_tid;

	if (!is_compressed_tid(tid))
	{
		child_slot = arrow_slot_get_noncompressed_slot(slot);

		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		bool result = rel->rd_tableam->index_fetch_tuple(cscan->uncompr_hscan,
														 tid,
														 snapshot,
														 child_slot,
														 call_again,
														 all_dead);
		rel->rd_tableam = oldtam;

		if (result)
		{
			slot->tts_tableOid = RelationGetRelid(scan->rel);
			ExecStoreArrowTuple(slot, InvalidTupleIndex);
		}

		return result;
	}

	/* Compressed tuples not visible through this TAM when scanned by
	 * transparent decompression enabled since DecompressChunk already scanned
	 * that data. */
	if (ts_guc_enable_transparent_decompression == 2)
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
		Assert(slot->tts_tableOid == RelationGetRelid(scan->rel));
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
		slot->tts_tableOid = RelationGetRelid(scan->rel);
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
	bool result;
	ItemPointerData decoded_tid;
	const uint16 tuple_index =
		is_compressed_tid(tid) ? compressed_tid_to_tid(&decoded_tid, tid) : InvalidTupleIndex;

	if (!is_compressed_tid(tid))
	{
		/*
		 * For non-compressed tuples, we fetch the tuple and copy it into the
		 * destination slot.
		 *
		 * We need to have a new slot for the call since the heap AM expects a
		 * BufferHeap TTS and we cannot pass down our Arrow TTS.
		 */
		TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
		const TableAmRoutine *oldtam = switch_to_heapam(relation);
		result = relation->rd_tableam->tuple_fetch_row_version(relation, tid, snapshot, child_slot);
		relation->rd_tableam = oldtam;
	}
	else
	{
		CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(relation);
		Relation child_rel = table_open(caminfo->compressed_relid, AccessShareLock);
		TupleTableSlot *child_slot =
			arrow_slot_get_compressed_slot(slot, RelationGetDescr(child_rel));
		result = table_tuple_fetch_row_version(child_rel, &decoded_tid, snapshot, child_slot);
		table_close(child_rel, NoLock);
	}

	if (result)
	{
		slot->tts_tableOid = RelationGetRelid(relation);
		ExecStoreArrowTuple(slot, tuple_index);
	}

	return result;
}

static bool
compressionam_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	CompressionScanDescData *cscan = (CompressionScanDescData *) scan;
	ItemPointerData ctid;

	if (!is_compressed_tid(tid))
	{
		Relation rel = scan->rs_rd;
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		bool valid = rel->rd_tableam->tuple_tid_valid(cscan->uscan_desc, tid);
		rel->rd_tableam = oldtam;
		return valid;
	}

	(void) compressed_tid_to_tid(&ctid, tid);
	return cscan->compressed_rel->rd_tableam->tuple_tid_valid(cscan->cscan_desc, &ctid);
}

static bool
compressionam_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);
	bool result;

	if (is_compressed_tid(&slot->tts_tid))
	{
		Relation crel = table_open(caminfo->compressed_relid, AccessShareLock);
		TupleTableSlot *child_slot = arrow_slot_get_compressed_slot(slot, NULL);
		result = crel->rd_tableam->tuple_satisfies_snapshot(crel, child_slot, snapshot);
		table_close(crel, AccessShareLock);
	}
	else
	{
		TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		result = rel->rd_tableam->tuple_satisfies_snapshot(rel, child_slot, snapshot);
		rel->rd_tableam = oldtam;
	}
	return result;
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

	/* Reset the deltids array before recreating it with the result */
	delstate->ndeltids = 0;

	if (noncompr_delstate.ndeltids > 0 &&
		(total_knowndeletable_non_compressed > 0 || delstate->bottomup))
	{
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		xid_noncompr = rel->rd_tableam->index_delete_tuples(rel, &noncompr_delstate);
		rel->rd_tableam = oldtam;
		memcpy(delstate->deltids,
			   noncompr_delstate.deltids,
			   noncompr_delstate.ndeltids * sizeof(TM_IndexDelete));
		delstate->ndeltids = noncompr_delstate.ndeltids;
	}

	if (compr_delstate.ndeltids > 0 && (total_knowndeletable_compressed > 0 || delstate->bottomup))
	{
		/* Assume RowExclusivelock since this involves deleting tuples */
		Relation compr_rel = table_open(caminfo->compressed_relid, RowExclusiveLock);

		xid_compr = compr_rel->rd_tableam->index_delete_tuples(compr_rel, &compr_delstate);

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
	{
		if (conversionstate->tuplesortstate)
		{
			tuplesort_puttupleslot(conversionstate->tuplesortstate, slot);
			return;
		}

		/* If no tuplesortstate is set, conversion is happening from legacy
		 * compression where a compressed relation already exists. Therefore,
		 * there is no need to recompress; just insert the non-compressed data
		 * into the new heap. */
	}

	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam->tuple_insert(relation, slot, cid, options, bistate);
	relation->rd_tableam = oldtam;

	TS_WITH_MEMORY_CONTEXT(CurTransactionContext, {
		partially_compressed_relids =
			list_append_unique_oid(partially_compressed_relids, RelationGetRelid(relation));
	});
}

static void
compressionam_tuple_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
									   int options, BulkInsertStateData *bistate, uint32 specToken)
{
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam
		->tuple_insert_speculative(relation, slot, cid, options, bistate, specToken);
	relation->rd_tableam = oldtam;
}

static void
compressionam_tuple_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 specToken,
										 bool succeeded)
{
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam->tuple_complete_speculative(relation, slot, specToken, succeeded);
	relation->rd_tableam = oldtam;
}

static TM_Result
compressionam_tuple_delete(Relation relation, ItemPointer tid, CommandId cid, Snapshot snapshot,
						   Snapshot crosscheck, bool wait, TM_FailureData *tmfd, bool changingPart)
{
	if (!is_compressed_tid(tid))
	{
		/* Just pass this on to regular heap AM */
		const TableAmRoutine *oldtam = switch_to_heapam(relation);
		TM_Result result =
			relation->rd_tableam
				->tuple_delete(relation, tid, cid, snapshot, crosscheck, wait, tmfd, changingPart);
		relation->rd_tableam = oldtam;
		return result;
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
		const TableAmRoutine *oldtam = switch_to_heapam(relation);
		TM_Result result = relation->rd_tableam->tuple_update(relation,
															  otid,
															  slot,
															  cid,
															  snapshot,
															  crosscheck,
															  wait,
															  tmfd,
															  lockmode,
															  update_indexes);
		relation->rd_tableam = oldtam;
		return result;
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
		{
			slot->tts_tableOid = RelationGetRelid(relation);
			ExecStoreArrowTuple(slot, tuple_index);
		}

		table_close(crel, NoLock);
	}
	else
	{
		TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
		const TableAmRoutine *oldtam = switch_to_heapam(relation);
		result = relation->rd_tableam->tuple_lock(relation,
												  tid,
												  snapshot,
												  child_slot,
												  cid,
												  mode,
												  wait_policy,
												  flags,
												  tmfd);
		relation->rd_tableam = oldtam;

		if (result == TM_Ok)
		{
			slot->tts_tableOid = RelationGetRelid(relation);
			ExecStoreArrowTuple(slot, InvalidTupleIndex);
		}
	}

	return result;
}

static void
compressionam_finish_bulk_insert(Relation rel, int options)
{
	if (conversionstate)
		convert_to_compressionam_finish(RelationGetRelid(rel));
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
	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	rel->rd_tableam->relation_set_new_filelocator(rel,
												  newrlocator,
												  persistence,
												  freezeXid,
												  minmulti);
	rel->rd_tableam = oldtam;
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

	/* TODO: Vacuum the uncompressed relation */

	/* The compressed relation can be vacuumed too, but might not need it
	 * unless we do a lot of insert/deletes of compressed rows */
	crel->rd_tableam->relation_vacuum(crel, params, bstrategy);
	table_close(crel, NoLock);
}

/*
 * Analyze the next block with the given blockno.
 *
 * The underlying ANALYZE functionality that calls this function samples
 * blocks in the relation. To be able to analyze all the blocks across both
 * the non-compressed and the compressed relations, this function relies on
 * the TAM giving the impression that the total number of blocks is the sum of
 * compressed and non-compressed blocks. This is done by returning the sum of
 * the total number of blocks across both relations in the relation_size() TAM
 * callback.
 *
 * The non-compressed relation is sampled first, and, only once the blockno
 * increases beyond the number of blocks in the non-compressed relation, the
 * compressed relation is sampled.
 */
static bool
compressionam_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
									  BufferAccessStrategy bstrategy)
{
	CompressionScanDescData *cscan = (CompressionScanDescData *) scan;
	HeapScanDesc uhscan = (HeapScanDesc) cscan->uscan_desc;

	/* If blockno is past the blocks in the non-compressed relation, we should
	 * analyze the compressed relation */
	if (blockno >= uhscan->rs_nblocks)
	{
		/* Get the compressed rel blockno by subtracting the number of
		 * non-compressed blocks */
		blockno -= uhscan->rs_nblocks;
		return cscan->compressed_rel->rd_tableam->scan_analyze_next_block(cscan->cscan_desc,
																		  blockno,
																		  bstrategy);
	}

	Relation rel = scan->rs_rd;
	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	bool result = rel->rd_tableam->scan_analyze_next_block(cscan->uscan_desc, blockno, bstrategy);
	rel->rd_tableam = oldtam;

	return result;
}

/*
 * Get the next tuple to sample during ANALYZE.
 *
 * Since the sampling happens across both the non-compressed and compressed
 * relations, it is necessary to determine from which relation to return a
 * tuple. This is driven by scan_analyze_next_block() above.
 *
 * When sampling from the compressed relation, a compressed segment is read
 * and it is then necessary to return all tuples in the segment.
 *
 * NOTE: the function currently relies on heapAM's scan_analyze_next_tuple()
 * to read compressed segments. This can lead to misrepresenting liverows and
 * deadrows numbers since heap AM might skip tuples that are dead or
 * concurrently inserted, but still count them in liverows or deadrows. Each
 * compressed tuple represents many rows, but heapAM only counts each
 * compressed tuple as one row. The only way to fix this is to either check
 * the diff between the count before and after calling the heap AM function
 * and then estimate the actual number of rows from that, or, reimplement the
 * heapam_scan_analyze_next_tuple() function so that it can properly account
 * for compressed tuples.
 */
static bool
compressionam_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin,
									  double *liverows, double *deadrows, TupleTableSlot *slot)
{
	CompressionScanDescData *cscan = (CompressionScanDescData *) scan;
	HeapScanDesc chscan = (HeapScanDesc) cscan->cscan_desc;
	uint16 tuple_index;
	bool result;

	/*
	 * Since non-compressed blocks are always sampled first, the current
	 * buffer for the compressed relation will be invalid until we reach the
	 * end of the non-compressed blocks.
	 */
	if (chscan->rs_cbuf != InvalidBuffer)
	{
		/* Keep on returning tuples from the compressed segment until it is
		 * consumed */
		if (!TTS_EMPTY(slot))
		{
			tuple_index = arrow_slot_row_index(slot);

			if (tuple_index != InvalidTupleIndex && !arrow_slot_is_last(slot))
			{
				ExecIncrArrowTuple(slot, 1);
				*liverows += 1;
				return true;
			}
		}

		TupleTableSlot *child_slot =
			arrow_slot_get_compressed_slot(slot, RelationGetDescr(cscan->compressed_rel));

		result = cscan->compressed_rel->rd_tableam->scan_analyze_next_tuple(cscan->cscan_desc,
																			OldestXmin,
																			liverows,
																			deadrows,
																			child_slot);
		/* Need to pick a row from the segment to sample. Might as well pick
		 * the first one, but might consider picking a random one. */
		tuple_index = 1;
	}
	else
	{
		TupleTableSlot *child_slot = arrow_slot_get_noncompressed_slot(slot);
		Relation rel = scan->rs_rd;
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		result = rel->rd_tableam->scan_analyze_next_tuple(cscan->uscan_desc,
														  OldestXmin,
														  liverows,
														  deadrows,
														  child_slot);
		rel->rd_tableam = oldtam;
		tuple_index = InvalidTupleIndex;
	}

	if (result)
	{
		slot->tts_tableOid = RelationGetRelid(scan->rs_rd);
		ExecStoreArrowTuple(slot, tuple_index);
	}
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
	Bitmapset *segmentby_cols;
	Bitmapset *orderby_cols;
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
	const Bitmapset *segmentby_cols = icstate->segmentby_cols;

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
		icstate->ntuples++;
	}
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
	/*
	 * We can be called from ProcessUtility with a hypertable because we need
	 * to process all ALTER TABLE commands in the list to set options
	 * correctly for the hypertable.
	 *
	 * If we are called on a hypertable, we just skip scanning for tuples and
	 * say that the relation was empty.
	 */
	if (ts_is_hypertable(relation->rd_id))
		return 0.0;

	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(relation);

	Relation crel = table_open(caminfo->compressed_relid, AccessShareLock);
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
		.arrow_columns = (ArrowArray **) palloc(sizeof(ArrowArray *) * indexInfo->ii_NumIndexAttrs),
	};
	IndexInfo iinfo = *indexInfo;

	build_segment_and_orderby_bms(caminfo, &icstate.segmentby_cols, &icstate.orderby_cols);

	/* Translate index attribute numbers for the compressed relation */
	for (int i = 0; i < indexInfo->ii_NumIndexAttrs; i++)
	{
		const AttrNumber attno = indexInfo->ii_IndexAttrNumbers[i];
		const AttrNumber cattno = caminfo->columns[AttrNumberGetAttrOffset(attno)].cattnum;

		iinfo.ii_IndexAttrNumbers[i] = cattno;
		icstate.arrow_columns[i] = NULL;
	}

	/* Include the count column in the callback. It is needed to know the
	 * uncompressed tuple count in case of building an index on the segmentby
	 * column. */
	iinfo.ii_IndexAttrNumbers[iinfo.ii_NumIndexAttrs++] = caminfo->count_cattno;

	/* Check uniqueness on compressed */
	iinfo.ii_Unique = false;
	iinfo.ii_ExclusionOps = NULL;
	iinfo.ii_Predicate = NULL;

	/* Call heap's index_build_range_scan() on the compressed relation. The
	 * custom callback we give it will "unwrap" the compressed segments into
	 * individual tuples. Therefore, we cannot use the tuple count returned by
	 * the function since it only represent the number of compressed
	 * tuples. Instead, tuples are counted in the callback state. */
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
	pfree((void *) icstate.arrow_columns);
	bms_free(icstate.segmentby_cols);
	bms_free(icstate.orderby_cols);

	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	icstate.ntuples += relation->rd_tableam->index_build_range_scan(relation,
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
	relation->rd_tableam = oldtam;

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

/*
 * Return the relation size in bytes.
 *
 * The relation size in bytes is computed from the number of blocks in the
 * relation multiplied by the block size.
 *
 * However, since the compression TAM is a "meta" relation over separate
 * non-compressed and compressed heaps, the total size is actually the sum of
 * the number of blocks in both heaps.
 *
 * To get the true size of the TAM (non-compressed) relation, it is possible
 * to use switch_to_heapam() and bypass the TAM callbacks.
 */
static uint64
compressionam_relation_size(Relation rel, ForkNumber forkNumber)
{
	CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);
	uint64 ubytes = table_block_relation_size(rel, forkNumber);
	/* For ANALYZE, need to return sum for both relations. */
	Relation crel = try_relation_open(caminfo->compressed_relid, AccessShareLock);

	if (crel == NULL)
		return ubytes;

	uint64 cbytes = table_block_relation_size(crel, forkNumber);
	relation_close(crel, NoLock);

	return ubytes + cbytes;
}

static void
compressionam_relation_estimate_size(Relation rel, int32 *attr_widths, BlockNumber *pages,
									 double *tuples, double *allvisfrac)
{
	/*
	 * We can be called from ProcessUtility with a hypertable because we need
	 * to process all ALTER TABLE commands in the list to set options
	 * correctly for the hypertable.
	 *
	 * If we are called on a hypertable, we just say that the hypertable does
	 * not have any pages or tuples.
	 */
	if (ts_is_hypertable(rel->rd_id))
	{
		*pages = 0;
		*allvisfrac = 0;
		*tuples = 0;
		return;
	}

	double ctuples;
	double callvisfrac;
	BlockNumber cpages;
	BlockNumber totalpages;
	BlockNumber relallvisible;
	Relation crel;

	const CompressionAmInfo *caminfo = RelationGetCompressionAmInfo(rel);

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	rel->rd_tableam->relation_estimate_size(rel, attr_widths, pages, tuples, allvisfrac);
	rel->rd_tableam = oldtam;

	/* Cannot pass on attr_widths since they are cached widths for the
	 * non-compressed relation which also doesn't have the same number of
	 * attribute (columns). If we pass NULL it will compute the estimate
	 * instead. */
	crel = table_open(caminfo->compressed_relid, AccessShareLock);
	crel->rd_tableam->relation_estimate_size(crel, NULL, &cpages, &ctuples, &callvisfrac);
	table_close(crel, NoLock);

	totalpages = *pages + cpages;

	if (totalpages == 0)
	{
		*pages = 0;
		*allvisfrac = 0;
		*tuples = 0;
		return;
	}

	relallvisible =
		(BlockNumber) rel->rd_rel->relallvisible + (BlockNumber) crel->rd_rel->relallvisible;

	if (relallvisible == 0 || totalpages <= 0)
		*allvisfrac = 0;
	else if (relallvisible >= totalpages)
		*allvisfrac = 1;
	else
		*allvisfrac = (double) relallvisible / totalpages;

	*pages = totalpages;
	*tuples = (ctuples * GLOBAL_MAX_ROWS_PER_COMPRESSION) + *tuples;
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

/*
 * Convert a table to compression AM.
 *
 * Need to setup the conversion state used to compress the data.
 */
static void
convert_to_compressionam(Oid relid)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(CurTransactionContext);
	ConversionState *state = palloc0(sizeof(ConversionState));
	Relation relation = table_open(relid, AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(relation);
	bool compress_chunk_created;
	CompressionAmInfo *caminfo =
		lazy_build_compressionam_info_cache(relation, true, &compress_chunk_created);

	state->relid = relid;

	if (!compress_chunk_created)
	{
		/* A compressed relation already exists, so converting from legacy
		 * compression. It is only necessary to write the non-compressed data
		 * to the new heap. */
		table_close(relation, AccessShareLock);
		return;
	}

	/*
	 * We compress the existing non-compressed data in the table when moving
	 * to a table access method.
	 *
	 * TODO: Consider if we should compress data when creating a
	 * hyperstore. It might make more sense to just "adopt" the table in the
	 * state they are and deal with compression as part of the normal
	 * procedure.
	 */
	AttrNumber *sort_keys = palloc0(sizeof(*sort_keys) * caminfo->num_keys);
	Oid *sort_operators = palloc0(sizeof(*sort_operators) * caminfo->num_keys);
	Oid *sort_collations = palloc0(sizeof(*sort_collations) * caminfo->num_keys);
	bool *nulls_first = palloc0(sizeof(*nulls_first) * caminfo->num_keys);
	int segmentby_pos = 0;
	int orderby_pos = 0;

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

	table_close(relation, AccessShareLock);
	conversionstate = state;
	MemoryContextSwitchTo(oldcxt);
}

/*
 * List of relation IDs used to clean up the compressed relation when
 * converting from compression TAM to another TAM (typically heap).
 */
static List *cleanup_relids = NIL;

static void
cleanup_compression_relations(void)
{
	if (cleanup_relids != NIL)
	{
		ListCell *lc;

		foreach (lc, cleanup_relids)
		{
			Oid relid = lfirst_oid(lc);
			Chunk *chunk = ts_chunk_get_by_relid(relid, true);
			Chunk *compress_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, false);

			ts_chunk_clear_compressed_chunk(chunk);

			if (compress_chunk)
				ts_chunk_drop(compress_chunk, DROP_RESTRICT, -1);
		}

		list_free(cleanup_relids);
		cleanup_relids = NIL;
	}
}

void
compressionam_xact_event(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		{
			ListCell *lc;
			/* Check for relations that might now be partially compressed and
			 * update their status */
			foreach (lc, partially_compressed_relids)
			{
				Oid relid = lfirst_oid(lc);
				Chunk *chunk = ts_chunk_get_by_relid(relid, true);
				ts_chunk_set_partial(chunk);
			}
			break;
		}
		default:
			break;
	}

	if (partially_compressed_relids != NIL)
	{
		list_free(partially_compressed_relids);
		partially_compressed_relids = NIL;
	}

	/*
	 * Cleanup in case of aborted transcation. Need not explicitly check for
	 * abort since the states should only exist if it is an abort.
	 */
	if (cleanup_relids != NIL)
	{
		list_free(cleanup_relids);
		cleanup_relids = NIL;
	}

	if (conversionstate)
	{
		if (conversionstate->tuplesortstate)
			tuplesort_end(conversionstate->tuplesortstate);
		pfree(conversionstate);
		conversionstate = NULL;
	}
}

static void
convert_to_compressionam_finish(Oid relid)
{
	if (!conversionstate->tuplesortstate)
	{
		/* Without a tuple sort state, conversion happens from legacy
		 * compression where a compressed relation (chunk) already
		 * exists. There's nothing more to do. */
		return;
	}

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

/*
 * Convert the chunk away from compression AM to another table access method.
 * When this happens it is necessary to cleanup metadata.
 */
static void
convert_from_compressionam(Oid relid)
{
	int32 chunk_id = get_chunk_id_from_relid(relid);
	ts_compression_chunk_size_delete(chunk_id);

	/* Need to truncate the compressed relation after converting from compression TAM */
	TS_WITH_MEMORY_CONTEXT(CurTransactionContext,
						   { cleanup_relids = lappend_oid(cleanup_relids, relid); });
}

void
compressionam_alter_access_method_begin(Oid relid, bool to_other_am)
{
	if (to_other_am)
		convert_from_compressionam(relid);
	else
		convert_to_compressionam(relid);
}

/*
 * Called at the end of converting a chunk to a table access method.
 */
void
compressionam_alter_access_method_finish(Oid relid, bool to_other_am)
{
	if (to_other_am)
		cleanup_compression_relations();

	/* Finishing the conversion to compression TAM is handled in
	 * the finish_bulk_insert callback */
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
	.parallelscan_estimate = compressionam_parallelscan_estimate,
	.parallelscan_initialize = compressionam_parallelscan_initialize,
	.parallelscan_reinitialize = compressionam_parallelscan_reinitialize,

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

	/* We do not support bitmap heap scan at this point. */
	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,

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
