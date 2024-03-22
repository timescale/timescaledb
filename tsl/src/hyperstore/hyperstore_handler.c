/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/heapam.h>
#include <access/hio.h>
#include <access/rewriteheap.h>
#include <access/skey.h>
#include <access/tableam.h>

#include <access/transam.h>
#include <access/xact.h>
#include <catalog/heap.h>
#include <catalog/index.h>
#include <catalog/pg_attribute.h>
#include <catalog/storage.h>
#include <commands/progress.h>
#include <commands/vacuum.h>
#include <executor/tuptable.h>
#include <nodes/bitmapset.h>
#include <nodes/execnodes.h>
#include <nodes/makefuncs.h>
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
#include <storage/lmgr.h>
#include <storage/lockdefs.h>
#include <storage/off.h>
#include <utils/builtins.h>
#include <utils/elog.h>
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/palloc.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <utils/tuplesort.h>
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

static const TableAmRoutine hyperstore_methods;
static void convert_to_hyperstore_finish(Oid relid);
static Tuplesortstate *create_tuplesort_for_compress(const HyperstoreInfo *caminfo,
													 const TupleDesc tupdesc);
static List *partially_compressed_relids = NIL; /* Relids that needs to have
												 * updated status set at end of
												 * transcation */

#define COMPRESSION_AM_INFO_SIZE(natts)                                                            \
	(sizeof(HyperstoreInfo) + (sizeof(ColumnCompressionSettings) * (natts)))

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
	Assert(tableam == hyperstore_routine());
	rel->rd_tableam = GetHeapamTableAmRoutine();
	return tableam;
}

static void
create_proxy_vacuum_index(Relation rel, Oid compressed_relid)
{
	Oid compressed_namespaceid = get_rel_namespace(compressed_relid);
	char *compressed_namespace = get_namespace_name(compressed_namespaceid);
	char *compressed_relname = get_rel_name(compressed_relid);
	IndexElem elem = {
		.type = T_IndexElem,
		.name = COMPRESSION_COLUMN_METADATA_COUNT_NAME,
		.indexcolname = NULL,
	};
	IndexStmt stmt = {
		.type = T_IndexStmt,
		.accessMethod = "hsproxy",
		.idxcomment = "Hyperstore vacuum proxy index",
		.idxname = psprintf("%s_ts_hsproxy_idx", compressed_relname),
		.indexParams = list_make1(&elem),
		.relation = makeRangeVar(compressed_namespace, compressed_relname, -1),
	};

	DefineIndexCompat(compressed_relid,
					  &stmt,
					  InvalidOid,
					  InvalidOid,
					  InvalidOid,
					  -1,
					  false,
					  false,
					  false,
					  false,
					  true);
}

static HyperstoreInfo *
lazy_build_hyperstore_info_cache(Relation rel, bool missing_compressed_ok,
								 bool create_chunk_constraints, bool *compressed_relation_created)
{
	Assert(OidIsValid(rel->rd_id) && !ts_is_hypertable(rel->rd_id));

	HyperstoreInfo *caminfo;
	CompressionSettings *settings;
	TupleDesc tupdesc = RelationGetDescr(rel);

	/* Anything put in rel->rd_amcache must be a single memory chunk
	 * palloc'd in CacheMemoryContext since PostgreSQL expects to be able
	 * to free it with a single pfree(). */
	caminfo = MemoryContextAllocZero(CacheMemoryContext, COMPRESSION_AM_INFO_SIZE(tupdesc->natts));
	caminfo->relation_id = get_chunk_id_from_relid(rel->rd_id);
	caminfo->compressed_relid = InvalidOid;
	caminfo->num_columns = tupdesc->natts;
	caminfo->hypertable_id = ts_chunk_get_hypertable_id_by_reloid(rel->rd_id);

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
		Hypertable *ht = ts_hypertable_get_by_id(chunk->fd.hypertable_id);
		Hypertable *ht_compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);

		if (NULL == ht_compressed)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("hypertable \"%s\" is missing compression settings",
							NameStr(ht->fd.table_name)),
					 errhint("Enable compression on the hypertable.")));

		Chunk *c_chunk = create_compress_chunk(ht_compressed, chunk, InvalidOid);

		caminfo->compressed_relation_id = c_chunk->fd.id;
		ts_chunk_set_compressed_chunk(chunk, c_chunk->fd.id);

		if (create_chunk_constraints)
		{
			ts_chunk_constraints_create(ht_compressed, c_chunk);
			ts_trigger_create_all_on_chunk(c_chunk);
			create_proxy_vacuum_index(rel, c_chunk->table_id);
		}
	}

	caminfo->compressed_relid = ts_chunk_get_relid(caminfo->compressed_relation_id, false);
	caminfo->count_cattno =
		get_attnum(caminfo->compressed_relid, COMPRESSION_COLUMN_METADATA_COUNT_NAME);

	Assert(caminfo->compressed_relation_id > 0 && OidIsValid(caminfo->compressed_relid));
	Assert(caminfo->count_cattno != InvalidAttrNumber);
	settings = ts_compression_settings_get(caminfo->compressed_relid);

	Ensure(settings,
		   "no compression settings for relation %s",
		   get_rel_name(RelationGetRelid(rel)));

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

HyperstoreInfo *
RelationGetHyperstoreInfo(Relation rel)
{
	if (NULL == rel->rd_amcache)
		rel->rd_amcache = lazy_build_hyperstore_info_cache(rel, false, true, NULL);

	Assert(rel->rd_amcache && OidIsValid(((HyperstoreInfo *) rel->rd_amcache)->compressed_relid));

	return rel->rd_amcache;
}

static void
build_segment_and_orderby_bms(const HyperstoreInfo *caminfo, Bitmapset **segmentby,
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
hyperstore_slot_callbacks(Relation relation)
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
		const HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(scan->rs_base.rs_rd);

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

#ifdef TS_DEBUG
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
#endif

static TableScanDesc
hyperstore_beginscan(Relation relation, Snapshot snapshot, int nkeys, ScanKey keys,
					 ParallelTableScanDesc parallel_scan, uint32 flags)
{
	CompressionScanDesc scan;
	HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(relation);
	CompressionParallelScanDesc cpscan = (CompressionParallelScanDesc) parallel_scan;

	RelationIncrementReferenceCount(relation);

	TS_DEBUG_LOG("starting %s scan of relation %s parallel_scan=%p",
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
hyperstore_rescan(TableScanDesc sscan, ScanKey key, bool set_params, bool allow_strat,
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
hyperstore_endscan(TableScanDesc sscan)
{
	CompressionScanDesc scan = (CompressionScanDesc) sscan;

	RelationDecrementReferenceCount(sscan->rs_rd);
	table_endscan(scan->cscan_desc);
	table_close(scan->compressed_rel, AccessShareLock);

	Relation relation = sscan->rs_rd;
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam->scan_end(scan->uscan_desc);
	relation->rd_tableam = oldtam;

	TS_DEBUG_LOG("scanned " INT64_FORMAT " tuples (" INT64_FORMAT " compressed, " INT64_FORMAT
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
hyperstore_getnextslot(TableScanDesc sscan, ScanDirection direction, TupleTableSlot *slot)
{
	CompressionScanDesc scan = (CompressionScanDesc) sscan;
	TupleTableSlot *child_slot;

	TS_DEBUG_LOG("relid: %d, relation: %s, reset: %s, compressed_read_done: %s",
				 sscan->rs_rd->rd_id,
				 get_rel_name(sscan->rs_rd->rd_id),
				 yes_no(scan->reset),
				 yes_no(scan->compressed_read_done));

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
			return hyperstore_getnextslot(sscan, direction, slot);
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
hyperstore_parallelscan_estimate(Relation rel)
{
	return sizeof(CompressionParallelScanDescData);
}

/*
 * Initialize ParallelTableScanDesc for a parallel scan of this relation.
 * `pscan` will be sized according to parallelscan_estimate() for the same
 * relation.
 */
static Size
hyperstore_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	CompressionParallelScanDesc cpscan = (CompressionParallelScanDesc) pscan;
	HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(rel);

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
hyperstore_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	CompressionParallelScanDesc cpscan = (CompressionParallelScanDesc) pscan;
	HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(rel);

	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	table_block_parallelscan_reinitialize(rel, (ParallelTableScanDesc) &cpscan->pscandesc);
	rel->rd_tableam = oldtam;

	Relation crel = table_open(caminfo->compressed_relid, AccessShareLock);
	table_block_parallelscan_reinitialize(crel, (ParallelTableScanDesc) &cpscan->cpscandesc);
	table_close(crel, NoLock);
}

static void
hyperstore_get_latest_tid(TableScanDesc sscan, ItemPointer tid)
{
	CompressionScanDesc scan = (CompressionScanDesc) sscan;

	if (is_compressed_tid(tid))
	{
		ItemPointerData decoded_tid;
		uint16 tuple_index = compressed_tid_to_tid(&decoded_tid, tid);
		const Relation rel = scan->cscan_desc->rs_rd;
		rel->rd_tableam->tuple_get_latest_tid(scan->cscan_desc, &decoded_tid);
		tid_to_compressed_tid(tid, &decoded_tid, tuple_index);
	}
	else
	{
		const Relation rel = scan->uscan_desc->rs_rd;
		const TableAmRoutine *oldtam = switch_to_heapam(rel);
		rel->rd_tableam->tuple_get_latest_tid(scan->uscan_desc, tid);
		rel->rd_tableam = oldtam;
	}
}

static void
hyperstore_multi_insert(Relation relation, TupleTableSlot **slots, int ntuples, CommandId cid,
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

enum SegmentbyIndexStatus
{
	SEGMENTBY_INDEX_UNKNOWN = -1,
	SEGMENTBY_INDEX_FALSE = 0,
	SEGMENTBY_INDEX_TRUE = 1,
};

typedef struct IndexFetchComprData
{
	IndexFetchTableData h_base; /* AM independent part of the descriptor */
	IndexFetchTableData *compr_hscan;
	IndexFetchTableData *uncompr_hscan;
	Relation compr_rel;
	ItemPointerData tid;
	int64 num_decompressions;
	uint64 return_count;
	enum SegmentbyIndexStatus segindex;
	bool call_again;		  /* Used to remember the previous value of call_again in
							   * index_fetch_tuple */
	bool internal_call_again; /* Call again passed on to compressed heap */
} IndexFetchComprData;

/* ------------------------------------------------------------------------
 * Index Scan Callbacks for compression AM
 * ------------------------------------------------------------------------
 */
static IndexFetchTableData *
hyperstore_index_fetch_begin(Relation rel)
{
	IndexFetchComprData *cscan = palloc0(sizeof(IndexFetchComprData));
	HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(rel);

	Relation crel = table_open(caminfo->compressed_relid, AccessShareLock);
	cscan->segindex = SEGMENTBY_INDEX_UNKNOWN;
	cscan->return_count = 0;
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
hyperstore_index_fetch_reset(IndexFetchTableData *scan)
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
hyperstore_index_fetch_end(IndexFetchTableData *scan)
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
 * Check if a scan is on a segmentby index.
 *
 * To identify a segmentby index, it is necessary to know the columns
 * (attributes) indexed by the index. Unfortunately, the TAM does not have
 * access to information about the index being scanned, so this information is
 * instead captured at the start of the scan (using the executor start hook)
 * and stored in the ArrowTupleTableSlot.
 *
 * For EXPLAINs (without ANALYZE), the index attributes in the slot might not
 * be set because the index is never really opened. For such a case, when
 * nothing is actually scanned, it is OK to return "false" even though the
 * query is using a segmentby index.
 */
static inline bool
is_segmentby_index_scan(IndexFetchComprData *cscan, TupleTableSlot *slot)
{
	if (cscan->segindex == SEGMENTBY_INDEX_UNKNOWN)
	{
		ArrowTupleTableSlot *aslot = (ArrowTupleTableSlot *) slot;
		const HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(cscan->h_base.rel);
		int16 attno = -1;

		if (bms_is_empty(aslot->index_attrs))
			return false;

		while ((attno = bms_next_member(aslot->index_attrs, attno)) >= 0)
		{
			if (!caminfo->columns[AttrNumberGetAttrOffset(attno)].is_segmentby)
				return false;
		}

		return true;
	}

	return false;
}

/*
 * Return tuple for given TID via index scan.
 *
 * An index scan calls this function to fetch the "heap" tuple with the given
 * TID from the index.
 *
 * The TID points to a tuple either in the regular (non-compressed) or the
 * compressed relation. The data is fetched from the identified relation.
 *
 * If the index only indexes segmentby column(s), the index is itself
 * "compressed" and there is only one TID per compressed segment/tuple. In
 * that case, the "call_again" parameter is used to make sure the index scan
 * calls this function until all the rows in a compressed tuple is
 * returned. This "unwrapping" only happens in the case of segmentby indexes.
 */
static bool
hyperstore_index_fetch_tuple(struct IndexFetchTableData *scan, ItemPointer tid, Snapshot snapshot,
							 TupleTableSlot *slot, bool *call_again, bool *all_dead)
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

		cscan->return_count++;
		return result;
	}

	/* Compressed tuples not visible through this TAM when scanned by
	 * transparent decompression enabled since DecompressChunk already scanned
	 * that data. */
	if (ts_guc_enable_transparent_decompression == 2)
		return false;

	bool is_segmentby_index = is_segmentby_index_scan(cscan, slot);

	/* Fast path for segmentby index scans. If the compressed tuple is still
	 * being consumed, just increment the tuple index and return. */
	if (is_segmentby_index && cscan->call_again)
	{
		ExecStoreNextArrowTuple(slot);
		cscan->call_again = !arrow_slot_is_last(slot);
		*call_again = cscan->call_again || cscan->internal_call_again;
		cscan->return_count++;
		return true;
	}

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
		cscan->return_count++;
		return true;
	}

	bool result = crel->rd_tableam->index_fetch_tuple(cscan->compr_hscan,
													  &decoded_tid,
													  snapshot,
													  child_slot,
													  &cscan->internal_call_again,
													  all_dead);

	if (result)
	{
		slot->tts_tableOid = RelationGetRelid(scan->rel);
		ExecStoreArrowTuple(slot, tuple_index);
		/* Save the current compressed TID */
		ItemPointerCopy(&decoded_tid, &cscan->tid);
		cscan->num_decompressions++;

		if (is_segmentby_index)
		{
			Assert(tuple_index == 1);
			cscan->call_again = !arrow_slot_is_last(slot);
			*call_again = cscan->call_again || cscan->internal_call_again;
		}

		cscan->return_count++;
	}

	return result;
}

/* ------------------------------------------------------------------------
 * Callbacks for non-modifying operations on individual tuples for compression AM
 * ------------------------------------------------------------------------
 */

static bool
hyperstore_fetch_row_version(Relation relation, ItemPointer tid, Snapshot snapshot,
							 TupleTableSlot *slot)
{
	bool result;
	uint16 tuple_index = InvalidTupleIndex;

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
		ItemPointerData decoded_tid;
		HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(relation);
		Relation child_rel = table_open(caminfo->compressed_relid, AccessShareLock);
		TupleTableSlot *child_slot =
			arrow_slot_get_compressed_slot(slot, RelationGetDescr(child_rel));

		tuple_index = compressed_tid_to_tid(&decoded_tid, tid);
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
hyperstore_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
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
hyperstore_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot, Snapshot snapshot)
{
	HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(rel);
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
hyperstore_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	TM_IndexDeleteOp noncompr_delstate = *delstate;
	TM_IndexDeleteOp compr_delstate = *delstate;
	HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(rel);
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
				TM_IndexDelete *deltid_compr = &compr_delstate.deltids[compr_delstate.ndeltids];
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
hyperstore_tuple_insert(Relation relation, TupleTableSlot *slot, CommandId cid, int options,
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
hyperstore_tuple_insert_speculative(Relation relation, TupleTableSlot *slot, CommandId cid,
									int options, BulkInsertStateData *bistate, uint32 specToken)
{
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam
		->tuple_insert_speculative(relation, slot, cid, options, bistate, specToken);
	relation->rd_tableam = oldtam;
}

static void
hyperstore_tuple_complete_speculative(Relation relation, TupleTableSlot *slot, uint32 specToken,
									  bool succeeded)
{
	const TableAmRoutine *oldtam = switch_to_heapam(relation);
	relation->rd_tableam->tuple_complete_speculative(relation, slot, specToken, succeeded);
	relation->rd_tableam = oldtam;
}

static TM_Result
hyperstore_tuple_delete(Relation relation, ItemPointer tid, CommandId cid, Snapshot snapshot,
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
hyperstore_tuple_update(Relation relation, ItemPointer otid, TupleTableSlot *slot, CommandId cid,
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
hyperstore_tuple_lock(Relation relation, ItemPointer tid, Snapshot snapshot, TupleTableSlot *slot,
					  CommandId cid, LockTupleMode mode, LockWaitPolicy wait_policy, uint8 flags,
					  TM_FailureData *tmfd)
{
	TM_Result result;

	if (is_compressed_tid(tid))
	{
		HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(relation);
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
hyperstore_finish_bulk_insert(Relation rel, int options)
{
	if (conversionstate)
		convert_to_hyperstore_finish(RelationGetRelid(rel));
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
hyperstore_relation_set_new_filelocator(Relation rel, const RelFileLocator *newrlocator,
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
hyperstore_relation_nontransactional_truncate(Relation rel)
{
	RelationTruncate(rel, 0);
}

static void
hyperstore_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	FEATURE_NOT_SUPPORTED;
}

static void
on_compression_progress(RowCompressor *rowcompress, uint64 ntuples)
{
	pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_TUPLES_WRITTEN, ntuples);
}

/*
 * Rewrite a relation and compress at the same time.
 *
 * Note that all tuples are frozen when compressed to make sure they are
 * visible to concurrent transactions after the rewrite. This isn't MVCC
 * compliant and does not work for isolation levels of repeatable read or
 * higher. Ideally, we should check visibility of each original tuple that we
 * roll up into a compressed tuple and transfer visibility information (XID)
 * based on that, just like done in heap when it is using a rewrite state.
 */
static Oid
compress_and_swap_heap(Relation rel, Tuplesortstate *tuplesort, TransactionId *xid_cutoff,
					   MultiXactId *multi_cutoff)
{
	const HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(rel);
	TupleDesc tupdesc = RelationGetDescr(rel);
	Oid old_compressed_relid = caminfo->compressed_relid;
	CompressionSettings *settings = ts_compression_settings_get(old_compressed_relid);
	Relation old_compressed_rel = table_open(old_compressed_relid, AccessExclusiveLock);
#if PG15_GE
	Oid accessMethod = old_compressed_rel->rd_rel->relam;
#endif
	Oid tableSpace = old_compressed_rel->rd_rel->reltablespace;
	char relpersistence = old_compressed_rel->rd_rel->relpersistence;
	Oid new_compressed_relid = make_new_heap(old_compressed_relid,
											 tableSpace,
#if PG15_GE
											 accessMethod,
#endif
											 relpersistence,
											 AccessExclusiveLock);
	Relation new_compressed_rel = table_open(new_compressed_relid, AccessExclusiveLock);
	RowCompressor row_compressor;
	double reltuples;
	int32 relpages;

	/* Initialize the compressor. */
	row_compressor_init(settings,
						&row_compressor,
						rel,
						new_compressed_rel,
						RelationGetDescr(old_compressed_rel)->natts,
						true /*need_bistate*/,
						HEAP_INSERT_FROZEN);

	row_compressor.on_flush = on_compression_progress;
	row_compressor_append_sorted_rows(&row_compressor, tuplesort, tupdesc, old_compressed_rel);
	reltuples = row_compressor.num_compressed_rows;
	relpages = RelationGetNumberOfBlocks(new_compressed_rel);
	row_compressor_close(&row_compressor);

	table_close(new_compressed_rel, NoLock);
	table_close(old_compressed_rel, NoLock);

	/* Update stats for the compressed relation */
	Relation relRelation = table_open(RelationRelationId, RowExclusiveLock);
	HeapTuple reltup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(new_compressed_relid));
	if (!HeapTupleIsValid(reltup))
		elog(ERROR, "cache lookup failed for relation %u", new_compressed_relid);
	Form_pg_class relform = (Form_pg_class) GETSTRUCT(reltup);

	relform->relpages = relpages;
	relform->reltuples = reltuples;

	CatalogTupleUpdate(relRelation, &reltup->t_self, reltup);

	/* Clean up. */
	heap_freetuple(reltup);
	table_close(relRelation, RowExclusiveLock);

	/* Make the update visible */
	CommandCounterIncrement();

	/* Finish the heap swap for the compressed relation. Note that it is not
	 * possible to swap toast content since new tuples were generated via
	 * compression. */
	finish_heap_swap(old_compressed_relid,
					 new_compressed_relid,
					 false /* is_system_catalog */,
					 false /* swap_toast_by_content */,
					 false,
					 true,
					 *xid_cutoff,
					 *multi_cutoff,
					 relpersistence);

	return new_compressed_relid;
}

/*
 * Rewrite/compress the relation for CLUSTER or VACUUM FULL.
 *
 * The copy_for_cluster() callback is called during a CLUSTER or VACUUM FULL,
 * and performs a heap swap/rewrite. The code is based on the heap's
 * copy_for_cluster(), with changes to handle two heaps and compressed tuples.
 *
 * For Hyperstore, two heap swaps are performed: one on the non-compressed
 * (user-visible) relation, which is managed by PostgreSQL and passed on to
 * this callback, and one on the compressed relation that is implemented
 * within the callback.
 *
 * The Hyperstore implementation of copy_for_cluster() is similar to the one
 * for Heap. However, instead of "rewriting" tuples into the new heap (while
 * at the same time handling freezing and visibility), Hyperstore will
 * compress all the data and write it to a new compressed relation. Since the
 * compression is based on the legacy compression implementation, visibility
 * of recently deleted tuples and freezing of tuples is not correctly handled,
 * at least not for higher isolation levels than read committed. Changes to
 * handle higher isolation levels should be considered in a future update of
 * this code.
 *
 * Some things missing includes the handling of recently dead tuples that need
 * to be transferred to the new heap since they might still be visible to some
 * ongoing transactions. PostgreSQL's heap implementation handles this via the
 * heap rewrite module. It should also be possible to write frozen compressed
 * tuples if all rows it compresses are also frozen.
 */
static void
hyperstore_relation_copy_for_cluster(Relation OldHyperstore, Relation NewCompression,
									 Relation OldIndex, bool use_sort, TransactionId OldestXmin,
									 TransactionId *xid_cutoff, MultiXactId *multi_cutoff,
									 double *num_tuples, double *tups_vacuumed,
									 double *tups_recently_dead)
{
	const HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(OldHyperstore);
	TupleDesc tupdesc = RelationGetDescr(OldHyperstore);
	CompressionScanDesc cscan;
	HeapScanDesc chscan;
	HeapScanDesc uhscan;
	Tuplesortstate *tuplesort;
	TableScanDesc tscan;
	TupleTableSlot *slot;
	ArrowTupleTableSlot *aslot;
	BufferHeapTupleTableSlot *hslot;
	BlockNumber prev_cblock = InvalidBlockNumber;
	BlockNumber startblock;
	BlockNumber nblocks;

	if (ts_is_hypertable(RelationGetRelid(OldHyperstore)))
		return;

	/* Error out if this is a CLUSTER. It would be possible to CLUSTER only
	 * the non-compressed relation, but utility of this is questionable as
	 * most of the data should be compressed (and ordered) anyway. */
	if (OldIndex != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot cluster a hyperstore table"),
				 errdetail("A hyperstore table is already ordered by compression.")));

	tuplesort = create_tuplesort_for_compress(caminfo, tupdesc);

	/* In scan-and-sort mode and also VACUUM FULL, set phase */
	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_SEQ_SCAN_HEAP);

	/* This will scan via the Hyperstore callbacks, getting tuples from both
	 * compressed and non-compressed relations */
	tscan = table_beginscan(OldHyperstore, SnapshotAny, 0, (ScanKey) NULL);
	cscan = (CompressionScanDesc) tscan;
	chscan = (HeapScanDesc) cscan->cscan_desc;
	uhscan = (HeapScanDesc) cscan->uscan_desc;
	slot = table_slot_create(OldHyperstore, NULL);
	startblock = chscan->rs_startblock + uhscan->rs_startblock;
	nblocks = chscan->rs_nblocks + uhscan->rs_nblocks;

	/* Set total heap blocks */
	pgstat_progress_update_param(PROGRESS_CLUSTER_TOTAL_HEAP_BLKS, nblocks);

	aslot = (ArrowTupleTableSlot *) slot;

	for (;;)
	{
		HeapTuple tuple;
		Buffer buf;
		bool isdead;
		BlockNumber cblock;

		CHECK_FOR_INTERRUPTS();

		if (!table_scan_getnextslot(tscan, ForwardScanDirection, slot))
		{
			/*
			 * If the last pages of the scan were empty, we would go to
			 * the next phase while heap_blks_scanned != heap_blks_total.
			 * Instead, to ensure that heap_blks_scanned is equivalent to
			 * total_heap_blks after the table scan phase, this parameter
			 * is manually updated to the correct value when the table
			 * scan finishes.
			 */
			pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_BLKS_SCANNED, nblocks);
			break;
		}
		/*
		 * In scan-and-sort mode and also VACUUM FULL, set heap blocks
		 * scanned
		 *
		 * Note that heapScan may start at an offset and wrap around, i.e.
		 * rs_startblock may be >0, and rs_cblock may end with a number
		 * below rs_startblock. To prevent showing this wraparound to the
		 * user, we offset rs_cblock by rs_startblock (modulo rs_nblocks).
		 */
		cblock = chscan->rs_cblock + uhscan->rs_cblock;

		if (prev_cblock != cblock)
		{
			pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_BLKS_SCANNED,
										 (cblock + nblocks - startblock) % nblocks + 1);
			prev_cblock = cblock;
		}
		/* Get the actual tuple from the child slot (either compressed or
		 * non-compressed). The tuple has all the visibility information. */
		tuple = ExecFetchSlotHeapTuple(aslot->child_slot, false, NULL);
		hslot = (BufferHeapTupleTableSlot *) aslot->child_slot;

		buf = hslot->buffer;

		LockBuffer(buf, BUFFER_LOCK_SHARE);

		switch (HeapTupleSatisfiesVacuum(tuple, OldestXmin, buf))
		{
			case HEAPTUPLE_DEAD:
				/* Definitely dead */
				isdead = true;
				break;
			case HEAPTUPLE_RECENTLY_DEAD:
				/* Note: This case is treated as "dead" in Hyperstore,
				 * although some of these tuples might still be visible to
				 * some transactions. For strict correctness, recently dead
				 * tuples should be transferred to the new heap if they are
				 * still visible to some transactions (e.g. under repeatable
				 * read). However, this is tricky since multiple rows with
				 * potentially different visibility is rolled up into one
				 * compressed row with singular visibility. */
				isdead = true;
				break;
			case HEAPTUPLE_LIVE:
				/* Live or recently dead, must copy it */
				isdead = false;
				break;
			case HEAPTUPLE_INSERT_IN_PROGRESS:

				/*
				 * Since we hold exclusive lock on the relation, normally the
				 * only way to see this is if it was inserted earlier in our
				 * own transaction.  However, it can happen in system
				 * catalogs, since we tend to release write lock before commit
				 * there. Still, system catalogs don't use Hyperstore.
				 */
				if (!TransactionIdIsCurrentTransactionId(HeapTupleHeaderGetXmin(tuple->t_data)))
					elog(WARNING,
						 "concurrent insert in progress within table \"%s\"",
						 RelationGetRelationName(OldHyperstore));
				/* treat as live */
				isdead = false;
				break;
			case HEAPTUPLE_DELETE_IN_PROGRESS:

				/*
				 * Similar situation to INSERT_IN_PROGRESS case.
				 */
				if (!TransactionIdIsCurrentTransactionId(
						HeapTupleHeaderGetUpdateXid(tuple->t_data)))
					elog(WARNING,
						 "concurrent delete in progress within table \"%s\"",
						 RelationGetRelationName(OldHyperstore));
				/* Note: This case is treated as "dead" in Hyperstore,
				 * although this is "recently dead" in heap */
				isdead = true;
				break;
			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				isdead = false; /* keep compiler quiet */
				break;
		}

		LockBuffer(buf, BUFFER_LOCK_UNLOCK);

		if (isdead)
		{
			*tups_vacuumed += 1;

			/* Skip whole segment if a dead compressed tuple */
			if (arrow_slot_is_compressed(slot))
				arrow_slot_mark_consumed(slot);
			continue;
		}

		while (!arrow_slot_is_last(slot))
		{
			*num_tuples += 1;
			tuplesort_puttupleslot(tuplesort, slot);
			ExecStoreNextArrowTuple(slot);
		}

		*num_tuples += 1;
		tuplesort_puttupleslot(tuplesort, slot);

		/* Report increase in number of tuples scanned */
		pgstat_progress_update_param(PROGRESS_CLUSTER_HEAP_TUPLES_SCANNED, *num_tuples);
	}

	table_endscan(tscan);
	ExecDropSingleTupleTableSlot(slot);

	/* Report that we are now sorting tuples */
	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_SORT_TUPLES);

	/* Sort and recreate compressed relation */
	tuplesort_performsort(tuplesort);

	/* Report that we are now writing new heap */
	pgstat_progress_update_param(PROGRESS_CLUSTER_PHASE, PROGRESS_CLUSTER_PHASE_WRITE_NEW_HEAP);

	compress_and_swap_heap(OldHyperstore, tuplesort, xid_cutoff, multi_cutoff);
	tuplesort_end(tuplesort);
}

/*
 * VACUUM (not VACUUM FULL).
 *
 * Vacuum the hyperstore by calling vacuum on both the non-compressed and
 * compressed relations.
 *
 * Indexes on a heap are normally vacuumed as part of vacuuming the
 * heap. However, a hyperstore index is defined on the non-compressed relation
 * and contains tuples from both the non-compressed and compressed relations
 * and therefore dead tuples vacuumed on the compressed relation won't be
 * removed from a hyperstore index by default. The vacuuming of dead
 * compressed tuples from the hyperstore index therefore requires special
 * handling, which is triggered via a proxy index (hsproxy) that relays the
 * clean up to the "correct" hyperstore indexes. (See hsproxy.c)
 *
 * For future: It would make sense to (re-)compress all non-compressed data as
 * part of vacuum since (re-)compression is a kind of cleanup but also leaves
 * a lot of garbage.
 */
static void
hyperstore_vacuum_rel(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy)
{
	HyperstoreInfo *caminfo;

	if (ts_is_hypertable(RelationGetRelid(rel)))
		return;

	caminfo = RelationGetHyperstoreInfo(rel);

	LOCKMODE lmode =
		(params->options & VACOPT_FULL) ? AccessExclusiveLock : ShareUpdateExclusiveLock;

	/* Vacuum the compressed relation */
	Relation crel = vacuum_open_relation(caminfo->compressed_relid,
										 NULL,
										 params->options,
										 params->log_min_duration >= 0,
										 lmode);

	if (crel)
	{
		crel->rd_tableam->relation_vacuum(crel, params, bstrategy);
		table_close(crel, NoLock);
	}

	/* Vacuum the non-compressed relation */
	const TableAmRoutine *oldtam = switch_to_heapam(rel);
	rel->rd_tableam->relation_vacuum(rel, params, bstrategy);
	rel->rd_tableam = oldtam;
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
hyperstore_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
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
hyperstore_scan_analyze_next_tuple(TableScanDesc scan, TransactionId OldestXmin, double *liverows,
								   double *deadrows, TupleTableSlot *slot)
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
	Bitmapset *segmentby_cols;
	Bitmapset *orderby_cols;
	bool is_segmentby_index;
	MemoryContext decompression_mcxt;
	ArrowArray **arrow_columns;
} IndexCallbackState;

/*
 * Callback for index builds on compressed relation.
 *
 * When building an index, this function is called once for every compressed
 * tuple. But to build an index over the original (non-compressed) values, it
 * is necessary to "unwrap" the compressed data. Therefore, the function calls
 * the original index build callback once for every value in the compressed
 * tuple.
 *
 * However, when the index covers only segmentby columns, and the value is the
 * same for all original rows in the segment, the index storage can be
 * optimized by only indexing the compressed row and then unwrapping it during
 * scanning instead.
 *
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
			/*
			 * For a segmentby column, there is nothing to decompress, so just
			 * return the non-compressed value.
			 *
			 * There's an opportunity to optimize index storage size for
			 * "segmentby indexes", i.e., those indexes that cover all, or a
			 * subset, of segmentby columns.
			 *
			 * In case of a segmentby index, index only one value (essentially
			 * the compressed tuple), otherwise, index the non-compressed
			 * values (the number of such values is indicated by "count"
			 * metadata column).
			 */
			if (icstate->is_segmentby_index)
			{
				num_rows = 1;
			}
			else
			{
				int countoff = icstate->index_info->ii_NumIndexAttrs;
				Assert(num_rows == -1 || num_rows == DatumGetInt32(values[countoff]));
				num_rows = DatumGetInt32(values[countoff]);
			}
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

				/* If "num_rows" was set previously by another column, the
				 * value should be the same for this column */
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
		Assert(!icstate->is_segmentby_index || rownum == 0);

		icstate->callback(index, &index_tid, values, isnull, tupleIsAlive, icstate->orig_state);
		icstate->ntuples++;
	}
}

/*
 * Build index.
 */
static double
hyperstore_index_build_range_scan(Relation relation, Relation indexRelation, IndexInfo *indexInfo,
								  bool allow_sync, bool anyvisible, bool progress,
								  BlockNumber start_blockno, BlockNumber numblocks,
								  IndexBuildCallback callback, void *callback_state,
								  TableScanDesc scan)
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

	HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(relation);

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
		.is_segmentby_index = true,
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

		/* If the indexed column is not a segmentby column, then this is not a
		 * segmentby index */
		if (!bms_is_member(attno, icstate.segmentby_cols))
			icstate.is_segmentby_index = false;
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
hyperstore_index_validate_scan(Relation compressionRelation, Relation indexRelation,
							   IndexInfo *indexInfo, Snapshot snapshot, ValidateIndexState *state)
{
	FEATURE_NOT_SUPPORTED;
}

/* ------------------------------------------------------------------------
 * Miscellaneous callbacks for the compression AM
 * ------------------------------------------------------------------------
 */
static bool
hyperstore_relation_needs_toast_table(Relation rel)
{
	return false;
}

static Oid
hyperstore_relation_toast_am(Relation rel)
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
hyperstore_relation_size(Relation rel, ForkNumber forkNumber)
{
	uint64 ubytes = table_block_relation_size(rel, forkNumber);
	int32 hyper_id = ts_chunk_get_hypertable_id_by_reloid(rel->rd_id);

	if (hyper_id == 0)
		return ubytes;

	HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(rel);

	/* For ANALYZE, need to return sum for both relations. */
	Relation crel = try_relation_open(caminfo->compressed_relid, AccessShareLock);

	if (crel == NULL)
		return ubytes;

	uint64 cbytes = table_block_relation_size(crel, forkNumber);
	relation_close(crel, NoLock);

	return ubytes + cbytes;
}

static void
hyperstore_relation_estimate_size(Relation rel, int32 *attr_widths, BlockNumber *pages,
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

	const HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(rel);

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
hyperstore_fetch_toast_slice(Relation toastrel, Oid valueid, int32 attrsize, int32 sliceoffset,
							 int32 slicelength, struct varlena *result)
{
	FEATURE_NOT_SUPPORTED;
}

/* ------------------------------------------------------------------------
 * Executor related callbacks for the compression AM
 * ------------------------------------------------------------------------
 */

static bool
hyperstore_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

static bool
hyperstore_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
								  TupleTableSlot *slot)
{
	FEATURE_NOT_SUPPORTED;
	return false;
}

static Tuplesortstate *
create_tuplesort_for_compress(const HyperstoreInfo *caminfo, const TupleDesc tupdesc)
{
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

	return tuplesort_begin_heap(tupdesc,
								caminfo->num_keys,
								sort_keys,
								sort_operators,
								sort_collations,
								nulls_first,
								maintenance_work_mem,
								NULL,
								false /*=randomAccess*/);
}

/*
 * Convert a table to compression AM.
 *
 * Need to setup the conversion state used to compress the data.
 */
static void
convert_to_hyperstore(Oid relid)
{
	Relation relation = table_open(relid, AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(relation);
	bool compress_chunk_created;
	HyperstoreInfo *caminfo =
		lazy_build_hyperstore_info_cache(relation, true, false, &compress_chunk_created);

	if (!compress_chunk_created)
	{
		/* A compressed relation already exists, so converting from legacy
		 * compression. It is only necessary to create the proxy vacuum
		 * index. */
		create_proxy_vacuum_index(relation, caminfo->compressed_relid);
		table_close(relation, AccessShareLock);
		return;
	}

	MemoryContext oldcxt = MemoryContextSwitchTo(CacheMemoryContext);
	ConversionState *state = palloc0(sizeof(ConversionState));
	state->tuplesortstate = create_tuplesort_for_compress(caminfo, tupdesc);
	Assert(state->tuplesortstate);
	state->relid = relid;
	conversionstate = state;
	MemoryContextSwitchTo(oldcxt);
	table_close(relation, AccessShareLock);
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
hyperstore_xact_event(XactEvent event, void *arg)
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
				Relation rel = table_open(relid, AccessShareLock);
				/* Calling RelationGetHyperstoreInfo() here will create the
				 * compressed relation if not already created. */
				HyperstoreInfo *hsinfo = RelationGetHyperstoreInfo(rel);
				Ensure(OidIsValid(hsinfo->compressed_relid),
					   "hyperstore \"%s\" has no compressed data relation",
					   get_rel_name(relid));
				Chunk *chunk = ts_chunk_get_by_relid(relid, true);
				ts_chunk_set_partial(chunk);
				table_close(rel, NoLock);
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
convert_to_hyperstore_finish(Oid relid)
{
	if (!conversionstate)
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

	/*
	 * The compressed chunk should have been created in
	 * convert_to_hyperstore_start() if it didn't already exist.
	 */
	Chunk *c_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);
	Relation compressed_rel = table_open(c_chunk->table_id, RowExclusiveLock);
	CompressionSettings *settings = ts_compression_settings_get(RelationGetRelid(compressed_rel));
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
	conversionstate->tuplesortstate = NULL;

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
	create_proxy_vacuum_index(relation, RelationGetRelid(compressed_rel));

	table_close(relation, NoLock);
	table_close(compressed_rel, NoLock);
	conversionstate = NULL;
}

/*
 * Convert the chunk away from compression AM to another table access method.
 * When this happens it is necessary to cleanup metadata.
 */
static void
convert_from_hyperstore(Oid relid)
{
	int32 chunk_id = get_chunk_id_from_relid(relid);
	ts_compression_chunk_size_delete(chunk_id);

	/* Need to truncate the compressed relation after converting from compression TAM */
	TS_WITH_MEMORY_CONTEXT(CurTransactionContext,
						   { cleanup_relids = lappend_oid(cleanup_relids, relid); });
}

void
hyperstore_alter_access_method_begin(Oid relid, bool to_other_am)
{
	if (to_other_am)
		convert_from_hyperstore(relid);
	else
		convert_to_hyperstore(relid);
}

/*
 * Called at the end of converting a chunk to a table access method.
 */
void
hyperstore_alter_access_method_finish(Oid relid, bool to_other_am)
{
	if (to_other_am)
		cleanup_compression_relations();

	/* Finishing the conversion to compression TAM is handled in
	 * the finish_bulk_insert callback */
}

/*
 * Convert any index-only scans on segmentby indexes to regular index scans
 * since index-only scans are not supported on segmentby indexes.
 *
 * Indexes on segmentby columns are optimized to store only one index
 * reference per segment instead of one per value in each segment. This relies
 * on "unwrapping" the segment during scanning. However, with an
 * IndexOnlyScan, the Hyperstore TAM's index_fetch_tuple() is not be called to
 * fetch the heap tuple (since the scan returns directly from the index), and
 * there is no opportunity to unwrap the tuple. Therefore, turn IndexOnlyScans
 * into regular IndexScans on segmentby indexes.
 */
static void
convert_index_only_scans(const HyperstoreInfo *caminfo, List *pathlist)
{
	ListCell *lc;

	foreach (lc, pathlist)
	{
		Path *path = lfirst(lc);
		bool is_segmentby_index = true;

		if (path->pathtype == T_IndexOnlyScan)
		{
			IndexPath *ipath = (IndexPath *) path;
			Relation irel = relation_open(ipath->indexinfo->indexoid, AccessShareLock);
			const int2vector *indkeys = &irel->rd_index->indkey;

			for (int i = 0; i < indkeys->dim1; i++)
			{
				const AttrNumber attno = indkeys->values[i];

				if (!caminfo->columns[AttrNumberGetAttrOffset(attno)].is_segmentby)
				{
					is_segmentby_index = false;
					break;
				}
			}

			/* Convert this IndexOnlyScan to a regular IndexScan since
			 * segmentby indexes do not support IndexOnlyScans */
			if (is_segmentby_index)
				path->pathtype = T_IndexScan;

			relation_close(irel, AccessShareLock);
		}
	}
}

void
hyperstore_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Hypertable *ht)
{
	const RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);
	Relation relation = table_open(rte->relid, AccessShareLock);
	const HyperstoreInfo *caminfo = RelationGetHyperstoreInfo(relation);
	convert_index_only_scans(caminfo, rel->pathlist);
	convert_index_only_scans(caminfo, rel->partial_pathlist);
	table_close(relation, AccessShareLock);
}

/* ------------------------------------------------------------------------
 * Definition of the compression table access method.
 * ------------------------------------------------------------------------
 */

static const TableAmRoutine hyperstore_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = hyperstore_slot_callbacks,

	.scan_begin = hyperstore_beginscan,
	.scan_end = hyperstore_endscan,
	.scan_rescan = hyperstore_rescan,
	.scan_getnextslot = hyperstore_getnextslot,
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
	.parallelscan_estimate = hyperstore_parallelscan_estimate,
	.parallelscan_initialize = hyperstore_parallelscan_initialize,
	.parallelscan_reinitialize = hyperstore_parallelscan_reinitialize,

	/* ------------------------------------------------------------------------
	 * Index Scan Callbacks
	 * ------------------------------------------------------------------------
	 */
	.index_fetch_begin = hyperstore_index_fetch_begin,
	.index_fetch_reset = hyperstore_index_fetch_reset,
	.index_fetch_end = hyperstore_index_fetch_end,
	.index_fetch_tuple = hyperstore_index_fetch_tuple,

	/* ------------------------------------------------------------------------
	 * Manipulations of physical tuples.
	 * ------------------------------------------------------------------------
	 */
	.tuple_insert = hyperstore_tuple_insert,
	.tuple_insert_speculative = hyperstore_tuple_insert_speculative,
	.tuple_complete_speculative = hyperstore_tuple_complete_speculative,
	.multi_insert = hyperstore_multi_insert,
	.tuple_delete = hyperstore_tuple_delete,
	.tuple_update = hyperstore_tuple_update,
	.tuple_lock = hyperstore_tuple_lock,

	.finish_bulk_insert = hyperstore_finish_bulk_insert,

	/* ------------------------------------------------------------------------
	 * Callbacks for non-modifying operations on individual tuples
	 * ------------------------------------------------------------------------
	 */
	.tuple_fetch_row_version = hyperstore_fetch_row_version,

	.tuple_get_latest_tid = hyperstore_get_latest_tid,
	.tuple_tid_valid = hyperstore_tuple_tid_valid,
	.tuple_satisfies_snapshot = hyperstore_tuple_satisfies_snapshot,
#if PG14_GE
	.index_delete_tuples = hyperstore_index_delete_tuples,
#endif

/* ------------------------------------------------------------------------
 * DDL related functionality.
 * ------------------------------------------------------------------------
 */
#if PG16_GE
	.relation_set_new_filelocator = hyperstore_relation_set_new_filelocator,
#else
	.relation_set_new_filenode = hyperstore_relation_set_new_filelocator,
#endif
	.relation_nontransactional_truncate = hyperstore_relation_nontransactional_truncate,
	.relation_copy_data = hyperstore_relation_copy_data,
	.relation_copy_for_cluster = hyperstore_relation_copy_for_cluster,
	.relation_vacuum = hyperstore_vacuum_rel,
	.scan_analyze_next_block = hyperstore_scan_analyze_next_block,
	.scan_analyze_next_tuple = hyperstore_scan_analyze_next_tuple,
	.index_build_range_scan = hyperstore_index_build_range_scan,
	.index_validate_scan = hyperstore_index_validate_scan,

	/* ------------------------------------------------------------------------
	 * Miscellaneous functions.
	 * ------------------------------------------------------------------------
	 */
	.relation_size = hyperstore_relation_size,
	.relation_needs_toast_table = hyperstore_relation_needs_toast_table,
	.relation_toast_am = hyperstore_relation_toast_am,
	.relation_fetch_toast_slice = hyperstore_fetch_toast_slice,

	/* ------------------------------------------------------------------------
	 * Planner related functions.
	 * ------------------------------------------------------------------------
	 */
	.relation_estimate_size = hyperstore_relation_estimate_size,

	/* ------------------------------------------------------------------------
	 * Executor related functions.
	 * ------------------------------------------------------------------------
	 */

	/* We do not support bitmap heap scan at this point. */
	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,

	.scan_sample_next_block = hyperstore_scan_sample_next_block,
	.scan_sample_next_tuple = hyperstore_scan_sample_next_tuple,
};

const TableAmRoutine *
hyperstore_routine(void)
{
	return &hyperstore_methods;
}

Datum
hyperstore_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&hyperstore_methods);
}
