/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/amapi.h>
#include <access/genam.h>
#include <access/generic_xlog.h>
#include <access/multixact.h>
#include <access/relation.h>
#include <access/reloptions.h>
#include <catalog/pg_class.h>
#include <commands/vacuum.h>
#include <math.h>
#include <nodes/makefuncs.h>
#include <postgres_ext.h>
#include <storage/buf.h>
#include <storage/bufmgr.h>
#include <storage/itemptr.h>
#include <storage/lockdefs.h>
#include <utils/regproc.h>

#include <compat/compat.h>
#include "hyperstore/arrow_tts.h"
#include "hyperstore/hsproxy.h"
#include <chunk.h>

/**
 * Hyperstore proxy index AM (hsproxy).
 *
 * The hsproxy index AM doesn't provide any indexing functionality itself. It
 * is only used to "proxy" vacuum calls between a hyperstore's internal
 * compressed relation (holding compressed data) and the indexes defined on
 * the user-visible hyperstore relation (holding non-compressed data).
 *
 * A hyperstore consists of two relations internally: the user-visible
 * "hyperstore" relation and the internal compressed relation and indexes on a
 * hyperstore encompass data from both these relations. This creates a
 * complication when vacuuming a relation because only he indexes defined on
 * the relation are vacuumed. Therefore, a vacuum on a hyperstore's
 * non-compressed relation will vacuum pointers to non-compressed tuples from
 * the indexes, but not pointers to compressed tuples. A vacuum on the
 * compressed relation, on the other hand, will not vacuum anything from the
 * hyperstore indexes because they are defined on the non-compressed relation
 * and only indexes defined directly on the internal compressed relation will
 * be vacuumed.
 *
 * The hsproxy index fixes this issue by relaying vacuum (bulkdelete calls)
 * from the compressed relation to all indexes defined on the non-compressed
 * relation. There needs to be only one hsproxy index defined on a compressed
 * relation to vacuum all indexes.
 *
 * The hsproxy index needs to be defined on at least one column on the
 * compressed relation (it does not really matter which one). By default it
 * uses the "count" column of the compressed relation and when a set of
 * compressed tuples are vacuumed, its bulkdelete callback is called with
 * those tuples. The callback relays that call to the hyperstore indexes and
 * also decodes TIDs from the indexes to match the TIDs in the compressed
 * relation.
 */

/*
 * Given the internal compressed relid, lookup the corresponding hyperstore
 * relid.
 *
 * Currently, this relies on information in the "chunk" metadata
 * table. Ideally, the lookup should not have any dependencies on chunks and,
 * instead, the hyperstore mappings should be self-contained in compression
 * settings or a dedicated hyperstore settings table. Another idea is to keep
 * the mappings in index reloptions, but this does not handle relation name
 * changes well.
 */
static Oid
get_hyperstore_relid(Oid compress_relid)
{
	Datum datid = DirectFunctionCall1(ts_chunk_id_from_relid, ObjectIdGetDatum(compress_relid));
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, AccessShareLock, CurrentMemoryContext);
	Oid hyperstore_relid = InvalidOid;

	iterator.ctx.index =
		catalog_get_index(ts_catalog_get(), CHUNK, CHUNK_COMPRESSED_CHUNK_ID_INDEX);
	ts_scan_iterator_scan_key_init(&iterator,
								   Anum_chunk_compressed_chunk_id_idx_compressed_chunk_id,
								   BTEqualStrategyNumber,
								   F_INT4EQ,
								   datid);

	ts_scanner_foreach(&iterator)
	{
		TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
		Datum datum;
		bool isnull;

		datum = slot_getattr(ti->slot, Anum_chunk_id, &isnull);

		if (!isnull)
		{
			hyperstore_relid = ts_chunk_get_relid(DatumGetInt32(datum), true);
			break;
		}
	}

	ts_scan_iterator_close(&iterator);

	return hyperstore_relid;
}

static IndexBuildResult *
hsproxy_build(Relation rel, Relation index, struct IndexInfo *indexInfo)
{
	IndexBuildResult *result = palloc0(sizeof(IndexBuildResult));
	result->heap_tuples = 0;
	result->index_tuples = 0;
	return result;
}

/*
 * HSProxy doesn't store any data, so buildempty() is a dummy.
 */
static void
hsproxy_buildempty(Relation index)
{
}

typedef struct HSProxyCallbackState
{
	void *orig_state;
	IndexBulkDeleteCallback orig_callback;
	ItemPointerData last_decoded_tid;
	bool last_delete_result;
} HSProxyCallbackState;

/*
 * IndexBulkDeleteCallback for determining if a hyperstore index entry (TID)
 * can be deleted.
 *
 * The state pointer contains to original callback and state.
 */
static bool
hsproxy_can_delete_tid(ItemPointer tid, void *state)
{
	HSProxyCallbackState *delstate = state;
	ItemPointerData decoded_tid;

	/* If this TID is not pointing to the compressed relation, there is
	 * nothing to do */
	if (!is_compressed_tid(tid))
		return false;

	/* Decode the TID into the original compressed relation TID */
	hyperstore_tid_decode(&decoded_tid, tid);

	/* Check if this is the same TID as in the last call. This is a simple
	 * optimization for when we are just traversing "compressed" TIDs that all
	 * point into the same compressed tuple. */
	if (ItemPointerIsValid(&delstate->last_decoded_tid) &&
		ItemPointerEquals(&delstate->last_decoded_tid, &decoded_tid))
		return delstate->last_delete_result;

	/* Ask the original callback whether the (decoded) TID can be deleted */
	ItemPointerCopy(&decoded_tid, &delstate->last_decoded_tid);
	delstate->last_delete_result = delstate->orig_callback(&decoded_tid, delstate->orig_state);

	return delstate->last_delete_result;
}

static IndexBulkDeleteResult *
bulkdelete_one_index(Relation hsrel, Relation indexrel, IndexBulkDeleteResult *istat,
					 BufferAccessStrategy strategy, HSProxyCallbackState *delstate)
{
	IndexVacuumInfo ivinfo;

	ItemPointerSetInvalid(&delstate->last_decoded_tid);

	ivinfo.index = indexrel;
#if PG16_GE
	ivinfo.heaprel = hsrel;
#endif
	ivinfo.analyze_only = false;
	ivinfo.report_progress = false;
	ivinfo.estimated_count = true;
	ivinfo.message_level = DEBUG2;
	ivinfo.num_heap_tuples = hsrel->rd_rel->reltuples;
	ivinfo.strategy = strategy;

	IndexBulkDeleteResult *result =
		index_bulk_delete(&ivinfo, istat, hsproxy_can_delete_tid, delstate);

	return result;
}

typedef struct HSProxyVacuumState
{
	IndexBulkDeleteResult stats; /* Must be first. Aggregate stats */
	int nindexes;
	/* Stats for each (sub-)index */
	IndexBulkDeleteResult indstats[FLEXIBLE_ARRAY_MEMBER];
} HSProxyVacuumState;

#define HSPROXY_VACUUM_STATE_SIZE(nindexes)                                                        \
	(sizeof(HSProxyVacuumState) + (sizeof(IndexBulkDeleteResult)) * (nindexes))

/*
 * Bulkdelete. Called by vacuum on the compressed relation.
 *
 * An index AM typically goes through the whole index in this function,
 * calling the IndexBulkDeleteCallback function for every TID in the index to
 * ask whether it should be removed or not.
 *
 * In the hsproxy case, this call is simply relayed to all indexes on the
 * user-visible hyperstore relation, calling our own callback instead.
 */
static IndexBulkDeleteResult *
hsproxy_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				   IndexBulkDeleteCallback callback, void *callback_state)
{
	Oid hyperstore_relid = get_hyperstore_relid(info->index->rd_index->indrelid);
	Relation hsrel = table_open(hyperstore_relid, ShareUpdateExclusiveLock);
	HSProxyCallbackState delstate = {
		.orig_callback = callback,
		.orig_state = callback_state,
	};
	Relation *indrels;
	int nindexes = 0;
	HSProxyVacuumState *vacstate = (HSProxyVacuumState *) stats;

	vac_open_indexes(hsrel, RowExclusiveLock, &nindexes, &indrels);

	/*
	 * If first time called, allocate state. Note that we overload the stats
	 * pointer to keep individual IndexBulkDeleteResults for each proxied
	 * index. This same state later gets passed to the vacuum_cleanup callback
	 * where stats for each index can be reported individually. The PG vacuum
	 * code takes care of freeing the allocated memory later.
	 */
	if (vacstate == NULL)
	{
		vacstate = palloc0(HSPROXY_VACUUM_STATE_SIZE(nindexes));
		vacstate->nindexes = nindexes;
	}

	for (int i = 0; i < nindexes; i++)
	{
		/* There should never be any hsproxy indexes that we proxy */
		Assert(indrels[i]->rd_indam->ambuildempty != hsproxy_buildempty);
		bulkdelete_one_index(hsrel, indrels[i], &vacstate->indstats[i], info->strategy, &delstate);
	}

	vac_close_indexes(nindexes, indrels, NoLock);
	table_close(hsrel, NoLock);

	return &vacstate->stats;
}

static IndexBulkDeleteResult *
vacuumcleanup_one_index(Relation hsrel, Relation indexrel, IndexBulkDeleteResult *istat,
						bool analyze_only, BufferAccessStrategy strategy)
{
	IndexVacuumInfo ivinfo;

	ivinfo.index = indexrel;
#if PG16_GE
	ivinfo.heaprel = hsrel;
#endif
	ivinfo.analyze_only = analyze_only;
	ivinfo.report_progress = false;
	ivinfo.estimated_count = true;
	ivinfo.message_level = DEBUG2;
	ivinfo.num_heap_tuples = hsrel->rd_rel->reltuples;
	ivinfo.strategy = strategy;

	IndexBulkDeleteResult *result = index_vacuum_cleanup(&ivinfo, istat);

	if (result != NULL && !result->estimated_count)
	{
		/*
		 * Update vacuum stats for the index. This includes only stats for the
		 * compressed relation, so not sure how useful that stats would be.
		 */
		vac_update_relstats(indexrel,
							result->num_pages,
							result->num_index_tuples,
							0,
							false,
							InvalidTransactionId,
							InvalidMultiXactId,
#if PG15_GE
							NULL,
							NULL,
#endif
							false);
	}

	return result;
}

/*
 * post-VACUUM cleanup
 *
 * This function is sometimes called without bulkdelete having been called
 * first. Therefore, we cannot always assume that vacstate has been created.
 */
static IndexBulkDeleteResult *
hsproxy_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	Oid hyperstore_relid = get_hyperstore_relid(info->index->rd_index->indrelid);
	Relation hsrel = table_open(hyperstore_relid, ShareUpdateExclusiveLock);
	HSProxyVacuumState *vacstate = (HSProxyVacuumState *) stats;
	Relation *indrels;
	int nindexes = 0;

	vac_open_indexes(hsrel, RowExclusiveLock, &nindexes, &indrels);

	/*
	 * In some cases, bulkdelete hasn't run, so need to allocate vacuum
	 * state. (See bulkdelete above for more information.)
	 */
	if (vacstate == NULL)
	{
		vacstate = palloc0(HSPROXY_VACUUM_STATE_SIZE(nindexes));
		vacstate->nindexes = nindexes;
	}

	for (int i = 0; i < nindexes; i++)
	{
		/* There should never be any hsproxy indexes that we proxy */
		Assert(indrels[i]->rd_indam->ambuildempty != hsproxy_buildempty);
		IndexBulkDeleteResult *result = vacuumcleanup_one_index(hsrel,
																indrels[i],
																&vacstate->indstats[i],
																info->analyze_only,
																info->strategy);

		/* Accumulate total stats for all indexes combined and return as the
		 * resulting stats. */
		vacstate->stats.pages_deleted += result->pages_deleted;
		vacstate->stats.tuples_removed += result->tuples_removed;
#if PG14_GE
		vacstate->stats.pages_newly_deleted += result->pages_newly_deleted;
#else
		vacstate->stats.pages_removed += result->pages_removed;
#endif
	}

	vac_close_indexes(nindexes, indrels, NoLock);
	table_close(hsrel, NoLock);

	return stats;
}

/*
 * Estimate cost of an indexscan.
 *
 * The proxy index should never be used for any queries, so it is important to
 * make the cost so high that the index is effectively never used in a query.
 */
static void
hsproxy_costestimate(struct PlannerInfo *root, struct IndexPath *path, double loop_count,
					 Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity,
					 double *indexCorrelation, double *indexPages)
{
	*indexTotalCost = *indexStartupCost = *indexCorrelation = INFINITY;
	*indexSelectivity = 1;
	*indexPages = UINT32_MAX;
}

/* parse index reloptions */
static bytea *
hsproxy_options(Datum reloptions, bool validate)
{
	return NULL;
}

static bool
hsproxy_validate(Oid opclassoid)
{
	/* Not really using opclass, so simply return true */
	return true;
}

/*
 * Index insert.
 *
 * Currently needed as a dummy. Could be used to insert into all indexes on
 * the hyperstore rel when inserting data into the compressed rel during,
 * e.g., recompression.
 */
static bool
hsproxy_insert(Relation indexRelation, Datum *values, bool *isnull, ItemPointer heap_tid,
			   Relation heapRelation, IndexUniqueCheck checkUnique,
#if PG14_GE
			   bool indexUnchanged,
#endif
			   struct IndexInfo *indexInfo)
{
	return true;
}

Datum
hsproxy_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies = 0;
	amroutine->amsupport = 1;
	amroutine->amoptsprocnum = 0;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = false;
	amroutine->amoptionalkey = false;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false;
#if PG16_GE
	amroutine->amsummarizing = false;
#endif
	amroutine->amparallelvacuumoptions = 0;
	amroutine->amkeytype = InvalidOid;

	/* Callbacks */
	amroutine->ambuild = hsproxy_build;
	amroutine->ambuildempty = hsproxy_buildempty;
	amroutine->ambulkdelete = hsproxy_bulkdelete;
	amroutine->amvacuumcleanup = hsproxy_vacuumcleanup;
	amroutine->amcostestimate = hsproxy_costestimate;
	amroutine->amoptions = hsproxy_options;

	/* Optional callbacks */
	amroutine->aminsert = hsproxy_insert;
	amroutine->amcanreturn = NULL;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = hsproxy_validate;
#if PG14_GE
	amroutine->amadjustmembers = NULL;
#endif
	amroutine->ambeginscan = NULL;
	amroutine->amrescan = NULL;
	amroutine->amgettuple = NULL;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = NULL;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}
