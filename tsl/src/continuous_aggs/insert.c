/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <fmgr.h>
#include <commands/dbcommands.h>
#include <commands/trigger.h>
#include <lib/stringinfo.h>
#include <miscadmin.h>
#include <utils/hsearch.h>
#include <access/tupconvert.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <access/xact.h>

#include <scanner.h>

#include "compat/compat.h"

#include "chunk.h"
#include "dimension.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "invalidation.h"
#include "export.h"
#include "partitioning.h"
#include "utils.h"
#include "time_bucket.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/continuous_agg.h"

#include "continuous_aggs/insert.h"

/*
 * When tuples in a hypertable that has a continuous aggregate are modified, the
 * lowest modified value and the greatest modified value must be tracked over
 * the course of a transaction or statement. At the end of the statement these
 * values will be inserted into the proper cache invalidation log table for
 * their associated hypertable if they are below the speculative materialization
 * watermark (or, if in REPEATABLE_READ isolation level or higher, they will be
 * inserted no matter what as we cannot see if a materialization transaction has
 * started and moved the watermark during our transaction in that case).
 *
 * We accomplish this at the transaction level by keeping a hash table of each
 * hypertable that has been modified in the transaction and the lowest and
 * greatest modified values. The hashtable will be updated via a trigger that
 * will be called for every row that is inserted, updated or deleted. We use a
 * hashtable because we need to keep track of this on a per hypertable basis and
 * multiple can have tuples modified during a single transaction. (And if we
 * move to per-chunk cache-invalidation it makes it even easier).
 *
 */
typedef struct ContinuousAggsCacheInvalEntry
{
	int32 hypertable_id;
	Oid hypertable_relid;
	int32 entry_id; /*
					 * This is what actually gets written to the hypertable log. It can be the same
					 * as the hypertable_id for normal hypertables. In the distributed case it is
					 * the ID of the parent hypertable in the Access Node.
					 */
	Dimension hypertable_open_dimension;
	Oid previous_chunk_relid;
	AttrNumber previous_chunk_open_dimension;
	bool value_is_set;
	int64 lowest_modified_value;
	int64 greatest_modified_value;
} ContinuousAggsCacheInvalEntry;

static int64 get_lowest_invalidated_time_for_hypertable(Oid hypertable_relid);

#define CA_CACHE_INVAL_INIT_HTAB_SIZE 64

static HTAB *continuous_aggs_cache_inval_htab = NULL;
static MemoryContext continuous_aggs_trigger_mctx = NULL;

void _continuous_aggs_cache_inval_init(void);
void _continuous_aggs_cache_inval_fini(void);

static void
cache_inval_init()
{
	HASHCTL ctl;

	Assert(continuous_aggs_trigger_mctx == NULL);

	continuous_aggs_trigger_mctx = AllocSetContextCreate(TopTransactionContext,
														 "ContinuousAggsTriggerCtx",
														 ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int32);
	ctl.entrysize = sizeof(ContinuousAggsCacheInvalEntry);
	ctl.hcxt = continuous_aggs_trigger_mctx;

	continuous_aggs_cache_inval_htab = hash_create("TS Continuous Aggs Cache Inval",
												   CA_CACHE_INVAL_INIT_HTAB_SIZE,
												   &ctl,
												   HASH_ELEM | HASH_BLOBS);
};

static int64
tuple_get_time(Dimension *d, HeapTuple tuple, AttrNumber col, TupleDesc tupdesc)
{
	Datum datum;
	bool isnull;
	Oid dimtype;

	datum = heap_getattr(tuple, col, tupdesc, &isnull);

	if (NULL != d->partitioning)
	{
		Oid collation = TupleDescAttr(tupdesc, col)->attcollation;
		datum = ts_partitioning_func_apply(d->partitioning, collation, datum);
	}

	Assert(d->type == DIMENSION_TYPE_OPEN);

	dimtype = ts_dimension_get_partition_type(d);

	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_NOT_NULL_VIOLATION),
				 errmsg("NULL value in column \"%s\" violates not-null constraint",
						NameStr(d->fd.column_name)),
				 errhint("Columns used for time partitioning cannot be NULL")));

	return ts_time_value_to_internal(datum, dimtype);
}

static inline void
cache_inval_entry_init(ContinuousAggsCacheInvalEntry *cache_entry, int32 hypertable_id,
					   int32 entry_id)
{
	Cache *ht_cache = ts_hypertable_cache_pin();
	/* NOTE: we can remove the id=>relid scan, if it becomes an issue, by getting the
	 * hypertable_relid directly from the Chunk*/
	Hypertable *ht = ts_hypertable_cache_get_entry_by_id(ht_cache, hypertable_id);
	cache_entry->hypertable_id = hypertable_id;
	cache_entry->entry_id = entry_id;
	cache_entry->hypertable_relid = ht->main_table_relid;
	cache_entry->hypertable_open_dimension = *hyperspace_get_open_dimension(ht->space, 0);
	if (cache_entry->hypertable_open_dimension.partitioning != NULL)
	{
		PartitioningInfo *open_dim_part_info =
			MemoryContextAllocZero(continuous_aggs_trigger_mctx, sizeof(*open_dim_part_info));
		*open_dim_part_info = *cache_entry->hypertable_open_dimension.partitioning;
		cache_entry->hypertable_open_dimension.partitioning = open_dim_part_info;
	}
	cache_entry->previous_chunk_relid = InvalidOid;
	cache_entry->value_is_set = false;
	cache_entry->lowest_modified_value = INVAL_POS_INFINITY;
	cache_entry->greatest_modified_value = INVAL_NEG_INFINITY;
	ts_cache_release(ht_cache);
}

static inline void
cache_entry_switch_to_chunk(ContinuousAggsCacheInvalEntry *cache_entry, Oid chunk_id,
							Relation chunk_relation)
{
	Chunk *modified_tuple_chunk = ts_chunk_get_by_relid(chunk_id, false);
	if (modified_tuple_chunk == NULL)
		elog(ERROR, "continuous agg trigger function must be called on hypertable chunks only");

	cache_entry->previous_chunk_relid = modified_tuple_chunk->table_id;
	cache_entry->previous_chunk_open_dimension =
		get_attnum(chunk_relation->rd_id,
				   NameStr(cache_entry->hypertable_open_dimension.fd.column_name));

	if (cache_entry->previous_chunk_open_dimension == InvalidAttrNumber)
		elog(ERROR, "continuous agg trigger function must be called on hypertable chunks only");
}

static inline void
update_cache_entry(ContinuousAggsCacheInvalEntry *cache_entry, int64 timeval)
{
	cache_entry->value_is_set = true;
	if (timeval < cache_entry->lowest_modified_value)
		cache_entry->lowest_modified_value = timeval;
	if (timeval > cache_entry->greatest_modified_value)
		cache_entry->greatest_modified_value = timeval;
}

/*
 * Trigger to store what the max/min updated values are for a function.
 * This is used by continuous aggregates to ensure that the aggregated values
 * are updated correctly. Upon creating a continuous aggregate for a hypertable,
 * this trigger should be registered, if it does not already exist.
 */
Datum
continuous_agg_trigfn(PG_FUNCTION_ARGS)
{
	/*
	 * Use TriggerData to determine which row to return/work with, in the case
	 * of updates, we'll need to call the functions twice, once with the old
	 * rows (which act like deletes) and once with the new rows.
	 */
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	char *hypertable_id_str, *parent_hypertable_id_str;
	int32 hypertable_id, parent_hypertable_id = 0;
	bool is_distributed_hypertable_trigger = false;
	if (trigdata->tg_trigger->tgnargs < 0)
		elog(ERROR, "must supply hypertable id");

	hypertable_id_str = trigdata->tg_trigger->tgargs[0];
	hypertable_id = atol(hypertable_id_str);

	if (trigdata->tg_trigger->tgnargs > 1)
	{
		parent_hypertable_id_str = trigdata->tg_trigger->tgargs[1];
		parent_hypertable_id = atol(parent_hypertable_id_str);
		is_distributed_hypertable_trigger = true;
	}

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "continuous agg trigger function must be called by trigger manager");
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) || !TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "continuous agg trigger function must be called in per row after trigger");
	execute_cagg_trigger(hypertable_id,
						 trigdata->tg_relation,
						 trigdata->tg_trigtuple,
						 trigdata->tg_newtuple,
						 TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event),
						 is_distributed_hypertable_trigger,
						 parent_hypertable_id);
	if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		return PointerGetDatum(trigdata->tg_trigtuple);
	else
		return PointerGetDatum(trigdata->tg_newtuple);
}

/*
 * chunk_tuple is the tuple from trigdata->tg_trigtuple
 * i.e. the one being/inserts/deleted/updated.
 * (for updates: this is the row before modification)
 * chunk_newtuple is the tuple from trigdata->tg_newtuple.
 */
void
execute_cagg_trigger(int32 hypertable_id, Relation chunk_rel, HeapTuple chunk_tuple,
					 HeapTuple chunk_newtuple, bool update, bool is_distributed_hypertable_trigger,
					 int32 parent_hypertable_id)
{
	ContinuousAggsCacheInvalEntry *cache_entry;
	bool found;
	int64 timeval;
	Oid chunk_relid = chunk_rel->rd_id;
	/* On first call, init the mctx and hash table */
	if (!continuous_aggs_cache_inval_htab)
		cache_inval_init();

	cache_entry = (ContinuousAggsCacheInvalEntry *)
		hash_search(continuous_aggs_cache_inval_htab, &hypertable_id, HASH_ENTER, &found);

	if (!found)
		cache_inval_entry_init(cache_entry,
							   hypertable_id,
							   is_distributed_hypertable_trigger ? parent_hypertable_id :
																   hypertable_id);

	/* handle the case where we need to repopulate the cached chunk data */
	if (cache_entry->previous_chunk_relid != chunk_relid)
		cache_entry_switch_to_chunk(cache_entry, chunk_relid, chunk_rel);

	timeval = tuple_get_time(&cache_entry->hypertable_open_dimension,
							 chunk_tuple,
							 cache_entry->previous_chunk_open_dimension,
							 RelationGetDescr(chunk_rel));

	update_cache_entry(cache_entry, timeval);

	if (!update)
		return;

	/* on update we need to invalidate the new time value as well as the old one */
	timeval = tuple_get_time(&cache_entry->hypertable_open_dimension,
							 chunk_newtuple,
							 cache_entry->previous_chunk_open_dimension,
							 RelationGetDescr(chunk_rel));

	update_cache_entry(cache_entry, timeval);
}

static void
cache_inval_entry_write(ContinuousAggsCacheInvalEntry *entry)
{
	int64 liv;

	if (!entry->value_is_set)
		return;

	Cache *ht_cache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry_by_id(ht_cache, entry->hypertable_id);
	bool is_distributed_member = hypertable_is_distributed_member(ht);
	ts_cache_release(ht_cache);

	/* The materialization worker uses a READ COMMITTED isolation level by default. Therefore, if we
	 * use a stronger isolation level, the isolation thereshold could update without us seeing the
	 * new value. In order to prevent serialization errors, we always append invalidation entries in
	 * the case when we're using a strong enough isolation level that we won't see the new
	 * threshold. The same applies for distributed member invalidation triggers of hypertables.
	 * The materializer can handle invalidations that are beyond the threshold gracefully.
	 */
	if (IsolationUsesXactSnapshot() || is_distributed_member)
	{
		invalidation_hyper_log_add_entry(entry->entry_id,
										 entry->lowest_modified_value,
										 entry->greatest_modified_value);
		return;
	}

	liv = get_lowest_invalidated_time_for_hypertable(entry->hypertable_relid);

	if (entry->lowest_modified_value < liv)
		invalidation_hyper_log_add_entry(entry->entry_id,
										 entry->lowest_modified_value,
										 entry->greatest_modified_value);
};

static void
cache_inval_cleanup(void)
{
	Assert(continuous_aggs_cache_inval_htab != NULL);
	hash_destroy(continuous_aggs_cache_inval_htab);
	MemoryContextDelete(continuous_aggs_trigger_mctx);

	continuous_aggs_cache_inval_htab = NULL;
	continuous_aggs_trigger_mctx = NULL;
};

static void
cache_inval_htab_write(void)
{
	HASH_SEQ_STATUS hash_seq;
	ContinuousAggsCacheInvalEntry *current_entry;
	Catalog *catalog;

	if (hash_get_num_entries(continuous_aggs_cache_inval_htab) == 0)
		return;

	catalog = ts_catalog_get();

	/* The invalidation threshold must remain locked until the end of
	 * the transaction to ensure the materializer will see our updates,
	 * so we explicitly lock it here
	 */
	LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
					AccessShareLock);

	hash_seq_init(&hash_seq, continuous_aggs_cache_inval_htab);
	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
		cache_inval_entry_write(current_entry);
};

/*
 * We use TopTransactionContext for our cached invalidations.
 * We need to make sure cache_inval_cleanup() is always called after cache_inval_htab_write().
 * We need this memory context to survive the transaction lifetime so that cache_inval_cleanup()
 * does not attempt to tear down memory that has already been freed due to a transaction ending.
 *
 * The order of operations in postgres can be this:
 * CallXactCallbacks(XACT_EVENT_PRE_PREPARE);
 * ...
 * CallXactCallbacks(XACT_EVENT_PREPARE);
 * ...
 * MemoryContextDelete(TopTransactionContext);
 *
 * or that:
 * CallXactCallbacks(XACT_EVENT_PRE_COMMIT);
 * ...
 * CallXactCallbacks(XACT_EVENT_COMMIT);
 * ...
 * MemoryContextDelete(TopTransactionContext);
 *
 * In the case of a 2PC transaction, we need to make sure to apply the invalidations at
 * XACT_EVENT_PRE_PREPARE time, before TopTransactionContext is torn down by PREPARE TRANSACTION.
 * Otherwise, we are unable to call cache_inval_cleanup() without corrupting the memory. For
 * this reason, we also deallocate at XACT_EVENT_PREPARE time.
 *
 * For local transactions we apply the invalidations at XACT_EVENT_PRE_COMMIT time.
 * Similar care is taken of parallel workers and aborting transactions.
 */
static void
continuous_agg_xact_invalidation_callback(XactEvent event, void *arg)
{
	/* Return quickly if we never initialize the hashtable */
	if (!continuous_aggs_cache_inval_htab)
		return;

	switch (event)
	{
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
			cache_inval_htab_write();
			break;
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			cache_inval_cleanup();
			break;
		default:
			break;
	}
}

void
_continuous_aggs_cache_inval_init(void)
{
	RegisterXactCallback(continuous_agg_xact_invalidation_callback, NULL);
}

void
_continuous_aggs_cache_inval_fini(void)
{
	UnregisterXactCallback(continuous_agg_xact_invalidation_callback, NULL);
}

static ScanTupleResult
invalidation_tuple_found(TupleInfo *ti, void *min)
{
	bool isnull;
	Datum watermark =
		slot_getattr(ti->slot, Anum_continuous_aggs_invalidation_threshold_watermark, &isnull);

	Assert(!isnull);

	if (DatumGetInt64(watermark) < *((int64 *) min))
		*((int64 *) min) = DatumGetInt64(watermark);

	/*
	 * Return SCAN_CONTINUE because we check for multiple tuples as an error
	 * condition.
	 */
	return SCAN_CONTINUE;
}

int64
get_lowest_invalidated_time_for_hypertable(Oid hypertable_relid)
{
	int64 min_val = INVAL_POS_INFINITY;
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx scanctx;

	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_invalidation_threshold_pkey_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(ts_hypertable_relid_to_id(hypertable_relid)));
	scanctx = (ScannerCtx){
		.table = catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
		.index = catalog_get_index(catalog,
								   CONTINUOUS_AGGS_INVALIDATION_THRESHOLD,
								   CONTINUOUS_AGGS_INVALIDATION_THRESHOLD_PKEY),
		.nkeys = 1,
		.scankey = scankey,
		.tuple_found = &invalidation_tuple_found,
		.filter = NULL,
		.data = &min_val,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
		.result_mctx = NULL,
	};

	/* if we don't find any watermark, then we've never done any materialization
	 * we'll treat this as if the invalidation timestamp is at min value, since
	 * the first materialization needs to scan the entire table anyway; the
	 * invalidations are redundant.
	 */
	if (!ts_scanner_scan_one(&scanctx, false, "invalidation watermark"))
		return INVAL_NEG_INFINITY;

	return min_val;
}
