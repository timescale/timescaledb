
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <executor/spi.h>
#include <fmgr.h>
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
#include <interval.h>
#include <continuous_agg.h>

#include "compat.h"

#include "chunk.h"
#include "dimension.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "export.h"
#include "partitioning.h"

#include "utils.h"
#include "time_bucket.h"

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
	Dimension hypertable_open_dimension;
	int64 modification_time;
	int64 minimum_invalidation_time; /* inclusive */

	Oid previous_chunk_relid;
	AttrNumber previous_chunk_open_dimension;

	bool value_is_set;
	int64 lowest_modified_value;
	int64 greatest_modified_value;
} ContinuousAggsCacheInvalEntry;

static void append_invalidation_entry(ContinuousAggsCacheInvalEntry *entry);
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
														 "ConinuousAggsTriggerCtx",
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
cache_inval_entry_init(ContinuousAggsCacheInvalEntry *cache_entry, int32 hypertable_id)
{
	Cache *ht_cache = ts_hypertable_cache_pin();
	int64 ignore_invalidation_older_than;
	/* NOTE: we can remove the id=>relid scan, if it becomes an issue, by getting the
	 * hypertable_relid directly from the Chunk*/
	Hypertable *ht = ts_hypertable_cache_get_entry_by_id(ht_cache, hypertable_id);
	cache_entry->hypertable_relid = ht->main_table_relid;
	cache_entry->hypertable_open_dimension = *hyperspace_get_open_dimension(ht->space, 0);
	if (cache_entry->hypertable_open_dimension.partitioning != NULL)
	{
		PartitioningInfo *open_dim_part_info =
			MemoryContextAllocZero(continuous_aggs_trigger_mctx, sizeof(*open_dim_part_info));
		*open_dim_part_info = *cache_entry->hypertable_open_dimension.partitioning;
		cache_entry->hypertable_open_dimension.partitioning = open_dim_part_info;
	}
	cache_entry->modification_time = ts_get_now_internal(&cache_entry->hypertable_open_dimension);
	ignore_invalidation_older_than = ts_hypertable_get_max_ignore_invalidation_older_than(ht);
	Assert(ignore_invalidation_older_than >= 0);
	cache_entry->minimum_invalidation_time =
		ts_continuous_aggs_get_minimum_invalidation_time(cache_entry->modification_time,
														 ignore_invalidation_older_than);

	cache_entry->previous_chunk_relid = InvalidOid;
	cache_entry->value_is_set = false;
	cache_entry->lowest_modified_value = PG_INT64_MAX;
	cache_entry->greatest_modified_value = PG_INT64_MIN;
	ts_cache_release(ht_cache);
}

static inline void
cache_entry_switch_to_chunk(ContinuousAggsCacheInvalEntry *cache_entry, Oid chunk_id,
							Relation chunk_relation)
{
	Chunk *modified_tuple_chunk = ts_chunk_get_by_relid(chunk_id, 0, false);
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
	if (timeval < cache_entry->minimum_invalidation_time)
		return;

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
	char *hypertable_id_str;
	int32 hypertable_id;
	ContinuousAggsCacheInvalEntry *cache_entry;
	bool found;
	int64 timeval;
	if (trigdata->tg_trigger->tgnargs < 0)
		elog(ERROR, "must supply hypertable id");

	hypertable_id_str = trigdata->tg_trigger->tgargs[0];
	hypertable_id = atol(hypertable_id_str);

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "continuous agg trigger function must be called by trigger manager");
	if (!TRIGGER_FIRED_AFTER(trigdata->tg_event) || !TRIGGER_FIRED_FOR_ROW(trigdata->tg_event))
		elog(ERROR, "continuous agg trigger function must be called in per row after trigger");

	/* On first call, init the mctx and hash table*/
	if (!continuous_aggs_cache_inval_htab)
		cache_inval_init();

	cache_entry = (ContinuousAggsCacheInvalEntry *)
		hash_search(continuous_aggs_cache_inval_htab, &hypertable_id, HASH_ENTER, &found);

	if (!found)
		cache_inval_entry_init(cache_entry, hypertable_id);

	/* handle the case where we need to repopulate the cached chunk data */
	if (cache_entry->previous_chunk_relid != trigdata->tg_relation->rd_id)
		cache_entry_switch_to_chunk(cache_entry,
									trigdata->tg_relation->rd_id,
									trigdata->tg_relation);

	timeval = tuple_get_time(&cache_entry->hypertable_open_dimension,
							 trigdata->tg_trigtuple,
							 cache_entry->previous_chunk_open_dimension,
							 RelationGetDescr(trigdata->tg_relation));

	update_cache_entry(cache_entry, timeval);

	if (!TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		return PointerGetDatum(trigdata->tg_trigtuple);

	/* on update we need to invalidate the new time value as well as the old one*/
	timeval = tuple_get_time(&cache_entry->hypertable_open_dimension,
							 trigdata->tg_newtuple,
							 cache_entry->previous_chunk_open_dimension,
							 RelationGetDescr(trigdata->tg_relation));

	update_cache_entry(cache_entry, timeval);

	return PointerGetDatum(trigdata->tg_newtuple);
};

static void
cache_inval_entry_write(ContinuousAggsCacheInvalEntry *entry)
{
	int64 liv;

	if (!entry->value_is_set)
		return;

	/* The materialization worker uses a READ COMMITTED isolation level by default. Therefore, if we
	 * use a stronger isolation level, the isolation thereshold could update without us seeing the
	 * new value. In order to prevent serialization errors, we always append invalidation entires in
	 * the case when we're using a strong enough isolation level that we won't see the new
	 * threshold; the materializer can handle invalidations that are beyond the threshold
	 * gracefully.
	 */
	if (IsolationUsesXactSnapshot())
	{
		append_invalidation_entry(entry);
		return;
	}

	liv = get_lowest_invalidated_time_for_hypertable(entry->hypertable_relid);
	if (entry->lowest_modified_value < liv)
		append_invalidation_entry(entry);
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
static void
continuous_agg_xact_invalidation_callback(XactEvent event, void *arg)
{
	/* Return quickly if we never initialize the hashtable */
	if (!continuous_aggs_cache_inval_htab)
		return;

	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
			cache_inval_htab_write();
			cache_inval_cleanup();
			break;
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
	Form_continuous_aggs_invalidation_threshold invalidation =
		(Form_continuous_aggs_invalidation_threshold) GETSTRUCT(ti->tuple);
	if (invalidation->watermark < *(int64 *) min)
		*(int64 *) min = invalidation->watermark;

	/*
	 * Return SCAN_CONTINUE because we check for multiple tuples as an error
	 * condition.
	 */
	return SCAN_CONTINUE;
}

int64
get_lowest_invalidated_time_for_hypertable(Oid hypertable_relid)
{
	int64 min_val = PG_INT64_MAX;
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
		return PG_INT64_MIN;

	return min_val;
}

static void
append_invalidation_entry(ContinuousAggsCacheInvalEntry *entry)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	TupleDesc desc;
	Datum values[Natts_continuous_aggs_hypertable_invalidation_log];
	CatalogSecurityContext sec_ctx;
	bool nulls[Natts_continuous_aggs_hypertable_invalidation_log] = { false };
	int32 hypertable_id = ts_hypertable_relid_to_id(entry->hypertable_relid);

	Assert(entry->lowest_modified_value <= entry->greatest_modified_value);

	rel = table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_HYPERTABLE_INVALIDATION_LOG),
					 RowExclusiveLock);
	desc = RelationGetDescr(rel);

	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_hypertable_invalidation_log_hypertable_id)] =
		ObjectIdGetDatum(hypertable_id);
	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_hypertable_invalidation_log_modification_time)] =
		Int64GetDatum(entry->modification_time);
	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_hypertable_invalidation_log_lowest_modified_value)] =
		Int64GetDatum(entry->lowest_modified_value);
	values[AttrNumberGetAttrOffset(
		Anum_continuous_aggs_hypertable_invalidation_log_greatest_modified_value)] =
		Int64GetDatum(entry->greatest_modified_value);

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	/* Lock will be released by the transaction end. Since this is called on the
	 * commit hook, this should be soon.
	 */
	table_close(rel, NoLock);
}
