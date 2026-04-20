/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/hash.h>
#include <access/xact.h>
#include <utils/builtins.h>
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "compat/compat.h"

#include "continuous_aggs/insert.h"
#include "debug_point.h"
#include "dimension.h"
#include "guc.h"
#include "hypertable.h"
#include "invalidation.h"
#include "partitioning.h"
#include "time_utils.h"
#include "ts_catalog/catalog.h"

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
 * greatest modified values. The hashtable will be updated via ModifyHypertable
 * for every row that is inserted, updated or deleted.
 * We use a hashtable because we need to keep track of this on a per hypertable
 * basis and multiple can have tuples modified during a single transaction.
 * (And if we move to per-chunk cache-invalidation it makes it even easier).
 *
 */
typedef struct ContinuousAggsCacheInvalEntry
{
	Oid chunk_relid;
	int32 hypertable_id;
	Dimension hypertable_open_dimension;
	AttrNumber open_dimension_attno;
	bool value_is_set;
	int64 lowest_modified_value;
	int64 greatest_modified_value;
} ContinuousAggsCacheInvalEntry;

typedef struct ContinuousAggsCacheHyperInvalThresholdEntry
{
	int32 hypertable_id;
	int64 watermark;
} ContinuousAggsCacheHyperInvalThresholdEntry;

static int64 get_lowest_invalidated_time_for_hypertable(int32 hypertable_id);
static inline int64 cache_get_lowest_invalidated_time_for_hypertable(int32 hypertable_id);

#define CA_CACHE_INVAL_INIT_HTAB_SIZE 64

static HTAB *continuous_aggs_cache_inval_htab = NULL;
static HTAB *continuous_aggs_cache_hyper_inval_threshold_htab = NULL;

static MemoryContext continuous_aggs_invalidation_mctx = NULL;

/* Backfill tracker state — initialized lazily on first backfill insert */
static HTAB *backfill_tracker_htab = NULL;
static MemoryContext backfill_tracker_mctx = NULL;
static int64 backfill_now_internal = 0; /* cached current time, computed once per transaction */

static inline void cache_inval_entry_init(ContinuousAggsCacheInvalEntry *cache_entry,
										  int32 hypertable_id, Oid chunk_relid);
static inline ContinuousAggsCacheInvalEntry *get_cache_inval_entry(int32 hypertable_id,
																   Oid chunk_relid);
static void cache_inval_cleanup(void);
static void cache_inval_htab_write(void);
static void continuous_agg_xact_invalidation_callback(XactEvent event, void *arg);
static ScanTupleResult invalidation_tuple_found(TupleInfo *ti, void *min);

/* Backfill tracker forward declarations */
static void backfill_tracker_flush(void);
static void backfill_tracker_cleanup(void);

static void
cache_inval_init()
{
	HASHCTL ctl;

	Assert(continuous_aggs_invalidation_mctx == NULL);

	continuous_aggs_invalidation_mctx = AllocSetContextCreate(TopTransactionContext,
															  "ContinuousAggsInvalidationCtx",
															  ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(ContinuousAggsCacheInvalEntry);
	ctl.hcxt = continuous_aggs_invalidation_mctx;

	continuous_aggs_cache_inval_htab = hash_create("TS Continuous Aggs Cache Inval",
												   CA_CACHE_INVAL_INIT_HTAB_SIZE,
												   &ctl,
												   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(int32);
	ctl.entrysize = sizeof(ContinuousAggsCacheHyperInvalThresholdEntry);
	ctl.hcxt = continuous_aggs_invalidation_mctx;

	continuous_aggs_cache_hyper_inval_threshold_htab =
		hash_create("TS Continuous Aggs Hypertable Invalidation Threshold",
					CA_CACHE_INVAL_INIT_HTAB_SIZE,
					&ctl,
					HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

static void
update_cache_from_tuple(ContinuousAggsCacheInvalEntry *cache_entry, HeapTuple tuple,
						TupleDesc tupdesc)
{
	Datum datum;
	bool isnull;
	Oid dimtype;
	Dimension *d = &cache_entry->hypertable_open_dimension;
	AttrNumber col = cache_entry->open_dimension_attno;

	Assert(d->type == DIMENSION_TYPE_OPEN);

	datum = heap_getattr(tuple, col, tupdesc, &isnull);
	/*
	 * Even though there are NOT NULL constraints on time columns checking these happens
	 * after invalidation processing so we skip nulls here to allow for normal postgres
	 * error handling for these NULL values.
	 */
	if (isnull)
	{
		return;
	}

	dimtype = ts_dimension_get_partition_type(d);
	int64 timeval = ts_time_value_to_internal(datum, dimtype);

	cache_entry->value_is_set = true;
	if (timeval < cache_entry->lowest_modified_value)
	{
		cache_entry->lowest_modified_value = timeval;
	}
	if (timeval > cache_entry->greatest_modified_value)
	{
		cache_entry->greatest_modified_value = timeval;
	}
}

static inline void
cache_inval_entry_init(ContinuousAggsCacheInvalEntry *cache_entry, int32 hypertable_id,
					   Oid chunk_relid)
{
	Cache *ht_cache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry_by_id(ht_cache, hypertable_id);
	Ensure(ht, "could not find hypertable with id %d", hypertable_id);

	const Dimension *open_dim = hyperspace_get_open_dimension(ht->space, 0);
	Ensure(open_dim, "hypertable %d has no open partitioning dimension", hypertable_id);

	cache_entry->chunk_relid = chunk_relid;
	cache_entry->hypertable_id = hypertable_id;
	cache_entry->hypertable_open_dimension = *open_dim;
	cache_entry->open_dimension_attno = get_attnum(chunk_relid, NameStr(open_dim->fd.column_name));
	cache_entry->value_is_set = false;
	cache_entry->lowest_modified_value = INVAL_POS_INFINITY;
	cache_entry->greatest_modified_value = INVAL_NEG_INFINITY;
	ts_cache_release(&ht_cache);
}

static inline ContinuousAggsCacheInvalEntry *
get_cache_inval_entry(int32 hypertable_id, Oid chunk_relid)
{
	ContinuousAggsCacheInvalEntry *cache_entry;
	bool found;

	if (!continuous_aggs_cache_inval_htab)
	{
		cache_inval_init();
	}

	cache_entry = (ContinuousAggsCacheInvalEntry *)
		hash_search(continuous_aggs_cache_inval_htab, &chunk_relid, HASH_ENTER, &found);

	if (!found)
	{
		cache_inval_entry_init(cache_entry, hypertable_id, chunk_relid);
	}

	return cache_entry;
}

/*
 * Used by direct compress invalidation
 */
void
continuous_agg_invalidate_range(int32 hypertable_id, Oid chunk_relid, int64 start, int64 end)
{
	ContinuousAggsCacheInvalEntry *cache_entry = get_cache_inval_entry(hypertable_id, chunk_relid);

	cache_entry->value_is_set = true;
	Assert(start <= end);
	if (start < cache_entry->lowest_modified_value)
	{
		cache_entry->lowest_modified_value = start;
	}
	if (end > cache_entry->greatest_modified_value)
	{
		cache_entry->greatest_modified_value = end;
	}
}

void
continuous_agg_dml_invalidate(int32 hypertable_id, Relation chunk_rel, HeapTuple chunk_tuple,
							  HeapTuple chunk_newtuple, bool update)
{
	ContinuousAggsCacheInvalEntry *cache_entry =
		get_cache_inval_entry(hypertable_id, chunk_rel->rd_id);

	update_cache_from_tuple(cache_entry, chunk_tuple, RelationGetDescr(chunk_rel));

	if (!update)
	{
		return;
	}

	/* on update we need to invalidate the new time value as well as the old one */
	update_cache_from_tuple(cache_entry, chunk_newtuple, RelationGetDescr(chunk_rel));
}

static inline void
cache_inval_entry_write(ContinuousAggsCacheInvalEntry *entry)
{
	int64 liv;

	if (!entry->value_is_set)
	{
		return;
	}

	/* The materialization worker uses a READ COMMITTED isolation level by default. Therefore, if we
	 * use a stronger isolation level, the isolation threshold could update without us seeing the
	 * new value. In order to prevent serialization errors, we always append invalidation entries in
	 * the case when we're using a strong enough isolation level that we won't see the new
	 * threshold. The materializer can handle invalidations that are beyond the threshold
	 * gracefully.
	 */
	if (IsolationUsesXactSnapshot())
	{
		invalidation_hyper_log_add_entry(entry->hypertable_id,
										 entry->lowest_modified_value,
										 entry->greatest_modified_value);
		return;
	}

	liv = cache_get_lowest_invalidated_time_for_hypertable(entry->hypertable_id);

	if (entry->lowest_modified_value < liv)
	{
		invalidation_hyper_log_add_entry(entry->hypertable_id,
										 entry->lowest_modified_value,
										 entry->greatest_modified_value);
	}
};

static void
cache_inval_cleanup(void)
{
	Assert(continuous_aggs_cache_inval_htab != NULL);
	Assert(continuous_aggs_cache_hyper_inval_threshold_htab != NULL);
	hash_destroy(continuous_aggs_cache_inval_htab);
	hash_destroy(continuous_aggs_cache_hyper_inval_threshold_htab);
	MemoryContextDelete(continuous_aggs_invalidation_mctx);

	continuous_aggs_cache_inval_htab = NULL;
	continuous_aggs_cache_hyper_inval_threshold_htab = NULL;
	continuous_aggs_invalidation_mctx = NULL;
};

static void
cache_inval_htab_write(void)
{
	HASH_SEQ_STATUS hash_seq;
	ContinuousAggsCacheInvalEntry *current_entry;
	Catalog *catalog;

	if (hash_get_num_entries(continuous_aggs_cache_inval_htab) == 0)
	{
		return;
	}

	catalog = ts_catalog_get();

	/* The invalidation threshold must remain locked until the end of
	 * the transaction to ensure the materializer will see our updates,
	 * so we explicitly lock it here
	 */
	LockRelationOid(catalog_get_table_id(catalog, CONTINUOUS_AGGS_INVALIDATION_THRESHOLD),
					AccessShareLock);

	hash_seq_init(&hash_seq, continuous_aggs_cache_inval_htab);
	while ((current_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		cache_inval_entry_write(current_entry);
	}
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
	/* Return quickly if we never initialized either hashtable */
	if (!continuous_aggs_cache_inval_htab && !backfill_tracker_htab)
		return;
	}

	switch (event)
	{
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
			if (continuous_aggs_cache_inval_htab)
				cache_inval_htab_write();
			backfill_tracker_flush();
			break;
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			if (continuous_aggs_cache_inval_htab)
				cache_inval_cleanup();
			backfill_tracker_cleanup();
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
	{
		*((int64 *) min) = DatumGetInt64(watermark);
	}

	DEBUG_WAITPOINT("invalidation_tuple_found_done");

	/*
	 * Return SCAN_CONTINUE because we check for multiple tuples as an error
	 * condition.
	 */
	return SCAN_CONTINUE;
}

static int64
get_lowest_invalidated_time_for_hypertable(int32 hypertable_id)
{
	int64 min_val = INVAL_POS_INFINITY;
	Catalog *catalog = ts_catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx scanctx;

	PushActiveSnapshot(GetLatestSnapshot());
	ScanKeyInit(&scankey[0],
				Anum_continuous_aggs_invalidation_threshold_pkey_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(hypertable_id));
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

		/* We need to define a custom snapshot for this scan. The default snapshot (SNAPSHOT_SELF)
		   reads data of all committed transactions, even if they have started after our scan. If a
		   parallel session updates the scanned value and commits during a scan, we end up in a
		   situation where we see the old and the new value. This causes ts_scanner_scan_one() to
		   fail. */
		.snapshot = GetActiveSnapshot(),
	};

	/* If we don't find any invalidation threshold watermark, then we've never done any
	 * materialization we'll treat this as if the invalidation timestamp is at min value, since the
	 * first materialization needs to scan the entire table anyway; the invalidations are redundant.
	 */
	if (!ts_scanner_scan_one(&scanctx, false, CAGG_INVALIDATION_THRESHOLD_NAME))
	{
		min_val = INVAL_NEG_INFINITY;
	}
	PopActiveSnapshot();

	return min_val;
}

static inline int64
cache_get_lowest_invalidated_time_for_hypertable(int32 hypertable_id)
{
	ContinuousAggsCacheHyperInvalThresholdEntry *hyper_inval_cache_entry;
	bool found;

	hyper_inval_cache_entry = (ContinuousAggsCacheHyperInvalThresholdEntry *)
		hash_search(continuous_aggs_cache_hyper_inval_threshold_htab,
					&hypertable_id,
					HASH_ENTER,
					&found);
	if (!found)
	{
		hyper_inval_cache_entry->hypertable_id = hypertable_id;
		hyper_inval_cache_entry->watermark =
			get_lowest_invalidated_time_for_hypertable(hypertable_id);
	}

	return hyper_inval_cache_entry->watermark;
}

/*
 * Backfill Tracker
 *
 * Tracks which devices have inserted data into old chunks (below the low
 * watermark). During a transaction, device values and their modified time
 * ranges are accumulated in a hash table. At commit time, the entries are
 * flushed to the continuous_aggs_backfill_tracker catalog table.
 *
 * The refresh code then uses this information to only re-materialize data
 * for devices that actually backfilled, rather than refreshing the entire
 * time range.
 */

/*
 * Hash key for the backfill tracker: (hypertable_id, device_value_hash).
 * We use a uint32 hash of the device value as part of the key because device
 * values can be variable-length (text). The actual device value string is
 * stored separately in the entry for flushing to the catalog.
 */
typedef struct BackfillTrackerKey
{
	int32 hypertable_id;
	uint32 device_hash;
} BackfillTrackerKey;

typedef struct BackfillTrackerEntry
{
	BackfillTrackerKey key;
	char *device_value_str;		   /* text representation, palloc'd in backfill mctx */
	int64 lowest_modified_value;   /* min timestamp of backfill data for this device */
	int64 greatest_modified_value; /* max timestamp of backfill data for this device */
} BackfillTrackerEntry;

#define BACKFILL_TRACKER_INIT_HTAB_SIZE 64

static void
backfill_tracker_init(void)
{
	HASHCTL ctl;

	Assert(backfill_tracker_mctx == NULL);

	backfill_tracker_mctx =
		AllocSetContextCreate(TopTransactionContext, "BackfillTrackerCtx", ALLOCSET_DEFAULT_SIZES);

	/* Hash table for device entries keyed on (hypertable_id, device_hash) */
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(BackfillTrackerKey);
	ctl.entrysize = sizeof(BackfillTrackerEntry);
	ctl.hcxt = backfill_tracker_mctx;

	backfill_tracker_htab = hash_create("TS Backfill Tracker",
										BACKFILL_TRACKER_INIT_HTAB_SIZE,
										&ctl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	/*
	 * Cache current timestamp once per transaction for consistent watermark
	 * checks. Use ts_get_mock_time_or_current_time() so that tests can
	 * control the "current" time via timescaledb.current_timestamp_mock GUC.
	 */
#ifdef TS_DEBUG
	backfill_now_internal =
		ts_time_value_to_internal(ts_get_mock_time_or_current_time(), TIMESTAMPTZOID);
#else
	backfill_now_internal =
		ts_time_value_to_internal(TimestampTzGetDatum(GetCurrentTimestamp()), TIMESTAMPTZOID);
#endif
}

/*
 * Determine if a chunk is a "backfill" chunk by comparing its time range end
 * against a watermark derived from the chunk interval.
 *
 * Watermark = now - max(chunk_interval, 1 day)
 * If the chunk's range_end <= watermark, it's a backfill chunk.
 *
 * chunk_range_end is passed in from ChunkInsertState (set during chunk routing),
 * so no catalog lookup is needed here.
 *
 * TODO: The 1-day minimum threshold is a placeholder. Needs a more robust
 * solution — e.g., user-configurable, derived from cagg bucket width, or
 * based on N recent chunks. Also needs validation with all time dimension
 * datatypes (not just timestamptz).
 */
static bool
is_backfill_chunk(int64 chunk_range_end, const Hypertable *ht)
{
	if (!backfill_tracker_htab)
		backfill_tracker_init();

	const Dimension *open_dim = hyperspace_get_open_dimension(ht->space, 0);
	if (!open_dim)
		return false;

	int64 chunk_interval = open_dim->fd.interval_length;
	int64 one_day_usec = 86400LL * 1000000LL; /* USECS_PER_DAY */
	int64 threshold = Max(chunk_interval, one_day_usec);

	return (chunk_range_end <= (backfill_now_internal - threshold));
}

/*
 * Check if an insert is going into an old chunk (backfill) and record the
 * device value if so. Called from the insert path for each row.
 *
 * The tenant_column_name is looked up at executor init time (when a snapshot
 * is active) and passed in here. This avoids catalog scans in the per-row
 * hot path and avoids snapshot Push/Pop which would interfere with the
 * executor's snapshot lifecycle.
 *
 * TODO: Cache attno/type info per hypertable to avoid repeated syscache access.
 */
void
continuous_agg_backfill_check(int32 hypertable_id, int64 chunk_range_end, TupleTableSlot *slot,
							  const Hypertable *ht, const char *tenant_column_name)
{
	if (!is_backfill_chunk(chunk_range_end, ht))
		return;

	if (tenant_column_name == NULL)
		return;

	/* Get the tenant column attribute number from the hypertable relation.
	 * We use the hypertable's tuple descriptor since slot is in hypertable format. */
	AttrNumber device_attno = get_attnum(ht->main_table_relid, tenant_column_name);
	if (device_attno == InvalidAttrNumber)
		return; /* device column not found — skip tracking */

	/* Extract the device value from the tuple */
	bool isnull;
	Datum device_datum = slot_getattr(slot, device_attno, &isnull);
	if (isnull)
		return;

	/* Get the time dimension value for this row */
	const Dimension *open_dim = hyperspace_get_open_dimension(ht->space, 0);
	if (!open_dim)
		return;

	AttrNumber time_attno = get_attnum(ht->main_table_relid, NameStr(open_dim->fd.column_name));
	bool time_isnull;
	Datum time_datum = slot_getattr(slot, time_attno, &time_isnull);
	if (time_isnull)
		return;

	Oid time_type = ts_dimension_get_partition_type(open_dim);
	int64 timeval = ts_time_value_to_internal(time_datum, time_type);

	/* Convert device value to text for hashing and storage */
	Oid device_type = get_atttype(ht->main_table_relid, device_attno);
	Oid typoutput;
	bool typIsVarlena;
	getTypeOutputInfo(device_type, &typoutput, &typIsVarlena);
	char *device_str = OidOutputFunctionCall(typoutput, device_datum);

	/* Hash the device string for the hash table key */
	uint32 device_hash = DatumGetUInt32(hash_any((unsigned char *) device_str, strlen(device_str)));

	BackfillTrackerKey key = {
		.hypertable_id = hypertable_id,
		.device_hash = device_hash,
	};

	bool found;
	BackfillTrackerEntry *entry =
		(BackfillTrackerEntry *) hash_search(backfill_tracker_htab, &key, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry — store the device string in our memory context */
		MemoryContext oldctx = MemoryContextSwitchTo(backfill_tracker_mctx);
		entry->device_value_str = pstrdup(device_str);
		MemoryContextSwitchTo(oldctx);
		entry->lowest_modified_value = timeval;
		entry->greatest_modified_value = timeval;
	}
	else
	{
		/* Update min/max */
		if (timeval < entry->lowest_modified_value)
			entry->lowest_modified_value = timeval;
		if (timeval > entry->greatest_modified_value)
			entry->greatest_modified_value = timeval;
	}

	pfree(device_str);
}

/*
 * Flush all backfill tracker entries to the catalog table.
 * Called at PRE_COMMIT time.
 */
static void
backfill_tracker_flush(void)
{
	HASH_SEQ_STATUS hash_seq;
	BackfillTrackerEntry *entry;

	if (!backfill_tracker_htab || hash_get_num_entries(backfill_tracker_htab) == 0)
		return;

	Catalog *catalog = ts_catalog_get();
	Relation rel = table_open(catalog_get_table_id(catalog, CONTINUOUS_AGGS_BACKFILL_TRACKER),
							  RowExclusiveLock);
	TupleDesc tupdesc = RelationGetDescr(rel);
	CatalogSecurityContext sec_ctx;

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	hash_seq_init(&hash_seq, backfill_tracker_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum values[Natts_continuous_aggs_backfill_tracker];
		bool nulls[Natts_continuous_aggs_backfill_tracker] = { false };

		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_backfill_tracker_hypertable_id)] =
			Int32GetDatum(entry->key.hypertable_id);
		values[AttrNumberGetAttrOffset(Anum_continuous_aggs_backfill_tracker_device_value)] =
			CStringGetTextDatum(entry->device_value_str);
		values[AttrNumberGetAttrOffset(
			Anum_continuous_aggs_backfill_tracker_lowest_modified_value)] =
			Int64GetDatum(entry->lowest_modified_value);
		values[AttrNumberGetAttrOffset(
			Anum_continuous_aggs_backfill_tracker_greatest_modified_value)] =
			Int64GetDatum(entry->greatest_modified_value);

		ts_catalog_insert_values(rel, tupdesc, values, nulls);
	}

	ts_catalog_restore_user(&sec_ctx);
	table_close(rel, NoLock);

	elog(DEBUG1,
		 "backfill tracker: flushed %ld entries",
		 hash_get_num_entries(backfill_tracker_htab));
}

/*
 * Clean up backfill tracker state at end of transaction.
 */
static void
backfill_tracker_cleanup(void)
{
	if (!backfill_tracker_htab)
		return;

	hash_destroy(backfill_tracker_htab);
	MemoryContextDelete(backfill_tracker_mctx);

	backfill_tracker_htab = NULL;
	backfill_tracker_mctx = NULL;
	backfill_now_internal = 0;
}
