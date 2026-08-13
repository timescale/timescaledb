/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <catalog/pg_type.h>
#include <fmgr.h>
#include <miscadmin.h>
#include <storage/lwlock.h>
#include <utils/guc.h>
#include <utils/hsearch.h>
#include <utils/lsyscache.h>
#include <utils/snapmgr.h>
#include <utils/timestamp.h>

#include "compat/compat.h"

#include "continuous_aggs/insert.h"
#include "debug_point.h"
#include "guc.h"
#include "invalidation.h"
#include "partitioning.h"
#include "tenant_tracker.h"
#include "ts_catalog/hypertable_cagg_settings.h"

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
/* Tracking column info cached for each chunk. */
typedef struct TrackingColumnInfo
{
	AttrNumber attno;  /* tracking column attno in the chunk, or InvalidAttrNumber */
	Oid typid;		   /* tracking column type */
	Oid outfunc;	   /* type output function for the tenant key */
	bool typisvarlena; /* detoast the value before calling outfunc */
} TrackingColumnInfo;

typedef struct ContinuousAggsCacheInvalEntry
{
	Oid chunk_relid;
	int32 hypertable_id;
	Dimension hypertable_open_dimension;
	Oid open_dimension_type; /* partition type of the open dimension, cached */
	AttrNumber open_dimension_attno;
	TrackingColumnInfo tenant_col;
	bool value_is_set;
	int64 lowest_modified_value;
	int64 greatest_modified_value;
} ContinuousAggsCacheInvalEntry;

/*
 * Per-transaction local buffer of tenant invalidations.  Rows are aggregated
 * here during DML (no shared memory, no locks) and drained into the shared
 * tracker at commit, so a rolled-back transaction never publishes anything.
 *
 * The key is fixed-size and zero-padded so it can be used directly with
 * dynahash HASH_BLOBS; tenant keys longer than TENANT_TRACKER_KEY_MAXLEN are not buffered
 * (they set tenant_buffer_unencodable, which forces the tracker INVALID).
 */
typedef struct TenantLocalKey
{
	int32 hypertable_id; /* which hypertable's tracker this tenant belongs to */
	uint16 key_len;
	char key[TENANT_TRACKER_KEY_MAXLEN];
} TenantLocalKey;

typedef struct TenantLocalEntry
{
	TenantLocalKey key; /* must be first: dynahash key */
	int64 min_ts;
	int64 max_ts;
} TenantLocalEntry;

typedef struct HypertableSeqnumEntry
{
	int32 hypertable_id;
	int32 seqnum;
	int64 late_threshold_start;
	int64 late_threshold_end;
} HypertableSeqnumEntry;

typedef struct ContinuousAggsCacheHyperInvalThresholdEntry
{
	int32 hypertable_id;
	int64 watermark;
} ContinuousAggsCacheHyperInvalThresholdEntry;

/*
 * Backend-local cache of this backend's resolved handle to hypertable's
 * shared tracker.  Tracking entries are held in shared mem. Each backend
 * has to attach to that address and resolve the pointer. Once that is done
 * we cache it here for the lifetime of the backend. This is helpful for
 * the DML path and avoids any additional lookups.
 * tracking == NULL means DISABLED, tracking = non NULL means we have a valid
 * entry in shared mem.
 * Both outcomes are stable for the backend's life: a tracker is
 * never moved or freed and its mapping is pinned, and a DISABLED marker persists
 * until restart . So entries never need invalidation.  MyDatabaseId is fixed
 * per backend, so hypertable_id alone is a sufficient key.
 */
typedef struct TenantTrackerCacheEntry
{
	int32 hypertable_id;
	TenantTracking *tracking;
} TenantTrackerCacheEntry;

static int64 get_lowest_invalidated_time_for_hypertable(int32 hypertable_id);
static inline int64 cache_get_lowest_invalidated_time_for_hypertable(int32 hypertable_id);

#define CA_CACHE_INVAL_INIT_HTAB_SIZE 64

static HTAB *continuous_aggs_cache_inval_htab = NULL;
static HTAB *continuous_aggs_cache_hyper_inval_threshold_htab = NULL;

/* Per-transaction tenant buffer (drained to shared memory at commit). */
static HTAB *tenant_local_htab = NULL;
static bool tenant_buffer_unencodable = false;

/*
 * Generation this backend currently pins, or NULL.  A flush will not drain a
 * generation while its num_writers is non-zero, so a writer that fails between
 * begin_batch and end_batch would stall every later flush.  The abort handler
 * releases whatever this points at, which covers both an ERROR unwinding out of
 * the drain and a FATAL exit (ShutdownPostgres, called during process cleanup,
 * aborts the open transaction before shared memory is detached).
 */
static TenantGeneration *pinned_generation = NULL;

/* Backend-lifetime resolved-tracker cache (see TenantTrackerCacheEntry). */
static HTAB *tenant_tracker_resolved_htab = NULL;

static MemoryContext continuous_aggs_invalidation_mctx = NULL;

static inline void cache_inval_entry_init(ContinuousAggsCacheInvalEntry *cache_entry,
										  int32 hypertable_id, Oid chunk_relid);
static inline ContinuousAggsCacheInvalEntry *get_cache_inval_entry(int32 hypertable_id,
																   Oid chunk_relid);
static void cache_inval_cleanup(void);
static void cache_inval_htab_write(List *hypertable_seqnums);
static HTAB *get_tenant_local_htab(void);
static TenantTracking *resolve_tenant_tracker(int32 hypertable_id);
static List *tenant_local_htab_write(void);
static void continuous_agg_xact_invalidation_callback(XactEvent event, void *arg);
static ScanTupleResult invalidation_tuple_found(TupleInfo *ti, void *min);

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
	AttrNumber col = cache_entry->open_dimension_attno;

	Assert(cache_entry->hypertable_open_dimension.type == DIMENSION_TYPE_OPEN);

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

	int64 timeval = ts_time_value_to_internal(datum, cache_entry->open_dimension_type);

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
	cache_entry->open_dimension_type = ts_dimension_get_partition_type(open_dim);
	cache_entry->open_dimension_attno = get_attnum(chunk_relid, NameStr(open_dim->fd.column_name));

	/* Resolve the tracking column for this chunk.  attno == InvalidAttrNumber
	 * when tenant tracking isn't configured for this hypertable or this chunk
	 * lacks the column; otherwise cache the type facts used to encode the key. */
	MemSet(&cache_entry->tenant_col, 0, sizeof(cache_entry->tenant_col));
	const char *tracking_column_name;
	if (ts_hypertable_cagg_settings_get_tenant_tracking_column(hypertable_id,
															   &tracking_column_name))
	{
		cache_entry->tenant_col.attno = get_attnum(chunk_relid, tracking_column_name);
		if (cache_entry->tenant_col.attno != InvalidAttrNumber)
		{
			/* Resolve through domains so a domain over a supported type is accepted;
			 * domains share their base type's I/O and on-disk representation. */
			cache_entry->tenant_col.typid =
				getBaseType(get_atttype(chunk_relid, cache_entry->tenant_col.attno));
			Ensure(ts_tenant_type_is_supported(cache_entry->tenant_col.typid),
				   "tenant tracking column \"%s\" has an unsupported type",
				   tracking_column_name);
			getTypeOutputInfo(cache_entry->tenant_col.typid,
							  &cache_entry->tenant_col.outfunc,
							  &cache_entry->tenant_col.typisvarlena);
		}
	}
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
 *
 * tenants_unknown says the caller invalidated [start, end] without being able to
 * enumerate the tenants in it, so this transaction's tenant tracking is
 * incomplete.  Force the tracker INVALID at commit (tenant_local_htab_write),
 * which leaves seqnum 0 on the invalidation entries and makes the refresh fall
 * back to a full, non-tenant-scoped refresh of the range.
 *
 * Without that a mixed transaction silently corrupts the aggregate: any other
 * DML in it buffers a tenant for the hypertable, so cache_inval_entry_write
 * stamps a live seqnum on the untracked range too, and the refresh then scopes
 * that range to the tenants it happens to know about.
 */
void
continuous_agg_invalidate_range(int32 hypertable_id, Oid chunk_relid, int64 start, int64 end,
								bool tenants_unknown)
{
	ContinuousAggsCacheInvalEntry *cache_entry = get_cache_inval_entry(hypertable_id, chunk_relid);

	if (tenants_unknown && cache_entry->tenant_col.attno != InvalidAttrNumber)
	{
		tenant_buffer_unencodable = true;
	}

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

static HTAB *
get_tenant_local_htab(void)
{
	/* continuous_aggs_invalidation_mctx already exists: record_tenant_invalidation
	 * runs after get_cache_inval_entry, which initializes it. */
	if (tenant_local_htab == NULL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(TenantLocalKey);
		ctl.entrysize = sizeof(TenantLocalEntry);
		ctl.hcxt = continuous_aggs_invalidation_mctx;
		tenant_local_htab = hash_create("TS Continuous Aggs Tenant Local",
										CA_CACHE_INVAL_INIT_HTAB_SIZE,
										&ctl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	return tenant_local_htab;
}

/*
 * Resolve (and cache) this backend's handle to a hypertable's shared tracker.
 *
 * Called from the DML path
 *
 * Returns the tracker, or NULL when tracking is disabled for this hypertable
 * (negative-cache marker, loader absent, or a contained OOM).  Both outcomes are
 * cached and stable for the backend's life.
 */
static TenantTracking *
resolve_tenant_tracker(int32 hypertable_id)
{
	TenantTrackerCacheEntry *ce;
	TenantTracking *volatile tracking = NULL;
	MemoryContext oldcxt = CurrentMemoryContext;
	ResourceOwner oldowner = CurrentResourceOwner;

	if (tenant_tracker_resolved_htab == NULL)
	{
		HASHCTL ctl;

		memset(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(int32);
		ctl.entrysize = sizeof(TenantTrackerCacheEntry);
		ctl.hcxt = TopMemoryContext; /* backend lifetime: entries never expire */
		tenant_tracker_resolved_htab = hash_create("TS Tenant Tracker Resolved",
												   CA_CACHE_INVAL_INIT_HTAB_SIZE,
												   &ctl,
												   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	ce = hash_search(tenant_tracker_resolved_htab, &hypertable_id, HASH_FIND, NULL);
	if (ce != NULL)
	{
		return ce->tracking; /* cached FOUND (ptr) or DISABLED (NULL) */
	}

	/*
	 * First time this backend touches this hypertable.  Park here so the
	 * first-touch race isolation test can line up two backends before the
	 * get_or_attach below (which serializes on the dshash partition lock).
	 */
	DEBUG_WAITPOINT("tenant_tracker_drain_before_attach");

	/*
	 * Contain the resolution in an internal subtransaction.  get_or_attach can
	 * throw OOM with a dshash partition lock held, and the seqnum-seed catalog
	 * scan opens relations / pins buffers; rolling the subtransaction back
	 * releases all resources cleanly
	 * The cached attach (TopMemoryContext + dsa_pin_mapping) survives the rollback.
	 */
	PG_TRY();
	{
		TenantLookupState state;

		BeginInternalSubTransaction(NULL);

		tracking = ts_tenant_tracker_lookup_wstate(hypertable_id, &state);

		if (state == TENANT_TRACKER_LOOKUP_ABSENT)
		{
			int64 seed_start, seed_end;
			int32 max_seqnum;

			/* Window to seed a brand-new tracker; ignored if a racer created it
			 * first (get_or_attach then returns the existing one). */
			ts_hypertable_cagg_settings_get_tenant_tracking_window(hypertable_id,
																   &seed_start,
																   &seed_end);
			/* Seed to max_seqnum + 1 so the first generation created has a seqnum
			 * above any pre-restart one. */
			max_seqnum = invalidation_max_seqnum_for_hypertable(hypertable_id);

			tracking = ts_tenant_tracker_get_or_attach(hypertable_id,
													   seed_start,
													   seed_end,
													   max_seqnum + 1);
		}

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;
	}
	PG_CATCH();
	{
		ErrorData *edata;

		/* CopyErrorData() must run outside ErrorContext and before FlushErrorState() */
		MemoryContextSwitchTo(oldcxt);
		edata = CopyErrorData();
		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;
		FlushErrorState();
		tracking = NULL; /* tracking disabled for server's lifetime */

		ereport(LOG,
				(errmsg("per-tenant invalidation tracker unavailable for hypertable %d: %s",
						hypertable_id,
						edata->message),
				 errdetail("Disabling per-tenant tracking for this hypertable; the continuous "
						   "aggregate refresh will fall back to the full invalidation log.")));
		ereport(NOTICE,
				(errmsg("per-tenant invalidation tracker unavailable for hypertable %d, "
						"falling back to full invalidation log",
						hypertable_id)));
		FreeErrorData(edata);
	}
	PG_END_TRY();

	ce = hash_search(tenant_tracker_resolved_htab, &hypertable_id, HASH_ENTER, NULL);
	ce->tracking = tracking;
	return tracking;
}

/*
 * Buffer one (tenant, time) pair into the per-transaction local tenant buffer.
 * Used by DML path (record_tenant_invalidation) and the direct-compress path
 * (continuous_agg_record_tenant_from_slot).
 */
static void
record_tenant_invalidation_values(const ContinuousAggsCacheInvalEntry *cache_entry, int64 timeval,
								  Datum tenant_datum)
{
	char *key;
	int key_len;
	TenantLocalKey lookup;
	TenantLocalEntry *entry;
	bool found;

	/* No tenant column -> nothing to record. */
	if (cache_entry->tenant_col.attno == InvalidAttrNumber)
	{
		return;
	}

	/*
	 * Key the tenant by the text form produced by its type's output function,
	 * so it round-trips through the decode in build_tenant_predicate
	 * (tenant_id::<coltype>).  The output function, varlena flag and type are
	 * cached per chunk (cache_entry->tenant_col).  Varlena values must be
	 * detoasted before the output function is called.
	 *
	 * DATE is the only supported type whose text form is GUC-dependent
	 * (DateStyle); pin it to a canonical ISO form while formatting so the stored
	 * text re-parses identically at refresh regardless of the refreshing
	 * session's DateStyle.
	 */
	if (cache_entry->tenant_col.typisvarlena)
	{
		tenant_datum = PointerGetDatum(
			pg_detoast_datum_packed((struct varlena *) DatumGetPointer(tenant_datum)));
	}
	if (cache_entry->tenant_col.typid == DATEOID && DateStyle != USE_ISO_DATES)
	{
		/* date_out isn't ISO under this session's DateStyle; pin it so the stored
		 * text is canonical.  Skipped when already ISO (the common case). */
		int save_nestlevel = NewGUCNestLevel();

		(void) set_config_option("datestyle",
								 "ISO, YMD",
								 PGC_USERSET,
								 PGC_S_SESSION,
								 GUC_ACTION_SAVE,
								 true,
								 0,
								 false);
		key = OidOutputFunctionCall(cache_entry->tenant_col.outfunc, tenant_datum);
		AtEOXact_GUC(false, save_nestlevel);
	}
	else
	{
		key = OidOutputFunctionCall(cache_entry->tenant_col.outfunc, tenant_datum);
	}
	key_len = (int) strlen(key);

	if (key_len == 0 || key_len > TENANT_TRACKER_KEY_MAXLEN)
	{
		/* Cannot key this tenant -> tracking is incomplete for this transaction;
		 * force the tracker INVALID at commit so the refresh falls back. */
		pfree(key);
		tenant_buffer_unencodable = true;
		return;
	}

	/* Zero the whole key struct so the pad bytes are stable for hashing. */
	memset(&lookup, 0, sizeof(lookup));
	lookup.hypertable_id = cache_entry->hypertable_id;
	lookup.key_len = (uint16) key_len;
	memcpy(lookup.key, key, key_len);
	// TODO: try to see if there's a way to avoid allocate and free memory for every key.
	pfree(key);

	resolve_tenant_tracker(cache_entry->hypertable_id);

	entry = (TenantLocalEntry *) hash_search(get_tenant_local_htab(), &lookup, HASH_ENTER, &found);
	if (!found)
	{
		entry->min_ts = timeval;
		entry->max_ts = timeval;
	}
	else
	{
		if (timeval < entry->min_ts)
		{
			entry->min_ts = timeval;
		}
		if (timeval > entry->max_ts)
		{
			entry->max_ts = timeval;
		}
	}
}

/*
 * DML path: extract the tenant and time values from a chunk heap tuple and
 * buffer them for tenant-level invalidation tracking.
 */
static void
record_tenant_invalidation(const ContinuousAggsCacheInvalEntry *cache_entry, Relation chunk_rel,
						   HeapTuple tuple)
{
	TupleDesc tupdesc = RelationGetDescr(chunk_rel);
	Datum time_datum;
	Datum tenant_datum;
	bool isnull;
	int64 timeval;

	/* No tenant column -> nothing to record. */
	if (cache_entry->tenant_col.attno == InvalidAttrNumber)
	{
		return;
	}

	time_datum = heap_getattr(tuple, cache_entry->open_dimension_attno, tupdesc, &isnull);
	if (isnull)
	{
		return;
	}
	timeval = ts_time_value_to_internal(time_datum, cache_entry->open_dimension_type);

	tenant_datum = heap_getattr(tuple, cache_entry->tenant_col.attno, tupdesc, &isnull);
	if (isnull)
	{
		/* A NULL tenant is a valid group but cannot be keyed -> tracking is
		 * incomplete for this transaction; force the tracker INVALID at commit
		 * so the refresh falls back. */
		tenant_buffer_unencodable = true;
		return;
	}

	record_tenant_invalidation_values(cache_entry, timeval, tenant_datum);
}

/*
 * Direct-compress path: extract the tenant and time values from an uncompressed
 * chunk-layout slot (available before the row is folded into a compressed batch)
 * and buffer them for tenant-level invalidation tracking.  Called once per input
 * row from tsl_compressor_add_slot.
 */
void
continuous_agg_record_tenant_from_slot(int32 hypertable_id, Oid chunk_relid, TupleTableSlot *slot)
{
	ContinuousAggsCacheInvalEntry *cache_entry;
	Datum time_datum;
	Datum tenant_datum;
	bool isnull;
	int64 timeval;

	cache_entry = get_cache_inval_entry(hypertable_id, chunk_relid);
	if (cache_entry->tenant_col.attno == InvalidAttrNumber)
	{
		return;
	}

	time_datum = slot_getattr(slot, cache_entry->open_dimension_attno, &isnull);
	if (isnull)
	{
		return;
	}
	timeval = ts_time_value_to_internal(time_datum, cache_entry->open_dimension_type);

	tenant_datum = slot_getattr(slot, cache_entry->tenant_col.attno, &isnull);
	if (isnull)
	{
		/* A NULL tenant is a valid group but cannot be keyed -> tracking is
		 * incomplete for this transaction; force the tracker INVALID at commit
		 * so the refresh falls back. */
		tenant_buffer_unencodable = true;
		return;
	}

	record_tenant_invalidation_values(cache_entry, timeval, tenant_datum);
}

/*
 * Drain the per-transaction tenant buffer into the shared tracker at commit.
 * Runs from the xact pre-commit callback, so an aborted transaction never gets
 * here and publishes nothing.
 */
static List *
tenant_local_htab_write(void)
{
	HASH_SEQ_STATUS hash_seq;
	TenantLocalEntry *entry;
	List *hypertable_ids = NIL;
	List *hypertable_seqnums = NIL;
	ListCell *lc;

	if (tenant_local_htab == NULL)
	{
		return NIL;
	}

	/* Distinct hypertables present in the buffer (usually just one). */
	hash_seq_init(&hash_seq, tenant_local_htab);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hypertable_ids = list_append_unique_int(hypertable_ids, entry->key.hypertable_id);
	}

	/* Drain each hypertable's tenants into its own tracker (one generation pin
	 * per hypertable). */
	foreach (lc, hypertable_ids)
	{
		int32 hypertable_id = lfirst_int(lc);
		TenantTracking *tracking;
		TenantGeneration *generation;
		int32 seqnum = 0;
		HypertableSeqnumEntry *seq_entry;
		int64 window_start, window_end;
		TenantTrackerCacheEntry *ce;

		/*
		 * Record this hypertable's seqnum entry up front, defaulting to 0
		 * (untracked -> refresh falls back to the full log) with an empty window.
		 * The tracked branch below overwrites both once the generation is pinned.
		 */
		seq_entry = palloc(sizeof(*seq_entry));
		seq_entry->hypertable_id = hypertable_id;
		seq_entry->seqnum = 0;
		seq_entry->late_threshold_start = PG_INT64_MAX;
		seq_entry->late_threshold_end = PG_INT64_MIN;
		hypertable_seqnums = lappend(hypertable_seqnums, seq_entry);

		/*
		 * The tracker was resolved (and cached) in the DML path by
		 * resolve_tenant_tracker, so the drain only reads the backend-local cache
		 * here -- no attach, lookup, allocation or catalog scan.  Errors are still
		 * possible (a debug build injects one below, and a cancel can arrive), so
		 * the pin taken further down is released by the abort handler rather than
		 * relying on this staying throw-free.  A NULL entry means tracking is
		 * disabled for this hypertable; a missing entry means nothing was buffered
		 * for it (shouldn't happen), both -> skip (untracked, seqnum 0).
		 */
		ce = (tenant_tracker_resolved_htab != NULL) ?
				 hash_search(tenant_tracker_resolved_htab, &hypertable_id, HASH_FIND, NULL) :
				 NULL;
		tracking = (ce != NULL) ? ce->tracking : NULL;

		if (tracking == NULL)
		{
			continue;
		}

		if (tenant_buffer_unencodable)
		{
			/* A tenant key could not be stored this transaction; force a
			 * fall back for this hypertable's tracker (seqnum 0). */
			ts_tenant_tracker_mark_invalid(tracking);
			continue;
		}

		/* Pin the generation and read its authoritative late-arrival window in one
		 * step (set by the last flush, or the seed above for a new tracker).
		 * Reading it under the pin means every backend draining into this
		 * generation gates on the same [window_start, window_end). */
		generation = ts_tenant_tracker_begin_batch(tracking, &seqnum, &window_start, &window_end);
		pinned_generation = generation;
		seq_entry->seqnum = seqnum;
		seq_entry->late_threshold_start = window_start;
		seq_entry->late_threshold_end = window_end;

		/* Fail while this generation is pinned, so a test can exercise what
		 * happens to num_writers when a writer does not reach end_batch. */
		DEBUG_ERROR_INJECTION("tenant_tracker_fail_in_pin");

		/* Park while the generation is pinned, so a test can terminate the
		 * writer before it reaches end_batch. */
		DEBUG_WAITPOINT("tenant_tracker_in_pin");

		hash_seq_init(&hash_seq, tenant_local_htab);
		while ((entry = hash_seq_search(&hash_seq)) != NULL)
		{
			if (entry->key.hypertable_id != hypertable_id)
			{
				continue;
			}
			/* Track the tenant only if its modified range overlaps the epoch
			 * late-arrival window (half-open [window_start, window_end)). */
			if (entry->min_ts >= window_end || entry->max_ts < window_start)
			{
				continue;
			}
			if (!ts_tenant_tracker_apply_one(generation,
											 entry->key.key,
											 entry->key.key_len,
											 entry->min_ts,
											 entry->max_ts))
			{
				/* Generation went INVALID; stop draining and release the scan. */
				hash_seq_term(&hash_seq);
				break;
			}
		}
		ts_tenant_tracker_end_batch(generation);
		pinned_generation = NULL;
	}

	list_free(hypertable_ids);
	return hypertable_seqnums;
}

void
continuous_agg_dml_invalidate(int32 hypertable_id, Relation chunk_rel, HeapTuple chunk_tuple,
							  HeapTuple chunk_newtuple, bool update)
{
	ContinuousAggsCacheInvalEntry *cache_entry =
		get_cache_inval_entry(hypertable_id, chunk_rel->rd_id);

	update_cache_from_tuple(cache_entry, chunk_tuple, RelationGetDescr(chunk_rel));
	record_tenant_invalidation(cache_entry, chunk_rel, chunk_tuple);

	if (update)
	{
		/* on update we need to invalidate the new value as well as the old one */
		update_cache_from_tuple(cache_entry, chunk_newtuple, RelationGetDescr(chunk_rel));
		record_tenant_invalidation(cache_entry, chunk_rel, chunk_newtuple);
	}
}

/*
 * Look up the seqnum and late-arrival window this hypertable's tenants were
 * drained under this transaction.  Returns NULL if nothing was buffered for it;
 * a found entry carries seqnum 0 when the hypertable ended up untracked (no
 * tracker available or buffer forced INVALID).
 */
static const HypertableSeqnumEntry *
tenant_tracking_for_hypertable(List *hypertable_seqnums, int32 hypertable_id)
{
	ListCell *lc;

	foreach (lc, hypertable_seqnums)
	{
		HypertableSeqnumEntry *seq_entry = (HypertableSeqnumEntry *) lfirst(lc);

		if (seq_entry->hypertable_id == hypertable_id)
		{
			return seq_entry;
		}
	}
	return NULL;
}

static inline void
cache_inval_entry_write(ContinuousAggsCacheInvalEntry *entry, List *hypertable_seqnums)
{
	int64 liv;
	int32 seqnum;
	const HypertableSeqnumEntry *tracking_entry;

	if (!entry->value_is_set)
	{
		return;
	}

	tracking_entry = tenant_tracking_for_hypertable(hypertable_seqnums, entry->hypertable_id);
	seqnum = (tracking_entry != NULL) ? tracking_entry->seqnum : 0;

	/* This invalidation entry is disjoint from the late-arrival window, so it does
	 * not have any tracking entries. Set seqnum as invalid i.e. 0. */
	if (seqnum != 0 && (entry->greatest_modified_value < tracking_entry->late_threshold_start ||
						entry->lowest_modified_value >= tracking_entry->late_threshold_end))
	{
		seqnum = 0;
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
										 entry->greatest_modified_value,
										 seqnum);
		return;
	}

	liv = cache_get_lowest_invalidated_time_for_hypertable(entry->hypertable_id);

	if (entry->lowest_modified_value < liv)
	{
		invalidation_hyper_log_add_entry(entry->hypertable_id,
										 entry->lowest_modified_value,
										 entry->greatest_modified_value,
										 seqnum);
	}
};

static void
cache_inval_cleanup(void)
{
	Assert(continuous_aggs_cache_inval_htab != NULL);
	Assert(continuous_aggs_cache_hyper_inval_threshold_htab != NULL);
	hash_destroy(continuous_aggs_cache_inval_htab);
	hash_destroy(continuous_aggs_cache_hyper_inval_threshold_htab);
	if (tenant_local_htab != NULL)
	{
		hash_destroy(tenant_local_htab);
	}
	MemoryContextDelete(continuous_aggs_invalidation_mctx);

	continuous_aggs_cache_inval_htab = NULL;
	continuous_aggs_cache_hyper_inval_threshold_htab = NULL;
	tenant_local_htab = NULL;
	tenant_buffer_unencodable = false;
	continuous_aggs_invalidation_mctx = NULL;
};

static void
cache_inval_htab_write(List *hypertable_seqnums)
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
		cache_inval_entry_write(current_entry, hypertable_seqnums);
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
	/* Return quickly if we never initialize the hashtable */
	if (!continuous_aggs_cache_inval_htab)
	{
		return;
	}

	switch (event)
	{
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		{
			/*
			 * Drain the per-tenant buffer first: it pins each hypertable's
			 * tracker generation and returns the <hypertable_id, seqnum> the
			 * tenants were written under.  We then stamp this transaction's
			 * invalidation-log entries with that seqnum so a refresh can
			 * correlate an invalidation entry with the tenant-tracking rows for
			 * the same generation (seqnum 0 == untracked -> full-log fallback).
			 */
			List *hypertable_seqnums = tenant_local_htab_write();

			cache_inval_htab_write(hypertable_seqnums);
			list_free_deep(hypertable_seqnums);
			/*
			 * Isolation-test anchor.  At this point the per-tenant info has
			 * been published to shared memory (tenant_local_htab_write released
			 * its generation pin), but this transaction has NOT yet written its
			 * commit record -- so its invalidation-log entry is not yet visible
			 * to other backends.  A concurrent refresh's flush can drain these
			 * tenant info  here, exercising the window where a seqnum's tenant
			 * tracker is persisted (+ maybe consumed) before the corresponding
			 * invalidation-log entry ever becomes visible.
			 */
			DEBUG_WAITPOINT("tenant_tracker_after_precommit_drain");
			break;
		}
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			/* A writer that did not reach end_batch still holds its generation
			 * pinned; release it before the backend goes away*/
			if (pinned_generation != NULL)
			{
				ts_tenant_tracker_end_batch(pinned_generation);
				pinned_generation = NULL;
			}
			cache_inval_cleanup();
			break;
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
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
