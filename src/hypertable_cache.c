#include <postgres.h>
#include <access/relscan.h>
#include <utils/catcache.h>
#include <utils/rel.h>
#include <utils/fmgroids.h>
#include <utils/tqual.h>
#include <utils/acl.h>

#include "hypertable_cache.h"
#include "catalog.h"
#include "cache.h"
#include "metadata_queries.h"
#include "utils.h"
#include "scanner.h"
#include "partitioning.h"

static void *hypertable_cache_create_entry(Cache * cache, CacheQueryCtx * ctx);

typedef struct HypertableCacheQueryCtx
{
	CacheQueryCtx cctx;
	int32		hypertable_id;
}	HypertableCacheQueryCtx;

static void *
hypertable_cache_get_key(CacheQueryCtx * ctx)
{
	return &((HypertableCacheQueryCtx *) ctx)->hypertable_id;
}


static Cache *
hypertable_cache_create()
{
	MemoryContext ctx = AllocSetContextCreate(CacheMemoryContext,
										  HYPERTABLE_CACHE_INVAL_PROXY_TABLE,
											  ALLOCSET_DEFAULT_SIZES);

	Cache	   *cache = MemoryContextAlloc(ctx, sizeof(Cache));

	Cache		tmp = (Cache)
	{
		.hctl =
		{
			.keysize = sizeof(int32),
			.entrysize = sizeof(Hypertable),
			.hcxt = ctx,
		},
		.name = HYPERTABLE_CACHE_INVAL_PROXY_TABLE,
		.numelements = 16,
		.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
		.get_key = hypertable_cache_get_key,
		.create_entry = hypertable_cache_create_entry,
	};

	*cache = tmp;
	cache_init(cache);

	return cache;
}


static Cache *hypertable_cache_current = NULL;

/* Column numbers for 'hypertable' table in  sql/common/tables.sql */

static bool
hypertable_tuple_found(TupleInfo * ti, void *data)
{
	HypertableCacheQueryCtx *hctx = data;
	Hypertable *he = hctx->cctx.entry;
	Datum		values[Natts_hypertable];
	bool		isnull[Natts_hypertable];
	int32		id;

	heap_deform_tuple(ti->tuple, ti->desc, values, isnull);

	id = DatumGetInt32(DATUM_GET(values, Anum_hypertable_id));

	if (id != hctx->hypertable_id)
	{
		elog(ERROR, "Expected hypertable ID %u, got %u", hctx->hypertable_id, id);
	}

	he->num_epochs = 0;
	he->id = hctx->hypertable_id;
	strncpy(he->time_column_name,
		DatumGetCString(DATUM_GET(values, Anum_hypertable_time_column_name)),
			NAMEDATALEN);
	he->time_column_type = DatumGetObjectId(DATUM_GET(values, Anum_hypertable_time_column_type));
	he->chunk_size_bytes = DatumGetInt64(DATUM_GET(values, Anum_hypertable_chunk_size_bytes));
	he->num_replicas = DatumGetInt16(DATUM_GET(values, Anum_hypertable_replication_factor));

	return false;
}

static void *
hypertable_cache_create_entry(Cache * cache, CacheQueryCtx * ctx)
{
	HypertableCacheQueryCtx *hctx = (HypertableCacheQueryCtx *) ctx;
	Catalog    *catalog = catalog_get();
	ScanKeyData scankey[1];
	ScannerCtx	scanCtx = {
		.table = catalog->tables[HYPERTABLE].id,
		.index = catalog->tables[HYPERTABLE].index_ids[HYPERTABLE_ID_INDEX],
		.scantype = ScannerTypeIndex,
		.nkeys = 1,
		.scankey = scankey,
		.data = ctx,
		.tuple_found = hypertable_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	/* Perform an index scan on primary key. */
	ScanKeyInit(&scankey[0], Anum_hypertable_pkey_idx_id, BTEqualStrategyNumber,
				F_INT4EQ, Int32GetDatum(hctx->hypertable_id));

	scanner_scan(&scanCtx);

	return ctx->entry;
}

void
hypertable_cache_invalidate_callback(void)
{
	CACHE1_elog(WARNING, "DESTROY hypertable_cache");
	cache_invalidate(hypertable_cache_current);
	hypertable_cache_current = hypertable_cache_create();
}


/* Get hypertable cache entry. If the entry is not in the cache, add it. */
Hypertable *
hypertable_cache_get_entry(Cache * cache, int32 hypertable_id)
{
	HypertableCacheQueryCtx ctx = {
		.hypertable_id = hypertable_id,
	};

	return cache_fetch(cache, &ctx.cctx);
}

/* function to compare epochs */
static int
cmp_epochs(const void *time_pt_pointer, const void *test)
{
	/* note reverse order; assume oldest stuff last */
	int64	   *time_pt = (int64 *) time_pt_pointer;
	PartitionEpoch **entry = (PartitionEpoch **) test;

	if ((*entry)->start_time <= *time_pt && (*entry)->end_time >= *time_pt)
	{
		return 0;
	}

	if (*time_pt < (*entry)->start_time)
	{
		return 1;
	}
	return -1;
}

PartitionEpoch *
hypertable_cache_get_partition_epoch(Cache * cache, Hypertable * hce, int64 time_pt, Oid relid)
{
	MemoryContext old;
	PartitionEpoch *epoch,
			  **cache_entry;
	int			j;

	/* fastpath: check latest entry */
	if (hce->num_epochs > 0)
	{
		epoch = hce->epochs[0];

		if (epoch->start_time <= time_pt && epoch->end_time >= time_pt)
		{
			return epoch;
		}
	}

	cache_entry = bsearch(&time_pt, hce->epochs, hce->num_epochs,
						  sizeof(PartitionEpoch *), cmp_epochs);

	if (cache_entry != NULL)
	{
		return (*cache_entry);
	}

	old = cache_switch_to_memory_context(cache);
	epoch = partition_epoch_scan(hce->id, time_pt, relid);

	/* check if full */
	if (hce->num_epochs == MAX_EPOCHS_PER_HYPERTABLE)
	{
		/* remove last */
		partition_epoch_free(hce->epochs[MAX_EPOCHS_PER_HYPERTABLE - 1]);
		hce->epochs[MAX_EPOCHS_PER_HYPERTABLE - 1] = NULL;
		hce->num_epochs--;
	}

	/* ordered insert */
	for (j = hce->num_epochs - 1; j >= 0 && cmp_epochs(&time_pt, hce->epochs + j) < 0; j--)
	{
		hce->epochs[j + 1] = hce->epochs[j];
	}
	hce->epochs[j + 1] = epoch;
	hce->num_epochs++;

	MemoryContextSwitchTo(old);

	return epoch;
}

extern Cache *
hypertable_cache_pin()
{
	return cache_pin(hypertable_cache_current);
}


void
_hypertable_cache_init(void)
{
	CreateCacheMemoryContext();
	hypertable_cache_current = hypertable_cache_create();
}

void
_hypertable_cache_fini(void)
{
	cache_invalidate(hypertable_cache_current);
}
