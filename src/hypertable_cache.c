
#include <postgres.h>
#include <utils/catcache.h>

#include "hypertable_cache.h"
#include "cache.h"
#include "metadata_queries.h"
#include "utils.h"

static void hypertable_cache_pre_invalidate(Cache *cache);
static void *hypertable_cache_create_entry(Cache *cache, CacheQueryCtx *ctx);

typedef struct HypertableCacheQueryCtx
{
	CacheQueryCtx cctx;
	int32		hypertable_id;
} HypertableCacheQueryCtx;

static void *
hypertable_cache_get_key(CacheQueryCtx *ctx)
{
	return &((HypertableCacheQueryCtx *) ctx)->hypertable_id;
}

static Cache hypertable_cache = {
	.hctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(hypertable_cache_entry),
		.hcxt = NULL,
	},
	.htab = NULL,
	.name = HYPERTABLE_CACHE_INVAL_PROXY_TABLE,
	.numelements = 16,
	.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
	.get_key = hypertable_cache_get_key,
	.create_entry = hypertable_cache_create_entry,
	.pre_invalidate_hook = hypertable_cache_pre_invalidate,
	.post_invalidate_hook = cache_init,
};

static void
hypertable_cache_pre_invalidate(Cache *cache)
{
	hypertable_cache_entry *entry;
	HASH_SEQ_STATUS scan;

	hash_seq_init(&scan, cache->htab);

	while ((entry = hash_seq_search(&scan)))
	{
		SPI_freeplan(entry->info->get_one_tuple_copyt_plan);
	}
}

static void *
hypertable_cache_create_entry(Cache *cache, CacheQueryCtx *ctx)
{
	HypertableCacheQueryCtx *hctx = (HypertableCacheQueryCtx *) ctx;
	hypertable_cache_entry *he = ctx->entry;

	he->info = fetch_hypertable_info(NULL, hctx->hypertable_id);
	he->num_epochs = 0;

	return he;
}

void
invalidate_hypertable_cache_callback(void)
{
	CACHE1_elog(WARNING, "DESTROY hypertable_cache");
	cache_invalidate(&hypertable_cache);
}


/* Get hypertable cache entry. If the entry is not in the cache, add it. */
hypertable_cache_entry *
get_hypertable_cache_entry(int32 hypertable_id)
{
	HypertableCacheQueryCtx ctx = {
		.hypertable_id = hypertable_id,
	};

	return cache_fetch(&hypertable_cache, &ctx.cctx);
}

/* function to compare epochs */
static int
cmp_epochs(const void *time_pt_pointer, const void *test)
{
	/* note reverse order; assume oldest stuff last */
	int64	   *time_pt = (int64 *) time_pt_pointer;
	epoch_and_partitions_set **entry = (epoch_and_partitions_set **) test;

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

epoch_and_partitions_set *
get_partition_epoch_cache_entry(hypertable_cache_entry *hce, int64 time_pt, Oid relid)
{
	MemoryContext old;
	epoch_and_partitions_set *entry,
			  **cache_entry;
	int			j;

	/* fastpath: check latest entry */
	if (hce->num_epochs > 0)
	{
		entry = hce->epochs[0];

		if (entry->start_time <= time_pt && entry->end_time >= time_pt)
		{
			return entry;
		}
	}

	cache_entry = bsearch(&time_pt, hce->epochs, hce->num_epochs,
						  sizeof(epoch_and_partitions_set *), cmp_epochs);

	if (cache_entry != NULL)
	{
		return (*cache_entry);
	}

	old = cache_switch_to_memory_context(&hypertable_cache);
	entry = fetch_epoch_and_partitions_set(NULL, hce->info->id, time_pt, relid);

	/* check if full */
	if (hce->num_epochs == MAX_EPOCHS_PER_HYPERTABLE_CACHE_ENTRY)
	{
		/* remove last */
		free_epoch(hce->epochs[MAX_EPOCHS_PER_HYPERTABLE_CACHE_ENTRY - 1]);
		hce->epochs[MAX_EPOCHS_PER_HYPERTABLE_CACHE_ENTRY - 1] = NULL;
		hce->num_epochs--;
	}

	/* ordered insert */
	for (j = hce->num_epochs - 1; j >= 0 && cmp_epochs(&time_pt, hce->epochs + j) < 0; j--)
	{
		hce->epochs[j + 1] = hce->epochs[j];
	}
	hce->epochs[j + 1] = entry;
	hce->num_epochs++;

	MemoryContextSwitchTo(old);

	return entry;
}

/* function to compare partitions */
static int
cmp_partitions(const void *keyspace_pt_arg, const void *test)
{
	/* note in keyspace asc; assume oldest stuff last */
	int64		keyspace_pt = *((int16 *) keyspace_pt_arg);
	partition_info *part = *((partition_info **) test);

	if (part->keyspace_start <= keyspace_pt && part->keyspace_end >= keyspace_pt)
	{
		return 0;
	}

	if (keyspace_pt > part->keyspace_end)
	{
		return 1;
	}
	return -1;
}


partition_info *
get_partition_info(epoch_and_partitions_set *epoch, int16 keyspace_pt)
{
	partition_info **part;

	if (keyspace_pt < 0)
	{
		if (epoch->num_partitions > 1)
		{
			elog(ERROR, "Found many partitions(%d) for an unpartitioned epoch",
				 epoch->num_partitions);
		}
		return epoch->partitions[0];
	}

	part = bsearch(&keyspace_pt, epoch->partitions, epoch->num_partitions,
				   sizeof(partition_info *), cmp_partitions);

	if (part == NULL)
	{
		elog(ERROR, "could not find partition for epoch");
	}

	return *part;
}

void
_hypertable_cache_init(void)
{
	CreateCacheMemoryContext();
	cache_init(&hypertable_cache);
}

void
_hypertable_cache_fini(void)
{
	hypertable_cache.post_invalidate_hook = NULL;
	cache_invalidate(&hypertable_cache);
}
