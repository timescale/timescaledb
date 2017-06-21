#include <postgres.h>
#include <access/relscan.h>
#include <catalog/namespace.h>
#include <utils/catcache.h>
#include <utils/rel.h>
#include <utils/fmgroids.h>
#include <utils/tqual.h>
#include <utils/acl.h>
#include <utils/lsyscache.h>

#include "hypertable_cache.h"
#include "hypertable.h"
#include "catalog.h"
#include "cache.h"
#include "utils.h"
#include "scanner.h"
#include "dimension.h"

static void *hypertable_cache_create_entry(Cache *cache, CacheQuery *query);

typedef struct HypertableCacheQuery
{
	CacheQuery	q;
	Oid			relid;
	const char *schema;
	const char *table;
} HypertableCacheQuery;

static void *
hypertable_cache_get_key(CacheQuery *query)
{
	return &((HypertableCacheQuery *) query)->relid;
}

typedef struct
{
	Oid			relid;
	Hypertable *hypertable;
} HypertableNameCacheEntry;


static Cache *
hypertable_cache_create()
{
	MemoryContext ctx = AllocSetContextCreate(CacheMemoryContext,
						 catalog_get_cache_proxy_name(CACHE_TYPE_HYPERTABLE),
											  ALLOCSET_DEFAULT_SIZES);

	Cache	   *cache = MemoryContextAlloc(ctx, sizeof(Cache));
	Cache		template =
	{
		.hctl =
		{
			.keysize = sizeof(Oid),
			.entrysize = sizeof(HypertableNameCacheEntry),
			.hcxt = ctx,
		},
		.name = "hypertable_cache",
		.numelements = 16,
		.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
		.get_key = hypertable_cache_get_key,
		.create_entry = hypertable_cache_create_entry,
	};
	
	*cache = template;

	cache_init(cache);

	return cache;
}


static Cache *hypertable_cache_current = NULL;

/* Column numbers for 'hypertable' table in  sql/common/tables.sql */

static bool
hypertable_tuple_found(TupleInfo *ti, void *data)
{
	HypertableNameCacheEntry *entry = data;
	entry->hypertable = hypertable_from_tuple(ti->tuple);
	return false;
}

static void *
hypertable_cache_create_entry(Cache *cache, CacheQuery *query)
{
	HypertableCacheQuery *hq = (HypertableCacheQuery *) query;
	Catalog    *catalog = catalog_get();
	HypertableNameCacheEntry *cache_entry = query->result;
	Hypertable *ht;
	int			number_found;
	ScanKeyData scankey[2];
	ScannerCtx	scanCtx = {
		.table = catalog->tables[HYPERTABLE].id,
		.index = catalog->tables[HYPERTABLE].index_ids[HYPERTABLE_NAME_INDEX],
		.scantype = ScannerTypeIndex,
		.nkeys = 2,
		.scankey = scankey,
		.data = query->result,
		.tuple_found = hypertable_tuple_found,
		.lockmode = AccessShareLock,
		.scandirection = ForwardScanDirection,
	};

	if (NULL == hq->schema)
		hq->schema = get_namespace_name(get_rel_namespace(hq->relid));

	if (NULL == hq->table)
		hq->table = get_rel_name(hq->relid);

	/* Perform an index scan on schema and table. */
	ScanKeyInit(&scankey[0], Anum_hypertable_name_idx_schema,
				BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(hq->schema));
	ScanKeyInit(&scankey[1], Anum_hypertable_name_idx_table,
				BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(hq->table));

	number_found = scanner_scan(&scanCtx);

	switch (number_found)
	{
		case 0:
			/* Negative cache entry: table is not a hypertable */
			cache_entry->hypertable = NULL;
			break;
		case 1:
			Assert(strncmp(cache_entry->hypertable->fd.schema_name.data, hq->schema, NAMEDATALEN) == 0);
			Assert(strncmp(cache_entry->hypertable->fd.table_name.data, hq->table, NAMEDATALEN) == 0);
			ht = cache_entry->hypertable;
			ht->space = dimension_scan(ht->fd.id, ht->main_table_relid, ht->fd.num_dimensions);
			break;
		default:
			elog(ERROR, "Got an unexpected number of records: %d", number_found);
			break;
	}

	return query->result;
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
hypertable_cache_get_entry(Cache *cache, Oid relid)
{
	if (!OidIsValid(relid))
		return NULL;

	return hypertable_cache_get_entry_with_table(cache, relid, NULL, NULL);
}

Hypertable *
hypertable_cache_get_entry_with_table(Cache *cache, Oid relid, const char *schema, const char *table)
{
	HypertableCacheQuery query = {
		.relid = relid,
		.schema = schema,
		.table = table,
	};
	HypertableNameCacheEntry *entry = cache_fetch(cache, &query.q);

	return entry->hypertable;
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
