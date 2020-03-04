/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/namespace.h>
#include <utils/catcache.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>

#include "errors.h"
#include "hypertable_cache.h"
#include "hypertable.h"
#include "catalog.h"
#include "cache.h"
#include "scanner.h"
#include "dimension.h"
#include "tablespace.h"

static void *hypertable_cache_create_entry(Cache *cache, CacheQuery *query);
static void hypertable_cache_missing_error(const Cache *cache, const CacheQuery *query);

typedef struct HypertableCacheQuery
{
	CacheQuery q;
	Oid relid;
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
	Oid relid;
	Hypertable *hypertable;
} HypertableCacheEntry;

static bool
hypertable_cache_valid_result(const void *result)
{
	if (result == NULL)
		return false;
	return ((HypertableCacheEntry *) result)->hypertable != NULL;
}

static Cache *
hypertable_cache_create()
{
	MemoryContext ctx =
		AllocSetContextCreate(CacheMemoryContext, "Hypertable cache", ALLOCSET_DEFAULT_SIZES);

	Cache *cache = MemoryContextAlloc(ctx, sizeof(Cache));
	Cache		template =
	{
		.hctl =
		{
			.keysize = sizeof(Oid),
			.entrysize = sizeof(HypertableCacheEntry),
			.hcxt = ctx,
		},
		.name = "hypertable_cache",
		.numelements = 16,
		.flags = HASH_ELEM | HASH_CONTEXT | HASH_BLOBS,
		.get_key = hypertable_cache_get_key,
		.create_entry = hypertable_cache_create_entry,
		.missing_error = hypertable_cache_missing_error,
		.valid_result = hypertable_cache_valid_result,
	};

	*cache = template;

	ts_cache_init(cache);

	return cache;
}

static Cache *hypertable_cache_current = NULL;

static ScanTupleResult
hypertable_tuple_found(TupleInfo *ti, void *data)
{
	HypertableCacheEntry *entry = data;

	entry->hypertable = ts_hypertable_from_tupleinfo(ti);
	return SCAN_DONE;
}

static void *
hypertable_cache_create_entry(Cache *cache, CacheQuery *query)
{
	HypertableCacheQuery *hq = (HypertableCacheQuery *) query;
	HypertableCacheEntry *cache_entry = query->result;
	int number_found;

	if (NULL == hq->schema)
		hq->schema = get_namespace_name(get_rel_namespace(hq->relid));

	if (NULL == hq->table)
		hq->table = get_rel_name(hq->relid);

	number_found = ts_hypertable_scan_with_memory_context(hq->schema,
														  hq->table,
														  hypertable_tuple_found,
														  query->result,
														  AccessShareLock,
														  false,
														  ts_cache_memory_ctx(cache));

	switch (number_found)
	{
		case 0:
			/* Negative cache entry: table is not a hypertable */
			cache_entry->hypertable = NULL;
			break;
		case 1:
			Assert(strncmp(cache_entry->hypertable->fd.schema_name.data, hq->schema, NAMEDATALEN) ==
				   0);
			Assert(strncmp(cache_entry->hypertable->fd.table_name.data, hq->table, NAMEDATALEN) ==
				   0);
			break;
		default:
			elog(ERROR, "got an unexpected number of records: %d", number_found);
			break;
	}

	return cache_entry->hypertable == NULL ? NULL : cache_entry;
}

static void
hypertable_cache_missing_error(const Cache *cache, const CacheQuery *query)
{
	HypertableCacheQuery *hq = (HypertableCacheQuery *) query;
	const char *const rel_name = get_rel_name(hq->relid);

	if (rel_name == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("OID %u does not refer to a table", hq->relid)));
	else
		ereport(ERROR,
				(errcode(ERRCODE_TS_HYPERTABLE_NOT_EXIST),
				 errmsg("table \"%s\" is not a hypertable", rel_name)));
}

void
ts_hypertable_cache_invalidate_callback(void)
{
	ts_cache_invalidate(hypertable_cache_current);
	hypertable_cache_current = hypertable_cache_create();
}

/* Get hypertable cache entry. If the entry is not in the cache, add it. */
TSDLLEXPORT Hypertable *
ts_hypertable_cache_get_entry(Cache *const cache, const Oid relid, const unsigned int flags)
{
	if (!OidIsValid(relid))
	{
		if (flags & CACHE_FLAG_MISSING_OK)
			return NULL;
		else
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid Oid")));
	}

	return ts_hypertable_cache_get_entry_with_table(cache, relid, NULL, NULL, flags);
}

/*
 * Returns cache into the argument and hypertable as the function result.
 * If hypertable is not found, fails with an error.
 */
Hypertable *
ts_hypertable_cache_get_cache_and_entry(const Oid relid, const unsigned int flags,
										Cache **const cache)
{
	*cache = ts_hypertable_cache_pin();
	return ts_hypertable_cache_get_entry(*cache, relid, flags);
}

Hypertable *
ts_hypertable_cache_get_entry_rv(Cache *cache, const RangeVar *rv)
{
	return ts_hypertable_cache_get_entry(cache, RangeVarGetRelid(rv, NoLock, true), true);
}

TSDLLEXPORT Hypertable *
ts_hypertable_cache_get_entry_by_id(Cache *cache, const int32 hypertable_id)
{
	return ts_hypertable_cache_get_entry(cache, ts_hypertable_id_to_relid(hypertable_id), true);
}

Hypertable *
ts_hypertable_cache_get_entry_with_table(Cache *cache, const Oid relid, const char *schema,
										 const char *table, const unsigned int flags)
{
	HypertableCacheQuery query = {
		.q.flags = flags,
		.relid = relid,
		.schema = schema,
		.table = table,
	};
	HypertableCacheEntry *entry = ts_cache_fetch(cache, &query.q);
	Assert((flags & CACHE_FLAG_MISSING_OK) ? true : (entry != NULL && entry->hypertable != NULL));
	return entry == NULL ? NULL : entry->hypertable;
}

extern TSDLLEXPORT Cache *
ts_hypertable_cache_pin()
{
	return ts_cache_pin(hypertable_cache_current);
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
	ts_cache_invalidate(hypertable_cache_current);
}
