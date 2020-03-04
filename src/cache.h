/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CACHE_H
#define TIMESCALEDB_CACHE_H

#include <postgres.h>
#include <utils/memutils.h>
#include <utils/hsearch.h>

#include "export.h"

typedef enum CacheQueryFlags
{
	CACHE_FLAG_NONE = 0,
	CACHE_FLAG_MISSING_OK = 1 << 0,
	CACHE_FLAG_NOCREATE = 1 << 1,
} CacheQueryFlags;

#define CACHE_FLAG_CHECK (CACHE_FLAG_MISSING_OK | CACHE_FLAG_NOCREATE)

typedef struct CacheQuery
{
	/* CacheQueryFlags as defined above */
	const unsigned int flags;
	void *result;
	void *data;
} CacheQuery;

typedef struct CacheStats
{
	long numelements;
	uint64 hits;
	uint64 misses;
} CacheStats;

typedef struct Cache
{
	HASHCTL hctl;
	HTAB *htab;
	int refcount;
	const char *name;
	long numelements;
	int flags;
	CacheStats stats;
	void *(*get_key)(struct CacheQuery *);
	void *(*create_entry)(struct Cache *, CacheQuery *);
	void *(*update_entry)(struct Cache *, CacheQuery *);
	void (*missing_error)(const struct Cache *, const CacheQuery *);
	bool (*valid_result)(const void *);
	void (*pre_destroy_hook)(struct Cache *);
	bool release_on_commit; /* This should be false if doing
							 * cross-commit operations like CLUSTER or
							 * VACUUM */
} Cache;

extern void ts_cache_init(Cache *cache);
extern void ts_cache_invalidate(Cache *cache);
extern void *ts_cache_fetch(Cache *cache, CacheQuery *query);
extern bool ts_cache_remove(Cache *cache, void *key);

extern MemoryContext ts_cache_memory_ctx(Cache *cache);

extern Cache *ts_cache_pin(Cache *cache);
extern TSDLLEXPORT int ts_cache_release(Cache *cache);

extern void _cache_init(void);
extern void _cache_fini(void);

#endif /* TIMESCALEDB_CACHE_H */
