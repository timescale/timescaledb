#ifndef TIMESCALEDB_CACHE_H
#define TIMESCALEDB_CACHE_H

#include <postgres.h>
#include <utils/memutils.h>
#include <utils/hsearch.h>

typedef struct CacheQuery
{
	void	   *result;
	void	   *data;
} CacheQuery;

typedef struct CacheStats
{
	long		numelements;
	uint64		hits;
	uint64		misses;
} CacheStats;

typedef struct Cache
{
	HASHCTL		hctl;
	HTAB	   *htab;
	int			refcount;
	const char *name;
	long		numelements;
	int			flags;
	CacheStats	stats;
	void	   *(*get_key) (struct CacheQuery *);
	void	   *(*create_entry) (struct Cache *, CacheQuery *);
	void	   *(*update_entry) (struct Cache *, CacheQuery *);
	void		(*pre_destroy_hook) (struct Cache *);
	bool		release_on_commit;	/* This should be false if doing
									 * cross-commit operations like CLUSTER or
									 * VACUUM */
} Cache;

extern void cache_init(Cache *cache);
extern void cache_invalidate(Cache *cache);
extern void *cache_fetch(Cache *cache, CacheQuery *query);
extern bool cache_remove(Cache *cache, void *key);

extern MemoryContext cache_memory_ctx(Cache *cache);
extern MemoryContext cache_switch_to_memory_context(Cache *cache);

extern Cache *cache_pin(Cache *cache);
extern int	cache_release(Cache *cache);
extern void _cache_init(void);
extern void _cache_fini(void);

#endif							/* TIMESCALEDB_CACHE_H */
