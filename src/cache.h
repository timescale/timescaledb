#ifndef _IOBEAMDB_CACHE_H_
#define _IOBEAMDB_CACHE_H_

#include <postgres.h>
#include <utils/memutils.h>
#include <utils/hsearch.h>

typedef struct CacheQueryCtx
{
	void	   *entry;
	void	   *private[0];
} CacheQueryCtx;

typedef struct Cache
{
	HASHCTL		hctl;
	HTAB 		*htab;
	int 		refcount;
	const char  *name;
	long		numelements;
	int			flags;
	void	   *(*get_key) (struct CacheQueryCtx *);
	void	   *(*create_entry) (struct Cache *, CacheQueryCtx *);
	void	   *(*update_entry) (struct Cache *, CacheQueryCtx *);
	void		(*pre_destroy_hook) (struct Cache *);
} Cache;

extern void cache_init(Cache *cache);
extern void cache_invalidate(Cache *cache);
extern void *cache_fetch(Cache *cache, CacheQueryCtx *ctx);

extern MemoryContext cache_memory_ctx(Cache *cache);
extern MemoryContext cache_switch_to_memory_context(Cache *cache);

extern Cache *cache_pin(Cache *cache);
extern void cache_release(Cache *cache);

#endif   /* _IOBEAMDB_CACHE_H_ */
