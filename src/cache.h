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

typedef struct CacheStorage {
	HTAB	      *htab;
	MemoryContext mcxt;
	int 		  refcount;
	bool 		  valid;
	void		(*destroy_storage_hook) (struct CacheStorage *);
} CacheStorage;

typedef struct Cache
{
	HASHCTL		hctl;
	CacheStorage *store;
	const char *name;
	long		numelements;
	int			flags;
	void	   *(*get_key) (struct CacheQueryCtx *);
	void	   *(*create_entry) (struct Cache *, CacheQueryCtx *);
	void	   *(*update_entry) (struct Cache *, CacheQueryCtx *);
	void		(*destroy_storage_hook) (struct CacheStorage *);
} Cache;

extern void cache_init(Cache *cache);
extern void cache_invalidate(Cache *cache);
extern void *cache_fetch(Cache *cache, CacheQueryCtx *ctx);

extern MemoryContext cache_memory_ctx(Cache *cache);
extern MemoryContext cache_switch_to_memory_context(Cache *cache);

extern CacheStorage *cache_pin_storage(Cache *cache);
extern void cache_release_storage(CacheStorage *store);

#endif   /* _IOBEAMDB_CACHE_H_ */
