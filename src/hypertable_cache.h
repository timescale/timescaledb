#ifndef TIMESCALEDB_HYPERTABLE_CACHE_H
#define TIMESCALEDB_HYPERTABLE_CACHE_H

#include <postgres.h>
#include "cache.h"
#include "hypertable.h"

extern Hypertable *hypertable_cache_get_entry(Cache *cache, Oid relid);
extern Hypertable *hypertable_cache_get_entry_with_table(Cache *cache, Oid relid, const char *schema, const char *table);

extern void hypertable_cache_invalidate_callback(void);

extern Cache *hypertable_cache_pin(void);

extern void _hypertable_cache_init(void);
extern void _hypertable_cache_fini(void);

#endif   /* TIMESCALEDB_HYPERTABLE_CACHE_H */
