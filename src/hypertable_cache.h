#ifndef TIMESCALEDB_HYPERTABLE_CACHE_H
#define TIMESCALEDB_HYPERTABLE_CACHE_H

#include <postgres.h>
#include "cache.h"
#include "hypertable.h"
//#include "partitioning.h"

extern Hypertable *hypertable_cache_get_entry(Cache *cache, Oid relid);
extern Hypertable *hypertable_cache_get_entry_with_table(Cache *cache, Oid relid, const char *schema, const char *table);

//extern PartitionEpoch *hypertable_cache_get_partition_epoch(Cache *cache, Hypertable *hce, int64 time_pt, Oid relid);

extern void hypertable_cache_invalidate_callback(void);

extern Cache *hypertable_cache_pin(void);

extern void _hypertable_cache_init(void);
extern void _hypertable_cache_fini(void);

#endif   /* TIMESCALEDB_HYPERTABLE_CACHE_H */
