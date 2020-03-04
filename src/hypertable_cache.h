/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERTABLE_CACHE_H
#define TIMESCALEDB_HYPERTABLE_CACHE_H

#include <postgres.h>

#include "export.h"
#include "cache.h"
#include "hypertable.h"

extern TSDLLEXPORT Hypertable *ts_hypertable_cache_get_entry(Cache *cache, const Oid relid,
															 const unsigned int flags);
extern TSDLLEXPORT Hypertable *
ts_hypertable_cache_get_cache_and_entry(const Oid relid, const unsigned int flags, Cache **cache);
extern Hypertable *ts_hypertable_cache_get_entry_rv(Cache *cache, const RangeVar *rv);
extern Hypertable *ts_hypertable_cache_get_entry_with_table(Cache *cache, const Oid relid,
															const char *schema, const char *table,
															const unsigned int flags);
extern TSDLLEXPORT Hypertable *ts_hypertable_cache_get_entry_by_id(Cache *cache,
																   const int32 hypertable_id);

extern void ts_hypertable_cache_invalidate_callback(void);

extern TSDLLEXPORT Cache *ts_hypertable_cache_pin(void);

extern void _hypertable_cache_init(void);
extern void _hypertable_cache_fini(void);

#endif /* TIMESCALEDB_HYPERTABLE_CACHE_H */
