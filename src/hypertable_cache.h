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

extern TSDLLEXPORT Hypertable *ts_hypertable_cache_get_entry(Cache *cache, Oid relid,
															 bool missing_ok);
extern Hypertable *ts_hypertable_cache_get_entry_no_resolve(Cache *cache, Oid relid);
extern TSDLLEXPORT Hypertable *ts_hypertable_cache_get_cache_and_entry(Oid relid, bool missing_ok,
																	   Cache **cache);
extern Hypertable *ts_hypertable_cache_get_entry_rv(Cache *cache, RangeVar *rv);
extern Hypertable *ts_hypertable_cache_get_entry_with_table(Cache *cache, Oid relid,
															const char *schema, const char *table,
															const bool nocreate);
extern TSDLLEXPORT Hypertable *ts_hypertable_cache_get_entry_by_id(Cache *cache,
																   int32 hypertable_id);

extern void ts_hypertable_cache_invalidate_callback(void);

extern TSDLLEXPORT Cache *ts_hypertable_cache_pin(void);

extern void _hypertable_cache_init(void);
extern void _hypertable_cache_fini(void);

#endif /* TIMESCALEDB_HYPERTABLE_CACHE_H */
