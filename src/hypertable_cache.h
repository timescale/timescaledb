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
#include "export.h"

/* When a hypertable entry ht is fetched using the cache
 * i.e. ts_hypertable_cache_get_entry and variants, all related information such as
 *  hyperspaces, dimensions etc are also fetched into the cache. These are allocated in
 *  the cache's memory context.
 *  If the cache pin is released by calling ts_cache_release or variants, the memory
 *  associated with hypertable, its space dimensions etc. have also been released.
 *  As a best practice, call ts_cache_release right before returning from the function
 *  where the cache entry was acquired. This prevents inadvertent errors if someone
 *  modifies this function later and uses an indirectly linked object from the cache.
 *  Example:
 *  void my_func(...)
 *  {
 *
 *     Hypertable * ht = ts_hypertable_cache_get_xxx(...)
 *     ......
 *
 *    if ( error )
 *    {
 *        elog(ERROR, ... ); <----- ts_cache_release not needed here.
 *    }
 *
 *    .....
 *    ts_cache_release();
 *    return ..;
 *  }
 *  Note that any exceptions/errors i.e. elog/ereport etc. will trigger an automatic
 *  cache release. So there is no need for additional  ts_cache_release() calls.
 */
extern TSDLLEXPORT Hypertable *ts_hypertable_cache_get_entry(Cache *const cache, const Oid relid,
															 const unsigned int flags);
extern TSDLLEXPORT Hypertable *ts_hypertable_cache_get_cache_and_entry(const Oid relid,
																	   const unsigned int flags,
																	   Cache **const cache);
extern TSDLLEXPORT Hypertable *ts_hypertable_cache_get_entry_rv(Cache *cache, const RangeVar *rv);
extern TSDLLEXPORT Hypertable *
ts_hypertable_cache_get_entry_with_table(Cache *cache, const Oid relid, const char *schema,
										 const char *table, const unsigned int flags);
extern TSDLLEXPORT Hypertable *ts_hypertable_cache_get_entry_by_id(Cache *cache,
																   const int32 hypertable_id);

extern void ts_hypertable_cache_invalidate_callback(void);

extern TSDLLEXPORT Cache *ts_hypertable_cache_pin(void);

extern void _hypertable_cache_init(void);
extern void _hypertable_cache_fini(void);

#endif /* TIMESCALEDB_HYPERTABLE_CACHE_H */
