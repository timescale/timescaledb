/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_CONNECTION_CACHE_H
#define TIMESCALEDB_TSL_REMOTE_CONNECTION_CACHE_H

#include <postgres.h>
#include <catalog/pg_user_mapping.h>

#include "connection.h"
#include "cache.h"

/* this is a cross-transaction connection cache that allows you to reuse
connections across transactions on a per-backend basis.

Note that automatic cache unpinning on txn callbacks have been disabled on this cache.
That means unpinning on aborts has to be done by the caller. This is because this cache
is used by xact callbacks.
*/
extern Cache *remote_connection_cache_pin(void);

extern TSConnection *remote_connection_cache_get_connection(Cache *cache, UserMapping *user);

extern void remote_connection_cache_remove(Cache *cache, UserMapping *user_mapping);
extern void remote_connection_cache_remove_by_oid(Cache *cache, Oid user_mapping_oid);

extern void remote_connection_cache_invalidate_callback(void);
extern void _remote_connection_cache_init(void);
extern void _remote_connection_cache_fini(void);

#endif /* TIMESCALEDB_TSL_REMOTE_CONNECTION_CACHE_H */
