/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_CONNECTION_CACHE_H
#define TIMESCALEDB_TSL_REMOTE_CONNECTION_CACHE_H

#include <postgres.h>

#include "connection.h"
#include "cache.h"

/*
 * This is a cross-transaction connection cache that allows you to reuse
 * connections across transactions on a per-data node basis.
 *
 * Note that automatic cache unpinning on txn callbacks have been disabled on this cache.
 * That means unpinning on aborts has to be done by the caller. This is because this cache
 * is used by xact callbacks.
 */

extern TSConnection *remote_connection_cache_get_connection(TSConnectionId id);
extern bool remote_connection_cache_remove(TSConnectionId id);

extern void remote_connection_cache_invalidate_callback(Datum arg, int cacheid, uint32 hashvalue);
extern void remote_connection_cache_dropped_db_callback(const char *dbname);
extern void remote_connection_cache_dropped_role_callback(const char *rolename);
extern Datum remote_connection_cache_show(PG_FUNCTION_ARGS);
extern void _remote_connection_cache_init(void);
extern void _remote_connection_cache_fini(void);

#endif /* TIMESCALEDB_TSL_REMOTE_CONNECTION_CACHE_H */
