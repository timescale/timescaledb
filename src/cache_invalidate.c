/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <utils/lsyscache.h>
#include <utils/inval.h>
#include <catalog/namespace.h>
#include <nodes/nodes.h>
#include <miscadmin.h>
#include <utils/syscache.h>

#include "annotations.h"
#include "ts_catalog/catalog.h"
#include "compat/compat.h"
#include "extension.h"
#include "hypertable_cache.h"

#include "bgw/scheduler.h"
#include "cross_module_fn.h"

/*
 * Notes on the way cache invalidation works.
 *
 * Since our caches are stored in per-process (per-backend memory), we need a
 * way to signal all backends that they should invalidate their caches. For this
 * we use the PostgreSQL relcache mechanism that propagates relation cache
 * invalidation events to all backends. We register a callback with this
 * mechanism to receive events on all backends whenever a relation cache entry
 * is invalidated.
 *
 * To know which events should trigger invalidation of our caches, we use dummy
 * (empty) tables. We can trigger relcache invalidation events for these tables
 * to signal other backends. If the received table OID is a dummy table, we know
 * that this is an event that we care about.
 *
 * Caches for catalog tables should be invalidated on:
 *
 * 1. INSERT/UPDATE/DELETE on a catalog table
 * 2. Aborted transactions that taint the caches
 *
 * Generally, INSERTS do not warrant cache invalidation, unless it is an insert
 * of a subobject that belongs to an object that might already be in the cache
 * (e.g., a new dimension of a hypertable), or when replacing an existing entry
 * (e.g., when replacing a negative hypertable entry with a positive one). Note,
 * also, that INSERTS can taint the cache if the transaction that did the INSERT
 * fails. This is why we also need to invalidate caches on transaction failure.
 */

void _cache_invalidate_init(void);
void _cache_invalidate_fini(void);

static inline void
cache_invalidate_relcache_all(void)
{
	ts_hypertable_cache_invalidate_callback();
	ts_bgw_job_cache_invalidate_callback();
}

/*
 * This function is called when any relcache is invalidated.
 * Should route the invalidation to the correct cache.
 */
static void
cache_invalidate_callback(Datum arg, Oid relid)
{
	static bool in_recursion = false;

	if (ts_extension_invalidate(relid))
	{
		cache_invalidate_relcache_all();
		return;
	}

	if (!ts_extension_is_loaded())
		return;

	/* The cache invalidation can be called indirectly further down in the
	 * call chain by calling `get_namespace_oid`, which can trigger a
	 * recursive cache invalidation callback. To prevent infinite recursion,
	 * we stop it here and instead consider the cache as invalidated, which
	 * will allow the `get_namespace_oid` to retrieve the OID from the heap
	 * table and return it properly. */
	if (!in_recursion)
	{
		Catalog *catalog;
		in_recursion = true;
		catalog = ts_catalog_get();
		in_recursion = false;

		if (relid == ts_catalog_get_cache_proxy_id(catalog, CACHE_TYPE_HYPERTABLE))
			ts_hypertable_cache_invalidate_callback();

		if (relid == ts_catalog_get_cache_proxy_id(catalog, CACHE_TYPE_BGW_JOB))
			ts_bgw_job_cache_invalidate_callback();

		if (relid == InvalidOid)
			cache_invalidate_relcache_all();
	}
}

/* Registration for given cache ids happens in non-TSL code when the extension
 * is created.
 *
 * Cache entries get invalidated when either the foreign server entry or the
 * role entry in the PostgreSQL catalog changes.
 *
 * When the foreign server entry changes, connection paramaters might have
 * changed. When the role entry changes, the certificate used for client
 * authentication with backend data nodes might no longer be valid.
 */
static void
cache_invalidate_syscache_callback(Datum arg, int cacheid, uint32 hashvalue)
{
	Assert(cacheid == FOREIGNSERVEROID || cacheid == AUTHOID);
	ts_cm_functions->cache_syscache_invalidate(arg, cacheid, hashvalue);
}

TS_FUNCTION_INFO_V1(ts_timescaledb_invalidate_cache);

/*
 * Force a cache invalidation for a catalog table.
 *
 * This function is used for debugging purposes and triggers a cache
 * invalidation.
 *
 * The first argument should be the catalog table that has changed, warranting a
 * cache invalidation.
 */
Datum
ts_timescaledb_invalidate_cache(PG_FUNCTION_ARGS)
{
	ts_catalog_invalidate_cache(PG_GETARG_OID(0), CMD_UPDATE);
	PG_RETURN_VOID();
}

static void
cache_invalidate_xact_end(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:

			/*
			 * Invalidate caches on aborted transactions to purge entries that
			 * have been added during the transaction and are now no longer
			 * valid. Note that we need not signal other backends of this
			 * change since the transaction hasn't been committed and other
			 * backends cannot have the invalid state.
			 */
			cache_invalidate_relcache_all();
			break;
		default:
			break;
	}
}

static void
cache_invalidate_subxact_end(SubXactEvent event, SubTransactionId mySubid,
							 SubTransactionId parentSubid, void *arg)
{
	switch (event)
	{
		case SUBXACT_EVENT_ABORT_SUB:

			/*
			 * Invalidate caches on aborted sub transactions. See notes above
			 * in cache_invalidate_xact_end.
			 */
			cache_invalidate_relcache_all();
			break;
		default:
			break;
	}
}

void
_cache_invalidate_init(void)
{
	RegisterXactCallback(cache_invalidate_xact_end, NULL);
	RegisterSubXactCallback(cache_invalidate_subxact_end, NULL);
	CacheRegisterRelcacheCallback(cache_invalidate_callback, PointerGetDatum(NULL));

	/* Specific syscache callbacks */
	CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
								  cache_invalidate_syscache_callback,
								  PointerGetDatum(NULL));
	CacheRegisterSyscacheCallback(AUTHOID,
								  cache_invalidate_syscache_callback,
								  PointerGetDatum(NULL));
}

void
_cache_invalidate_fini(void)
{
	UnregisterXactCallback(cache_invalidate_xact_end, NULL);
	UnregisterSubXactCallback(cache_invalidate_subxact_end, NULL);
	/* No way to unregister relcache callback */
}
