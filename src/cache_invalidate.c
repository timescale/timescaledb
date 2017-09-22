/*
 *	Notes on the way caching works: Since out caches are stored in per-process
 *	(per-backend memory), we have to have a way to propagate invalidation
 *	messages to all backends, for that we use the Postgres relcache
 *	mechanism. Relcache is a cache that keeps internal info about
 *	relations(tables).  Postgres has a mechanism for registering for relcache
 *	invalidation events that are propagated to all backends:
 *	CacheRegisterRelcacheCallback(). We register inval_cache_callback() with
 *	this mechanism and route all invalidation messages through it to the correct
 *	cache invalidation functions.
 *
 *	The plan for our caches is to use (abuse) this mechanism to serve as a
 *	notify to invalidate our caches.  Thus, we create proxy tables for each
 *	cache we use and attach the invalidate_relcache_trigger trigger to all
 *	tables whose changes should invalidate the cache. This trigger will
 *	invalidate the relcache for the proxy table specified as the first argument
 *	to the trigger (see cache.sql).
 */

#include <postgres.h>
#include <access/xact.h>
#include <utils/lsyscache.h>
#include <catalog/namespace.h>
#include <miscadmin.h>
#include <commands/trigger.h>
#include <commands/event_trigger.h>
#include <nodes/nodes.h>
#include <utils/inval.h>
#include <unistd.h>

#include "hypertable_cache.h"
#include "catalog.h"
#include "extension.h"

void		_cache_invalidate_init(void);
void		_cache_invalidate_fini(void);
void		_cache_invalidate_extload(void);

/*
 * This function is called when any relcache is invalidated.
 * Should route the invalidation to the correct cache.
 */
static void
inval_cache_callback(Datum arg, Oid relid)
{
	Catalog    *catalog;

	if (extension_invalidate(relid))
	{
		hypertable_cache_invalidate_callback();
		return;
	}

	if (!extension_is_loaded())
		return;

	catalog = catalog_get();

	if (relid == catalog_get_cache_proxy_id(catalog, CACHE_TYPE_HYPERTABLE))
		hypertable_cache_invalidate_callback();
}

PGDLLEXPORT Datum invalidate_relcache_trigger(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(invalidate_relcache_trigger);

/*
 * This trigger causes the relcache for the cache_inval_proxy table (passed in
 * as arg 0) to be invalidated.  It should be called to invalidate the caches
 * associated with a proxy table (usually each cache has it's own proxy table)
 * This function is attached to the right tables in common/cache.sql
 *
 */
Datum
invalidate_relcache_trigger(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Oid			proxy_oid;
	Catalog    *catalog = catalog_get();

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "not called by trigger manager");

	/* arg 0 = name of the proxy table */
	proxy_oid = catalog_get_cache_proxy_id_by_name(catalog, trigdata->tg_trigger->tgargs[0]);
	if (proxy_oid != 0)
	{
		CacheInvalidateRelcacheByRelid(proxy_oid);
	}
	else
	{
		/*
		 * This can happen during  upgrade scripts when the catalog is
		 * unavailable
		 */
		CacheInvalidateRelcacheByRelid(get_relname_relid(trigdata->tg_trigger->tgargs[0], get_namespace_oid(CACHE_SCHEMA_NAME, false)));
	}

	/* tuple to return to executor */
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		return PointerGetDatum(trigdata->tg_newtuple);
	else
		return PointerGetDatum(trigdata->tg_trigtuple);
}

PGDLLEXPORT Datum invalidate_relcache(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(invalidate_relcache);

/*
 *	This is similar to invalidate_relcache_trigger but not a trigger.
 *	Not used regularly but useful for debugging.
 *
 */
Datum
invalidate_relcache(PG_FUNCTION_ARGS)
{
	Oid			proxy_oid = PG_GETARG_OID(0);

	/* arg 0 = relid of the cache_inval_proxy table */
	CacheInvalidateRelcacheByRelid(proxy_oid);

	/* tuple to return to executor */
	return BoolGetDatum(true);
}

void
_cache_invalidate_init(void)
{
	CacheRegisterRelcacheCallback(inval_cache_callback, PointerGetDatum(NULL));
}

void
_cache_invalidate_fini(void)
{
	/* No way to unregister relcache callback */
}
