#include <postgres.h>
#include <access/xact.h>
#include <utils/lsyscache.h>
#include <utils/inval.h>
#include <catalog/namespace.h>
#include <commands/trigger.h>
#include <nodes/nodes.h>
#include <miscadmin.h>

#include "catalog.h"
#include "compat.h"
#include "extension.h"
#include "hypertable_cache.h"

/*
 * Notes on the way cache invalidation works.
 *
 * Since our caches are stored in per-process (per-backend memory), we need a
 * way to signal all backends that they should invalidate their caches. For this
 * we use the PostgreSQL relcache mechanism that propagates relation cache
 * invalidation events to all backends. We register a callback with this
 * mechanism to recieve events on all backends whenever a relation cache entry
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

void		_cache_invalidate_init(void);
void		_cache_invalidate_fini(void);
void		_cache_invalidate_extload(void);

/*
 * This function is called when any relcache is invalidated.
 * Should route the invalidation to the correct cache.
 */
static void
cache_invalidate_callback(Datum arg, Oid relid)
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

static inline CmdType
trigger_event_to_cmdtype(TriggerEvent event)
{
	if (TRIGGER_FIRED_BY_INSERT(event))
		return CMD_INSERT;

	if (TRIGGER_FIRED_BY_UPDATE(event))
		return CMD_UPDATE;

	return CMD_DELETE;
}

PGDLLEXPORT Datum invalidate_relcache_trigger(PG_FUNCTION_ARGS);

TS_FUNCTION_INFO_V1(invalidate_relcache_trigger);

/*
 * Trigger for catalog tables that invalidates caches.
 *
 * This trigger generates a cache invalidation event on changes to the catalog
 * table that the trigger is defined for.
 */
Datum
invalidate_relcache_trigger(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "not called by trigger manager");

	catalog_invalidate_cache(RelationGetRelid(trigdata->tg_relation),
							 trigger_event_to_cmdtype(trigdata->tg_event));

	/* tuple to return to executor */
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		return PointerGetDatum(trigdata->tg_newtuple);
	else
		return PointerGetDatum(trigdata->tg_trigtuple);
}

TS_FUNCTION_INFO_V1(invalidate_relcache);

/*
 * Force a cache invalidation for a catalog table.
 *
 * This function is used for debugging purposes and triggers
 * a cache invalidation.
 *
 * The first argument should be the catalog table that has changed, warranting a
 * cache invalidation.
 */
Datum
invalidate_relcache(PG_FUNCTION_ARGS)
{
	catalog_invalidate_cache(PG_GETARG_OID(0), CMD_UPDATE);
	PG_RETURN_BOOL(true);
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
			hypertable_cache_invalidate_callback();
		default:
			break;
	}
}

void
_cache_invalidate_init(void)
{
	RegisterXactCallback(cache_invalidate_xact_end, NULL);
	CacheRegisterRelcacheCallback(cache_invalidate_callback, PointerGetDatum(NULL));
}

void
_cache_invalidate_fini(void)
{
	UnregisterXactCallback(cache_invalidate_xact_end, NULL);
	/* No way to unregister relcache callback */
}
