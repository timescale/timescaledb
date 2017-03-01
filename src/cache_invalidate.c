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
#include <utils/inval.h>

#include "iobeamdb.h"
#include "hypertable_cache.h"
#include "chunk_cache.h"

#define CACHE_INVAL_PROXY_SCHEMA "_iobeamdb_cache"
#define CACHE_INVAL_PROXY_SCHEMA_OID get_namespace_oid(CACHE_INVAL_PROXY_SCHEMA, false)

void		_cache_invalidate_init(void);
void		_cache_invalidate_fini(void);
void		_cache_invalidate_extload(void);
Datum		invalidate_relcache_trigger(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(invalidate_relcache_trigger);


static Oid	hypertable_cache_inval_proxy_oid = InvalidOid;
static Oid	chunk_cache_inval_proxy_oid = InvalidOid;


/*
 * This function is called when any relcache is invalidated.
 * Should route the invalidation to the correct cache.
 */
static void
inval_cache_callback(Datum arg, Oid relid)
{
	/* TODO: look at IsTransactionState should this be necessary? */
	if (!IobeamLoaded())
		return;

	if (relid == InvalidOid || relid == hypertable_cache_inval_proxy_oid)
		invalidate_hypertable_cache_callback();

	if (relid == InvalidOid || relid == chunk_cache_inval_proxy_oid)
		invalidate_chunk_cache_callback();
}

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
	char	   *inval_proxy_name_arg;
	Oid			proxy_oid;

	if (!CALLED_AS_TRIGGER(fcinfo))
		elog(ERROR, "not called by trigger manager");

	/* arg 0 = relid of the cache_inval_proxy table */
	inval_proxy_name_arg = trigdata->tg_trigger->tgargs[0];
	proxy_oid = get_relname_relid(inval_proxy_name_arg, get_namespace_oid(CACHE_INVAL_PROXY_SCHEMA, false));
	CacheInvalidateRelcacheByRelid(proxy_oid);

	/* tuple to return to executor */
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		return PointerGetDatum(trigdata->tg_newtuple);
	else
		return PointerGetDatum(trigdata->tg_trigtuple);
}


void
_cache_invalidate_init(void)
{
}

void
_cache_invalidate_fini(void)
{
	/* No way to unregister relcache callback */
	hypertable_cache_inval_proxy_oid = InvalidOid;
	chunk_cache_inval_proxy_oid = InvalidOid;
}


void
_cache_invalidate_extload(void)
{
	CacheRegisterRelcacheCallback(inval_cache_callback, PointerGetDatum(NULL));
	hypertable_cache_inval_proxy_oid = get_relname_relid(HYPERTABLE_CACHE_INVAL_PROXY_TABLE, CACHE_INVAL_PROXY_SCHEMA_OID);
	chunk_cache_inval_proxy_oid = get_relname_relid(HYPERTABLE_CACHE_INVAL_PROXY_TABLE, CACHE_INVAL_PROXY_SCHEMA_OID);
}
