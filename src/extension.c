#include <postgres.h>
#include <access/xact.h>
#include <access/transam.h>
#include <commands/extension.h>
#include <commands/event_trigger.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <utils/inval.h>

#include "catalog.h"
#include "extension.h"

#define EXTENSION_PROXY_TABLE "cache_inval_extension"

static Oid	extension_proxy_oid = InvalidOid;

/*
 * ExtensionState tracks the state of extension metadata in the backend.
 *
 * Since we want to cache extension metadata to speed up common checks (e.g.,
 * check for presence of the extension itself), we also need to track the
 * extension state to know when the metadata is valid.
 *
 * We use a proxy_table to be notified of extension drops/creates. Namely,
 * we rely on the fact that postgres will internally create RelCacheInvalidation
 * events when any tables are created or dropped. We rely on the following properties
 * of Postgres's dependency managment:
 *	* The proxy table will be created before the extension itself.
 *	* The proxy table will be dropped before the extension itself.
 */
enum ExtensionState
{
	/*
	 * NOT_INSTALLED means that this backend knows that the extension is not
	 * present.  In this state we know that the proxy table is not present.
	 * Thus, the only way to get out of this state is a RelCacheInvalidation
	 * indicating that the proxy table was added.
	 */
	EXTENSION_STATE_NOT_INSTALLED,

	/*
	 * UNKNOWN state is used only if we cannot be sure what the state is. This
	 * can happen in two cases: 1) at the start of a backend or 2) We got a
	 * relcache event outside of a transaction and thus could not check the
	 * cache for the presence/absence of the proxy table or extension.
	 */
	EXTENSION_STATE_UNKNOWN,

	/*
	 * TRANSITIONING only occurs when the proxy table exists but the extension
	 * does not. This can only happen in the middle of a create or drop
	 * extension.
	 */
	EXTENSION_STATE_TRANSITIONING,

	/*
	 * CREATED means we know the extension is loaded, metadata is up-to-date,
	 * and we therefore do not need a full check until a RelCacheInvalidation
	 * on the proxy table.
	 */
	EXTENSION_STATE_CREATED,
};

static enum ExtensionState extstate = EXTENSION_STATE_UNKNOWN;

static bool
proxy_table_exists()
{
	Oid			nsid = get_namespace_oid(CACHE_SCHEMA_NAME, true);
	Oid			proxy_table = get_relname_relid(EXTENSION_PROXY_TABLE, nsid);

	return OidIsValid(proxy_table);
}

static bool
extension_exists()
{
	return OidIsValid(get_extension_oid(EXTENSION_NAME, true));
}

/* Returns the recomputed current state */
static enum ExtensionState
extension_new_state()
{
	if (!IsTransactionState())
		return EXTENSION_STATE_UNKNOWN;

	if (proxy_table_exists())
	{
		if (!extension_exists())
			return EXTENSION_STATE_TRANSITIONING;
		else
			return EXTENSION_STATE_CREATED;
	}
	return EXTENSION_STATE_NOT_INSTALLED;
}

/* Sets a new state, returning whether the state has changed */
static bool
extension_set_state(enum ExtensionState newstate)
{
	if (newstate == extstate)
	{
		return false;
	}
	switch (newstate)
	{
		case EXTENSION_STATE_TRANSITIONING:
		case EXTENSION_STATE_UNKNOWN:
			break;
		case EXTENSION_STATE_CREATED:
			extension_proxy_oid = get_relname_relid(EXTENSION_PROXY_TABLE, get_namespace_oid(CACHE_SCHEMA_NAME, false));
			catalog_reset();
			break;
		case EXTENSION_STATE_NOT_INSTALLED:
			extension_proxy_oid = InvalidOid;
			catalog_reset();
			break;
	}
	extstate = newstate;
	return true;
}

/* Updates the state based on the current state, returning whether there had been a change. */
static bool
extension_update_state()
{
	return extension_set_state(extension_new_state());
}

/*
 *	Called upon all Relcache invalidate events.
 *	Returns whether or not to invalidate the entire extension.
 */
bool
extension_invalidate(Oid relid)
{
	switch (extstate)
	{
		case EXTENSION_STATE_NOT_INSTALLED:
			/* This event may mean we just added the proxy table */
		case EXTENSION_STATE_UNKNOWN:
			/* Can we recompute the state now? */
		case EXTENSION_STATE_TRANSITIONING:
			/* Has the create/drop extension finished? */
			extension_update_state();
			return false;
		case EXTENSION_STATE_CREATED:

			/*
			 * Here we know the proxy table oid so only listen to potential
			 * drops on that oid. Note that an invalid oid passed in the
			 * invalidation event applies to all tables.
			 */
			if (extension_proxy_oid == relid || !OidIsValid(relid))
			{
				extension_update_state();
				if (EXTENSION_STATE_CREATED != extstate)
				{
					/*
					 * note this state may be UNKNOWN but should be
					 * conservative
					 */
					return true;
				}
			}
			return false;
		default:
			elog(ERROR, "unknown state: %d", extstate);
			return false;
	}
}


bool
extension_is_loaded(void)
{
	if (EXTENSION_STATE_UNKNOWN == extstate || EXTENSION_STATE_TRANSITIONING == extstate)
	{
		/* status may have updated without a relcache invalidate event */
		extension_update_state();
	}

	if (creating_extension && OidIsValid(get_extension_oid(EXTENSION_NAME, true)) && get_extension_oid(EXTENSION_NAME, true) == CurrentExtensionObject)
	{
		/* turn off extension during upgrade scripts */

		/*
		 * This is necessary so that, for example, the catalog does not go
		 * looking for things that aren't yet there.
		 */
		return false;
	}

	switch (extstate)
	{
		case EXTENSION_STATE_CREATED:
			return true;
		case EXTENSION_STATE_NOT_INSTALLED:
		case EXTENSION_STATE_UNKNOWN:
		case EXTENSION_STATE_TRANSITIONING:
			return false;
		default:
			elog(ERROR, "unknown state: %d", extstate);
			return false;
	}
}
