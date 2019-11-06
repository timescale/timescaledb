/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/xact.h>
#include <access/transam.h>
#include <commands/event_trigger.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <utils/inval.h>

#include "compat-msvc-enter.h" /* To label externs in extension.h and
								 * miscadmin.h correctly */
#include <commands/extension.h>
#include <miscadmin.h>
#include "compat-msvc-exit.h"

#include <access/relscan.h>
#include <catalog/indexing.h>
#include <catalog/pg_extension.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>

#include "catalog.h"
#include "extension.h"
#include "guc.h"
#include "config.h"
#include "extension_utils.c"
#include "compat.h"

static Oid extension_proxy_oid = InvalidOid;

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
 * of Postgres's dependency management:
 *	* The proxy table will be created before the extension itself.
 *	* The proxy table will be dropped before the extension itself.
 */

static enum ExtensionState extstate = EXTENSION_STATE_UNKNOWN;

static bool
extension_loader_present()
{
	void **presentptr = find_rendezvous_variable(RENDEZVOUS_LOADER_PRESENT_NAME);

	return (*presentptr != NULL && *((bool *) *presentptr));
}

void
ts_extension_check_version(const char *so_version)
{
	char *sql_version;

	if (!IsNormalProcessingMode() || !IsTransactionState() || !extension_exists())
		return;
	sql_version = extension_version();

	if (strcmp(sql_version, so_version) != 0)
	{
		/*
		 * Throw a FATAL error here so that clients will be forced to reconnect
		 * when they have the wrong extension version loaded.
		 */
		ereport(FATAL,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("extension \"%s\" version mismatch: shared library version %s; SQL version "
						"%s",
						EXTENSION_NAME,
						so_version,
						sql_version)));
	}

	if (!process_shared_preload_libraries_in_progress && !extension_loader_present())
	{
		extension_load_without_preload();
	}
}

void
ts_extension_check_server_version()
{
	/*
	 * This is a load-time check for the correct server version since the
	 * extension may be distributed as a binary
	 */
	char *server_version_num_guc = GetConfigOptionByName("server_version_num", NULL, false);
	long server_version_num = strtol(server_version_num_guc, NULL, 10);

	if (!is_supported_pg_version(server_version_num))
	{
		char *server_version_guc = GetConfigOptionByName("server_version", NULL, false);

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("extension \"%s\" does not support postgres version %s",
						EXTENSION_NAME,
						server_version_guc)));
	}
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
			ts_extension_check_version(TIMESCALEDB_VERSION_MOD);
			extension_proxy_oid = get_relname_relid(EXTENSION_PROXY_TABLE,
													get_namespace_oid(CACHE_SCHEMA_NAME, false));
			ts_catalog_reset();
			break;
		case EXTENSION_STATE_NOT_INSTALLED:
			extension_proxy_oid = InvalidOid;
			ts_catalog_reset();
			break;
	}
	extstate = newstate;
	return true;
}

/* Updates the state based on the current state, returning whether there had been a change. */
static bool
extension_update_state()
{
	return extension_set_state(extension_current_state());
}

Oid
ts_extension_schema_oid(void)
{
	Datum result;
	Relation rel;
	SysScanDesc scandesc;
	HeapTuple tuple;
	ScanKeyData entry[1];
	bool is_null = true;
	Oid schema = InvalidOid;

	rel = table_open(ExtensionRelationId, AccessShareLock);

	ScanKeyInit(&entry[0],
				Anum_pg_extension_extname,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(EXTENSION_NAME)));

	scandesc = systable_beginscan(rel, ExtensionNameIndexId, true, NULL, 1, entry);

	tuple = systable_getnext(scandesc);

	/* We assume that there can be at most one matching tuple */
	if (HeapTupleIsValid(tuple))
	{
		result =
			heap_getattr(tuple, Anum_pg_extension_extnamespace, RelationGetDescr(rel), &is_null);

		if (!is_null)
			schema = DatumGetObjectId(result);
	}

	systable_endscan(scandesc);
	table_close(rel, AccessShareLock);

	if (schema == InvalidOid)
		elog(ERROR, "extension schema not found");
	return schema;
}

char *
ts_extension_schema_name(void)
{
	return get_namespace_name(ts_extension_schema_oid());
}

/*
 *	Called upon all Relcache invalidate events.
 *	Returns whether or not to invalidate the entire extension.
 */
bool
ts_extension_invalidate(Oid relid)
{
	static bool in_recursion = false;
	bool invalidate_all = false;

	/* Since the state of the extension is determined by the snapshot of the transaction there
	 * is no point processing recursive calls as the outer call will always set the correct state.
	 * This also prevents deep recursion during `AcceptInvalidationMessages`.
	 */
	if (in_recursion)
		return false;

	in_recursion = true;

	switch (extstate)
	{
		case EXTENSION_STATE_NOT_INSTALLED:
			/* This event may mean we just added the proxy table */
		case EXTENSION_STATE_UNKNOWN:
			/* Can we recompute the state now? */
		case EXTENSION_STATE_TRANSITIONING:
			/* Has the create/drop extension finished? */
			extension_update_state();
			break;
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
					invalidate_all = true;
				}
			}
			break;
		default:
			elog(ERROR, "unknown state: %d", extstate);
			break;
	}
	in_recursion = false;
	return invalidate_all;
}

bool
ts_extension_is_loaded(void)
{
	/* when restoring deactivate extension */
	if (ts_guc_restoring)
		return false;

	if (EXTENSION_STATE_UNKNOWN == extstate || EXTENSION_STATE_TRANSITIONING == extstate)
	{
		/* status may have updated without a relcache invalidate event */
		extension_update_state();
	}

	switch (extstate)
	{
		case EXTENSION_STATE_CREATED:
			return true;
		case EXTENSION_STATE_NOT_INSTALLED:
		case EXTENSION_STATE_UNKNOWN:
		case EXTENSION_STATE_TRANSITIONING:

			/*
			 * Turn off extension during upgrade scripts. This is necessary so
			 * that, for example, the catalog does not go looking for things
			 * that aren't yet there.
			 */
			return false;
		default:
			elog(ERROR, "unknown state: %d", extstate);
			return false;
	}
}

char *
ts_extension_get_so_name()
{
	/* TODO: after merge check whether this is the right place */
	return EXTENSION_NAME "-" TIMESCALEDB_VERSION_MOD;
}
