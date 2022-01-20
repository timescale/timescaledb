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

#include "compat/compat-msvc-enter.h" /* To label externs in extension.h and
								 * miscadmin.h correctly */
#include <commands/extension.h>
#include <miscadmin.h>
#include "compat/compat-msvc-exit.h"

#include <access/relscan.h>
#include <catalog/indexing.h>
#include <catalog/pg_extension.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>

#include "ts_catalog/catalog.h"
#include "extension.h"
#include "guc.h"
#include "extension_utils.c"
#include "compat/compat.h"

#define TS_UPDATE_SCRIPT_CONFIG_VAR "timescaledb.update_script_stage"
#define POST_UPDATE "post"
/*
 * The name of the experimental schema.
 *
 * Call ts_extension_schema_name() or ts_experimental_schema_name() for
 * consistency. Don't use this macro directly.
 */
#define TS_EXPERIMENTAL_SCHEMA_NAME "timescaledb_experimental"
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

/*
 * Looking up the extension oid is a catalog lookup that can be costly, and we
 * often need it during the planning, so we cache it here. We update it when
 * the extension status is updated.
 */
Oid ts_extension_oid = InvalidOid;

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
static void
extension_update_state()
{
	static bool in_recursion = false;
	/* Since the state of the extension is determined by the snapshot of the transaction there
	 * is no point processing recursive calls as the outer call will always set the correct state.
	 * This also prevents deep recursion during `AcceptInvalidationMessages`.
	 */
	if (in_recursion)
		return;

	in_recursion = true;
	enum ExtensionState new_state = extension_current_state();
	extension_set_state(new_state);
	/*
	 * Update the extension oid. Note that it is only safe to run
	 * get_extension_oid() when the extension state is 'CREATED' or
	 * 'TRANSITIONING', because otherwise we might not be even able to do a
	 * catalog lookup because we are not in transaction state, and the like.
	 */
	if (new_state == EXTENSION_STATE_CREATED || new_state == EXTENSION_STATE_TRANSITIONING)
	{
		ts_extension_oid = get_extension_oid(EXTENSION_NAME, true /* missing_ok */);
		Assert(ts_extension_oid != InvalidOid);
	}
	else
	{
		ts_extension_oid = InvalidOid;
	}
	in_recursion = false;
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
				CStringGetDatum(EXTENSION_NAME));

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

const char *
ts_experimental_schema_name(void)
{
	return TS_EXPERIMENTAL_SCHEMA_NAME;
}

/*
 *	Called upon all Relcache invalidate events.
 *	Returns whether or not to invalidate the entire extension.
 */
bool
ts_extension_invalidate(Oid relid)
{
	bool invalidate_all = false;

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
	return invalidate_all;
}

bool
ts_extension_is_loaded(void)
{
	/* When restoring deactivate extension.
	 *
	 * We are using IsBinaryUpgrade (and ts_guc_restoring).  If a user set
	 * `ts_guc_restoring` for a database, it will be stored in
	 * `pg_db_role_settings` and be included in a dump, which will cause
	 * `pg_upgrade` to fail.
	 *
	 * See dumpDatabaseConfig in pg_dump.c. */
	if (ts_guc_restoring || IsBinaryUpgrade)
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
			if (extstate == EXTENSION_STATE_TRANSITIONING)
			{
				/* when we are updating the extension, we execute
				 * scripts in post_update.sql after setting up the
				 * the dependencies. At this stage, TS
				 * specific functionality is permitted as we now have
				 * all catalogs and functions in place
				 */
				const char *update_script_stage =
					GetConfigOption(TS_UPDATE_SCRIPT_CONFIG_VAR, true, false);
				if (update_script_stage &&
					(strncmp(update_script_stage, POST_UPDATE, strlen(POST_UPDATE)) == 0) &&
					(strlen(POST_UPDATE) == strlen(update_script_stage)))
					return true;
			}
			return false;
		default:
			elog(ERROR, "unknown state: %d", extstate);
			return false;
	}
}

const char *
ts_extension_get_so_name(void)
{
	return EXTENSION_NAME "-" TIMESCALEDB_VERSION_MOD;
}

/*
 * Get the currently installed extension version.
 */
const char *
ts_extension_get_version(void)
{
	return extension_version();
}
