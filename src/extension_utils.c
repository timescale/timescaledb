/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
/* This file will be used by the versioned timescaledb extension and the loader
 * Because we want the loader not to export symbols all files here should be static
 * and be included via #include "extension_utils.c" instead of the regular linking process
 */

#include <postgres.h>
#include <access/xact.h>
#include <commands/extension.h>
#include <catalog/namespace.h>
#include <utils/lsyscache.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include <access/relscan.h>
#include <catalog/pg_extension.h>
#include <catalog/pg_authid.h>
#include <utils/fmgroids.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <utils/guc.h>
#include <catalog/indexing.h>

#include "compat.h"
#if PG12
#include <access/genam.h>
#endif

#include "extension_constants.h"

#define EXTENSION_PROXY_TABLE "cache_inval_extension"

#define RENDEZVOUS_LOADER_PRESENT_NAME "timescaledb.loader_present"

enum ExtensionState
{
	/*
	 * NOT_INSTALLED means that this backend knows that the extension is not
	 * present.  In this state we know that the proxy table is not present.
	 * Thus, the only way to get out of this state is a RelCacheInvalidation
	 * indicating that the proxy table was added. This is the state returned
	 * during DROP EXTENSION.
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
	 * TRANSITIONING only occurs in the middle of a CREATE EXTENSION or ALTER
	 * EXTENSION UPDATE
	 */
	EXTENSION_STATE_TRANSITIONING,

	/*
	 * CREATED means we know the extension is loaded, metadata is up-to-date,
	 * and we therefore do not need a full check until a RelCacheInvalidation
	 * on the proxy table.
	 */
	EXTENSION_STATE_CREATED,
};

static char *
extension_version(void)
{
	Datum result;
	Relation rel;
	SysScanDesc scandesc;
	HeapTuple tuple;
	ScanKeyData entry[1];
	bool is_null = true;
	char *sql_version = NULL;

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
		result = heap_getattr(tuple, Anum_pg_extension_extversion, RelationGetDescr(rel), &is_null);

		if (!is_null)
		{
			sql_version = pstrdup(TextDatumGetCString(result));
		}
	}

	systable_endscan(scandesc);
	table_close(rel, AccessShareLock);

	if (sql_version == NULL)
	{
		elog(ERROR, "extension not found while getting version");
	}
	return sql_version;
}

static bool inline proxy_table_exists()
{
	Oid nsid = get_namespace_oid(CACHE_SCHEMA_NAME, true);

	if (!OidIsValid(nsid))
		return false;

	return OidIsValid(get_relname_relid(EXTENSION_PROXY_TABLE, nsid));
}

static bool inline extension_exists()
{
	return OidIsValid(get_extension_oid(EXTENSION_NAME, true));
}

static bool inline extension_is_transitioning()
{
	/*
	 * Determine whether the extension is being created or upgraded (as a
	 * misnomer creating_extension is true during upgrades)
	 */
	if (creating_extension)
	{
		return get_extension_oid(EXTENSION_NAME, true) == CurrentExtensionObject;
	}
	return false;
}

/* Returns the recomputed current state */
static enum ExtensionState
extension_current_state()
{
	/*
	 * NormalProcessingMode necessary to avoid accessing cache before its
	 * ready (which may result in an infinite loop). More concretely we need
	 * RelationCacheInitializePhase3 to have been already called.
	 */
	if (!IsNormalProcessingMode() || !IsTransactionState())
		return EXTENSION_STATE_UNKNOWN;

	/*
	 * NOTE: do not check for proxy_table_exists here. Want to be in
	 * TRANSITIONING state even before the proxy table is created
	 */
	if (extension_is_transitioning())
		return EXTENSION_STATE_TRANSITIONING;

	/*
	 * proxy_table_exists uses syscache. Must come first. NOTE: during DROP
	 * EXTENSION proxy_table_exists() will return false right away, while
	 * extension_exists will return true until the end of the command
	 */
	if (proxy_table_exists())
	{
		Assert(extension_exists());
		return EXTENSION_STATE_CREATED;
	}

	return EXTENSION_STATE_NOT_INSTALLED;
}

static void
extension_load_without_preload()
{
	/* cannot use GUC variable here since extension not yet loaded */
	char *allow_install_without_preload =
		GetConfigOptionByName("timescaledb.allow_install_without_preload", NULL, true);

	if (allow_install_without_preload == NULL || strcmp(allow_install_without_preload, "on") != 0)
	{
		/*
		 * These are FATAL because otherwise the loader ends up in a weird
		 * half-loaded state after an ERROR
		 */
		/* Only privileged users can get the value of `config file` */

#if PG96
		if (superuser())
#else
		if (is_member_of_role(GetUserId(), DEFAULT_ROLE_READ_ALL_SETTINGS))
#endif
		{
			char *config_file = GetConfigOptionByName("config_file", NULL, false);

			ereport(FATAL,
					(errmsg("extension \"%s\" must be preloaded", EXTENSION_NAME),
					 errhint("Please preload the timescaledb library via "
							 "shared_preload_libraries.\n\n"
							 "This can be done by editing the config file at: %1$s\n"
							 "and adding 'timescaledb' to the list in the shared_preload_libraries "
							 "config.\n"
							 "	# Modify postgresql.conf:\n	shared_preload_libraries = "
							 "'timescaledb'\n\n"
							 "Another way to do this, if not preloading other libraries, is with "
							 "the command:\n"
							 "	echo \"shared_preload_libraries = 'timescaledb'\" >> %1$s \n\n"
							 "(Will require a database restart.)\n\n"
							 "If you REALLY know what you are doing and would like to load the "
							 "library without preloading, you can disable this check with: \n"
							 "	SET timescaledb.allow_install_without_preload = 'on';",
							 config_file)));
		}
		else
		{
			ereport(FATAL,
					(errmsg("extension \"%s\" must be preloaded", EXTENSION_NAME),
					 errhint(
						 "Please preload the timescaledb library via shared_preload_libraries.\n\n"
						 "This can be done by editing the postgres config file \n"
						 "and adding 'timescaledb' to the list in the shared_preload_libraries "
						 "config.\n"
						 "	# Modify postgresql.conf:\n	shared_preload_libraries = "
						 "'timescaledb'\n\n"
						 "Another way to do this, if not preloading other libraries, is with the "
						 "command:\n"
						 "	echo \"shared_preload_libraries = 'timescaledb'\" >> "
						 "/path/to/config/file \n\n"
						 "(Will require a database restart.)\n\n"
						 "If you REALLY know what you are doing and would like to load the library "
						 "without preloading, you can disable this check with: \n"
						 "	SET timescaledb.allow_install_without_preload = 'on';")));
		}

		return;
	}
}
