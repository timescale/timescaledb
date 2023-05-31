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
#include <access/genam.h>
#include <access/relscan.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_authid.h>
#include <catalog/pg_extension.h>
#include <commands/extension.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>

#include "compat/compat.h"
#include "extension_constants.h"
#include "utils.h"

#define EXTENSION_PROXY_TABLE "cache_inval_extension"

#define RENDEZVOUS_LOADER_PRESENT_NAME "timescaledb.loader_present"

enum ExtensionState
{
	/*
	 * NOT_INSTALLED means that this backend knows that the extension is not
	 * present. In this state we know that the proxy table is not present.
	 * This state is never saved since there is no real way to get out of it
	 * since we cannot signal via the proxy table as its relid is not known
	 * post installation without a full lookup, which is not allowed in the
	 * relcache callback.
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
extension_version(char const *const extension_name)
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
				CStringGetDatum(extension_name));

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

inline static bool
extension_exists(char const *const extension_name)
{
	return OidIsValid(get_extension_oid(extension_name, true));
}

inline static bool
extension_is_transitioning(char const *const extension_name)
{
	/*
	 * Determine whether the extension is being created or upgraded (as a
	 * misnomer creating_extension is true during upgrades)
	 */
	if (creating_extension)
	{
		return get_extension_oid(extension_name, true) == CurrentExtensionObject;
	}
	return false;
}

/* Returns the recomputed current state.
 *
 * (schema_name, table_name) refer to a table that is owned by the extension.
 * thus it is created with the extension and dropped with the extension.
 */
static enum ExtensionState
extension_current_state(char const *const extension_name, char const *const schema_name,
						char const *const table_name)
{
	/*
	 * NormalProcessingMode necessary to avoid accessing cache before its
	 * ready (which may result in an infinite loop). More concretely we need
	 * RelationCacheInitializePhase3 to have been already called.
	 */
	if (!IsNormalProcessingMode() || !IsTransactionState() || !OidIsValid(MyDatabaseId))
		return EXTENSION_STATE_UNKNOWN;

	/*
	 * NOTE: do not check for (schema_name, table_name) existing here. Want to be in
	 * TRANSITIONING state even before that table is created
	 */
	if (extension_is_transitioning(extension_name))
		return EXTENSION_STATE_TRANSITIONING;

	/*
	 * We use syscache to check (schema_name, table_name) exists. Must come first.
	 *
	 * A table that is owned by the extension is dropped before the extension itself is dropped.
	 * this logic lets us detect that an extension is being dropped early on in the drop process and
	 * return `EXTENSION_STATE_NOT_INSTALLED` if that is the case.
	 * It is best to use a table created early on in the extension creation process because
	 * that means it will be dropped early in the drop process.
	 */
	if (OidIsValid(ts_get_relation_relid(schema_name, table_name, true)))
	{
		Assert(extension_exists(extension_name));
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

		if (has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_SETTINGS))
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
