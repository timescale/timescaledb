#include <postgres.h>
#include <pg_config.h>
#include <access/xact.h>
#include "../compat-msvc-enter.h"
#include <commands/extension.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include "../compat-msvc-exit.h"
#include <utils/guc.h>
#include <utils/inval.h>
#include <nodes/print.h>

#define EXTENSION_NAME "timescaledb"

#include "../extension_utils.c"

#define PG96 ((PG_VERSION_NUM >= 90600) && (PG_VERSION_NUM < 100000))
#define PG10 ((PG_VERSION_NUM >= 100000) && (PG_VERSION_NUM < 110000))
/*
 * Some notes on design:
 *
 * We do not check for the installation of the extension upon loading the extension and instead rely on a hook for two reasons:
 * 1) We probably can't
 *	- The shared_preload_libraries is called in PostmasterMain which is way before InitPostgres is called.
 *			(Note: This happens even before the fork of the backend) -- so we don't even know which database this is for.
 *	-- This means we cannot query for the existance of the extension yet because the caches are initialized in InitPostgres.
 * 2) We actually don't want to load the extension in two cases:
 *	  a) We are upgrading the extension.
 *	  b) We set the guc timescaledb.disable_load.
 *
 *
 */

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define GUC_DISABLE_LOAD_NAME "timescaledb.disable_load"

extern void PGDLLEXPORT _PG_init(void);
extern void PGDLLEXPORT _PG_fini(void);

/* was the versioned-extension loaded*/
static bool loaded = false;

/* GUC to disable the load */
static bool guc_disable_load = false;

/* This is the hook that existed before the loader was installed */
static post_parse_analyze_hook_type prev_post_parse_analyze_hook;

/* This is timescaleDB's versioned-extension's post_parse_analyze_hook */
static post_parse_analyze_hook_type extension_post_parse_analyze_hook = NULL;

static void inline extension_check(void);
static void call_extension_post_parse_analyze_hook(ParseState *pstate,
									   Query *query);


static void
inval_cache_callback(Datum arg, Oid relid)
{
	if (guc_disable_load)
		return;
	extension_check();
}


static bool
drop_statement_drops_extension(DropStmt *stmt)
{
	if (stmt->removeType == OBJECT_EXTENSION)
	{
		if (list_length(stmt->objects) == 1)
		{
			char	   *ext_name;
#if PG96
			List	   *names = linitial(stmt->objects);

			Assert(list_length(names) == 1);
			ext_name = strVal(linitial(names));
#elif PG10
			void	   *name = linitial(stmt->objects);

			ext_name = strVal(name);
#endif
			if (strcmp(ext_name, EXTENSION_NAME) == 0)
				return true;
		}
	}
	return false;
}

static bool
load_utility_cmd(Node *utilityStmt)
{
	switch (nodeTag(utilityStmt))
	{
		case T_VariableSetStmt:
			if (strcmp(((VariableSetStmt *) utilityStmt)->name, GUC_DISABLE_LOAD_NAME) == 0)
				/* Do not load when setting the guc */
				return false;
			return true;
		case T_AlterExtensionStmt:
			if (strcmp(((AlterExtensionStmt *) utilityStmt)->extname, EXTENSION_NAME) != 0)
				return true;

			/* disallow loading two .so from different versions */
			if (loaded)
				ereport(ERROR,
						(errmsg("Cannot update the extension after the old version has already been loaded"),
						 errhint("You should start a new session and execute ALTER EXTENSION as the first command")));

			/* do not load the current (old) version's .so */
			return false;
		case T_CreateExtensionStmt:
			if (!loaded || strcmp(((CreateExtensionStmt *) utilityStmt)->extname, EXTENSION_NAME) != 0)
				return true;

			/* disallow loading two .so from different versions */
			ereport(ERROR,
					(errmsg("Cannot create the extension after the another version has already been loaded"),
					 errhint("You should start a new session and execute CREATE EXTENSION as the first command")));

		case T_DropStmt:
			if (drop_statement_drops_extension((DropStmt *) utilityStmt))
				/* do not load when dropping */
				return false;
			return true;
		default:
			return true;
	}
}

static void
post_analyze_hook(ParseState *pstate, Query *query)
{
	if (!guc_disable_load && (query->commandType != CMD_UTILITY || load_utility_cmd(query->utilityStmt)))
		extension_check();

	/*
	 * Call the extension's hook. This is necessary since the extension is
	 * installed during the hook. If we did not do this the extension's hook
	 * would not be called during the first command because the extension
	 * would not have yet been installed. Thus the loader captures the
	 * extension hook and calls it explicitly after the check for installing
	 * the extension.
	 */
	call_extension_post_parse_analyze_hook(pstate, query);

	if (prev_post_parse_analyze_hook != NULL)
	{
		prev_post_parse_analyze_hook(pstate, query);
	}
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		/* cannot use GUC variable here since extension not yet loaded */
		char	   *allow_install_without_preload = GetConfigOptionByName("timescaledb.allow_install_without_preload", NULL, true);

		if (allow_install_without_preload == NULL ||
			strcmp(allow_install_without_preload, "on") != 0)
		{
			char	   *config_file = GetConfigOptionByName("config_file", NULL, false);

			ereport(ERROR,
					(errmsg("The timescaledb library is not preloaded"),
					 errhint("Please preload the timescaledb library via shared_preload_libraries.\n\n"
							 "This can be done by editing the config file at: %1$s\n"
							 "and adding 'timescaledb' to the list in the shared_preload_libraries config.\n"
							 "	# Modify postgresql.conf:\n	shared_preload_libraries = 'timescaledb'\n\n"
							 "Another way to do this, if not preloading other libraries, is with the command:\n"
							 "	echo \"shared_preload_libraries = 'timescaledb'\" >> %1$s \n\n"
							 "(Will require a database restart.)\n\n"
							 "If you REALLY know what you are doing and would like to load the library without preloading, you can disable this check with: \n"
							 "	SET timescaledb.allow_install_without_preload = 'on';", config_file)));
			return;
		}
	}
	elog(INFO, "timescaledb loaded");

	/* This is a safety-valve variable to prevent loading the full extension */
	DefineCustomBoolVariable(GUC_DISABLE_LOAD_NAME, "Disable the loading of the actual extension",
							 NULL,
							 &guc_disable_load,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	/*
	 * cannot check for extension here since not inside a transaction yet. Nor
	 * do we even have an assigned database yet
	 */

	CacheRegisterRelcacheCallback(inval_cache_callback, PointerGetDatum(NULL));

	/*
	 * using the post_parse_analyze_hook since it's the earliest available
	 * hook
	 */
	prev_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = post_analyze_hook;
}

void
_PG_fini(void)
{
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	/* No way to unregister relcache callback */
}

static void inline
do_load()
{
	char	   *version = extension_version();
	char		soname[MAX_SO_NAME_LEN];
	post_parse_analyze_hook_type old_hook;


	snprintf(soname, MAX_SO_NAME_LEN, "%s-%s", EXTENSION_NAME, version);

	/*
	 * we need to capture the loaded extension's post analyze hook, giving it
	 * a NULL as previous
	 */
	old_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = NULL;

	PG_TRY();
	{
		load_file(soname, false);
		loaded = true;
	}
	PG_CATCH();
	{
		/* Assume the extension was loaded to prevent re-loading another .so */
		loaded = true;

		extension_post_parse_analyze_hook = post_parse_analyze_hook;
		post_parse_analyze_hook = old_hook;
		PG_RE_THROW();
	}
	PG_END_TRY();

	extension_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = old_hook;
}

static void inline
extension_check()
{
	if (!loaded)
	{
		enum ExtensionState state = extension_current_state();

		switch (state)
		{
			case EXTENSION_STATE_TRANSITIONING:

				/*
				 * Always load as soon as the extension is transitioning. This
				 * is necessary so that the extension load before any CREATE
				 * FUNCTION calls. Otherwise, the CREATE FUNCTION calls will
				 * load the .so without capturing the post_parse_analyze_hook.
				 */
			case EXTENSION_STATE_CREATED:
				do_load();
				return;
			case EXTENSION_STATE_UNKNOWN:
			case EXTENSION_STATE_NOT_INSTALLED:
				return;
		}
	}
}

static void
call_extension_post_parse_analyze_hook(ParseState *pstate,
									   Query *query)
{
	if (loaded && extension_post_parse_analyze_hook != NULL)
	{
		extension_post_parse_analyze_hook(pstate, query);
	}
}
