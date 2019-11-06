/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <pg_config.h>
#include <access/xact.h>
#include <access/heapam.h>
#include "../compat-msvc-enter.h"
#include <postmaster/bgworker.h>
#include <commands/extension.h>
#include <commands/user.h>
#include <miscadmin.h>
#include <parser/analyze.h>
#include <storage/ipc.h>
#include "../compat-msvc-exit.h"
#include <utils/guc.h>
#include <utils/inval.h>
#include <nodes/print.h>
#include <commands/dbcommands.h>
#include <commands/defrem.h>
#include <access/parallel.h>

#include "extension_utils.c"
#include "export.h"
#include "compat.h"
#include "extension_constants.h"

#include "loader/loader.h"
#include "loader/bgw_counter.h"
#include "loader/bgw_interface.h"
#include "loader/bgw_launcher.h"
#include "loader/bgw_message_queue.h"
#include "loader/lwlocks.h"

/*
 * Loading process:
 *
 *   1. _PG_init starts up cluster-wide background worker stuff, and sets the
 *      post_parse_analyze_hook (a postgres-defined hook which is called after
 *      every statement is parsed) to our function post_analyze_hook
 *   2. When a command is run with timescale not loaded, post_analyze_hook:
 *        a. Gets the extension version.
 *        b. Loads the versioned extension.
 *        c. Grabs the post_parse_analyze_hook from the versioned extension
 *           (src/init.c:post_analyze_hook) and stores it in
 *           extension_post_parse_analyze_hook.
 *        d. Sets the post_parse_analyze_hook back to what it was before we
 *           loaded the versioned extension (this hook eventually called our
 *           post_analyze_hook, but may not be our function, for instance, if
 *           another extension is loaded).
 *        e. Calls extension_post_parse_analyze_hook.
 *        f. Calls the prev_post_parse_analyze_hook.
 *
 * Some notes on design:
 *
 * We do not check for the installation of the extension upon loading the extension and instead rely
 *on a hook for two reasons: 1) We probably can't
 *	- The shared_preload_libraries is called in PostmasterMain which is way before InitPostgres is
 *called. (Note: This happens even before the fork of the backend) -- so we don't even know which
 *database this is for.
 *	-- This means we cannot query for the existence of the extension yet because the caches are
 *initialized in InitPostgres. 2) We actually don't want to load the extension in two cases: a) We
 *are upgrading the extension. b) We set the guc timescaledb.disable_load.
 *
 * 3) We include a section for the bgw launcher and some workers below the rest, separated with its
 *own notes, some function definitions are included as they are referenced by other loader
 *functions.
 *
 */

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

#define POST_LOAD_INIT_FN "ts_post_load_init"
#define GUC_DISABLE_LOAD_NAME "timescaledb.disable_load"

/*
 * The loader really shouldn't load if we're in a parallel worker as there is a
 * separate infrastructure for loading libraries inside of parallel workers. The
 * issue is that IsParallelWorker() doesn't work on Windows because the var used
 * is not dll exported correctly, so we have an alternate macro that looks for
 * the parallel worker flags in MyBgworkerEntry, if it exists. These flags only
 * exist in PG10 and above, so we have to have switch back to IsParallelWorker()
 * for 9.6 and, unfortunately, can't do much about 9.6 Windows installs.
 */

#if PG96
#ifdef WIN32
#define CalledInParallelWorker() false
#else
#define CalledInParallelWorker() IsParallelWorker()
#endif /* WIN32 */
#else
#define CalledInParallelWorker()                                                                   \
	(MyBgworkerEntry != NULL && (MyBgworkerEntry->bgw_flags & BGWORKER_CLASS_PARALLEL) != 0)
#endif /* PG96 */
extern void TSDLLEXPORT _PG_init(void);
extern void TSDLLEXPORT _PG_fini(void);

/* was the versioned-extension loaded*/
static bool loaded = false;
static bool loader_present = true;

static char soversion[MAX_VERSION_LEN];

/* GUC to disable the load */
static bool guc_disable_load = false;

/* This is the hook that existed before the loader was installed */
static post_parse_analyze_hook_type prev_post_parse_analyze_hook;
static shmem_startup_hook_type prev_shmem_startup_hook;

/* This is timescaleDB's versioned-extension's post_parse_analyze_hook */
static post_parse_analyze_hook_type extension_post_parse_analyze_hook = NULL;

static void inline extension_check(void);
static void call_extension_post_parse_analyze_hook(ParseState *pstate, Query *query);

extern char *
ts_loader_extension_version(void)
{
	return extension_version();
}

extern bool
ts_loader_extension_exists(void)
{
	return extension_exists();
}

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
	if (!extension_exists())
		return false;

	if (stmt->removeType == OBJECT_EXTENSION)
	{
		if (list_length(stmt->objects) == 1)
		{
			char *ext_name;
#if PG96
			List *names = linitial(stmt->objects);

			Assert(list_length(names) == 1);
			ext_name = strVal(linitial(names));
#else
			void *name = linitial(stmt->objects);

			ext_name = strVal(name);
#endif
			if (strcmp(ext_name, EXTENSION_NAME) == 0)
				return true;
		}
	}
	return false;
}

static Oid
extension_owner(void)
{
	Datum result;
	Relation rel;
	SysScanDesc scandesc;
	HeapTuple tuple;
	ScanKeyData entry[1];
	bool is_null = true;
	Oid extension_owner = InvalidOid;

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
		result = heap_getattr(tuple, Anum_pg_extension_extowner, RelationGetDescr(rel), &is_null);

		if (!is_null)
			extension_owner = ObjectIdGetDatum(result);
	}

	systable_endscan(scandesc);
	table_close(rel, AccessShareLock);

	if (extension_owner == InvalidOid)
		elog(ERROR, "extension not found while getting owner");

	return extension_owner;
}

static bool
drop_owned_statement_drops_extension(DropOwnedStmt *stmt)
{
	Oid extension_owner_oid;
	List *role_ids;
	ListCell *lc;

	if (!extension_exists())
		return false;

	Assert(IsTransactionState());
	extension_owner_oid = extension_owner();

	role_ids = roleSpecsToIds(stmt->roles);

	/* Check privileges */
	foreach (lc, role_ids)
	{
		Oid role_id = lfirst_oid(lc);

		if (role_id == extension_owner_oid)
			return true;
	}
	return false;
}

static bool
should_load_on_variable_set(Node *utility_stmt)
{
	VariableSetStmt *stmt = (VariableSetStmt *) utility_stmt;

	switch (stmt->kind)
	{
		case VAR_SET_VALUE:
		case VAR_SET_DEFAULT:
		case VAR_RESET:
			/* Do not load when setting the guc to disable load */
			return stmt->name == NULL || strcmp(stmt->name, GUC_DISABLE_LOAD_NAME) != 0;
		default:
			return true;
	}
}

static bool
should_load_on_alter_extension(Node *utility_stmt)
{
	AlterExtensionStmt *stmt = (AlterExtensionStmt *) utility_stmt;

	if (strcmp(stmt->extname, EXTENSION_NAME) != 0)
		return true;

	/* disallow loading two .so from different versions */
	if (loaded)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("extension \"%s\" cannot be updated after the old version has already been "
						"loaded",
						stmt->extname),
				 errhint("Start a new session and execute ALTER EXTENSION as the first command. "
						 "Make sure to pass the \"-X\" flag to psql.")));
	/* do not load the current (old) version's .so */
	return false;
}

static bool
should_load_on_create_extension(Node *utility_stmt)
{
	CreateExtensionStmt *stmt = (CreateExtensionStmt *) utility_stmt;
	bool is_extension = strcmp(stmt->extname, EXTENSION_NAME) == 0;

	if (!is_extension)
		return false;

	if (!loaded)
		return true;

	/*
	 * If the extension exists and the create statement has an IF NOT EXISTS
	 * option, we continue without loading and let CREATE EXTENSION bail out
	 * with a standard NOTICE. We can only do this if the extension actually
	 * exists (is created), or else we might potentially load the shared
	 * library of another version of the extension. Loading typically happens
	 * on CREATE EXTENSION (via CREATE FUNCTION as SQL files are installed)
	 * even if we do not explicitly load the library here. If we load another
	 * version of the library, in addition to the currently loaded version, we
	 * might taint the backend.
	 */
	if (extension_exists() && stmt->if_not_exists)
		return false;

	/* disallow loading two .so from different versions */
	ereport(ERROR,
			(errcode(ERRCODE_DUPLICATE_OBJECT),
			 errmsg("extension \"%s\" has already been loaded with another version", stmt->extname),
			 errdetail("The loaded version is \"%s\".", soversion),
			 errhint("Start a new session and execute CREATE EXTENSION as the first command. "
					 "Make sure to pass the \"-X\" flag to psql.")));
	return false;
}

static bool
should_load_on_drop_extension(Node *utility_stmt)
{
	return !drop_statement_drops_extension((DropStmt *) utility_stmt);
}

static bool
load_utility_cmd(Node *utility_stmt)
{
	switch (nodeTag(utility_stmt))
	{
		case T_VariableSetStmt:
			return should_load_on_variable_set(utility_stmt);
		case T_AlterExtensionStmt:
			return should_load_on_alter_extension(utility_stmt);
		case T_CreateExtensionStmt:
			return should_load_on_create_extension(utility_stmt);
		case T_DropStmt:
			return should_load_on_drop_extension(utility_stmt);
		default:
			return true;
	}
}

static void
stop_workers_on_db_drop(DropdbStmt *drop_db_statement)
{
	/*
	 * Don't check if extension exists here because even though the current
	 * database might not have TimescaleDB installed the database we are
	 * dropping might.
	 */
	Oid dropped_db_oid = get_database_oid(drop_db_statement->dbname, drop_db_statement->missing_ok);

	if (dropped_db_oid != InvalidOid)
	{
		ereport(LOG,
				(errmsg("TimescaleDB background worker scheduler for database %u will be stopped",
						dropped_db_oid)));
		ts_bgw_message_send_and_wait(STOP, dropped_db_oid);
	}
	return;
}

static void
post_analyze_hook(ParseState *pstate, Query *query)
{
	if (query->commandType == CMD_UTILITY)
	{
		switch (nodeTag(query->utilityStmt))
		{
			case T_AlterDatabaseStmt:
			{
				/*
				 * On ALTER DATABASE SET TABLESPACE we need to stop background
				 * workers for the command to succeed.
				 */
				AlterDatabaseStmt *stmt = (AlterDatabaseStmt *) query->utilityStmt;
				if (list_length(stmt->options) == 1)
				{
					DefElem *option = linitial(stmt->options);
					if (option->defname && strcmp(option->defname, "tablespace") == 0)
					{
						Oid db_oid = get_database_oid(stmt->dbname, false);

						if (OidIsValid(db_oid))
						{
							ts_bgw_message_send_and_wait(RESTART, db_oid);
							ereport(WARNING,
									(errmsg("You may need to manually restart any running "
											"background workers after this command.")));
						}
					}
				}
				break;
			}
			case T_CreatedbStmt:
			{
				/*
				 * If we create a database and the database used as template
				 * has background workers we need to stop those background
				 * workers connected to the template database.
				 */
				CreatedbStmt *stmt = (CreatedbStmt *) query->utilityStmt;
				ListCell *lc;

				foreach (lc, stmt->options)
				{
					DefElem *option = lfirst(lc);
					if (option->defname != NULL && option->arg != NULL &&
						strcmp(option->defname, "template") == 0)
					{
						Oid db_oid = get_database_oid(defGetString(option), false);

						if (OidIsValid(db_oid))
							ts_bgw_message_send_and_wait(RESTART, db_oid);
					}
				}
				break;
			}
			case T_DropdbStmt:
				/*
				 * If we drop a database, we need to intercept and stop any of our
				 * schedulers that might be connected to said db.
				 */
				stop_workers_on_db_drop((DropdbStmt *) query->utilityStmt);
				break;
			case T_DropStmt:
				if (drop_statement_drops_extension((DropStmt *) query->utilityStmt))

				/*
				 * if we drop the extension we should restart (in case of
				 * a rollback) the scheduler
				 */
				{
					ts_bgw_message_send_and_wait(RESTART, MyDatabaseId);
				}
				break;
			case T_DropOwnedStmt:
				if (drop_owned_statement_drops_extension((DropOwnedStmt *) query->utilityStmt))
					ts_bgw_message_send_and_wait(RESTART, MyDatabaseId);
				break;
			case T_RenameStmt:
				if (((RenameStmt *) query->utilityStmt)->renameType == OBJECT_DATABASE)
				{
					RenameStmt *stmt = (RenameStmt *) query->utilityStmt;
					Oid db_oid = get_database_oid(stmt->subname, stmt->missing_ok);

					if (OidIsValid(db_oid))
					{
						ts_bgw_message_send_and_wait(STOP, db_oid);
						ereport(WARNING,
								(errmsg("You need to manually restart any running "
										"background workers after this command.")));
					}
				}
				break;
			default:

				break;
		}
	}
	if (!guc_disable_load &&
		(query->commandType != CMD_UTILITY || load_utility_cmd(query->utilityStmt)))
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

static void
timescale_shmem_startup_hook(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();
	ts_bgw_counter_shmem_startup();
	ts_bgw_message_queue_shmem_startup();
	ts_lwlocks_shmem_startup();
}

static void
extension_mark_loader_present()
{
	void **presentptr = find_rendezvous_variable(RENDEZVOUS_LOADER_PRESENT_NAME);

	*presentptr = &loader_present;
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		extension_load_without_preload();
	}
	extension_mark_loader_present();

	elog(INFO, "timescaledb loaded");

	ts_bgw_counter_shmem_alloc();
	ts_bgw_message_queue_alloc();
	ts_lwlocks_shmem_alloc();
	ts_bgw_cluster_launcher_register();
	ts_bgw_counter_setup_gucs();
	ts_bgw_interface_register_api_version();

	/* This is a safety-valve variable to prevent loading the full extension */
	DefineCustomBoolVariable(GUC_DISABLE_LOAD_NAME,
							 "Disable the loading of the actual extension",
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
	/* register shmem startup hook for the background worker stuff */
	prev_shmem_startup_hook = shmem_startup_hook;

	post_parse_analyze_hook = post_analyze_hook;
	shmem_startup_hook = timescale_shmem_startup_hook;
}

void
_PG_fini(void)
{
	post_parse_analyze_hook = prev_post_parse_analyze_hook;
	shmem_startup_hook = prev_shmem_startup_hook;
	/* No way to unregister relcache callback */
}

static void inline do_load()
{
	char *version = extension_version();
	char soname[MAX_SO_NAME_LEN];
	post_parse_analyze_hook_type old_hook;

	StrNCpy(soversion, version, MAX_VERSION_LEN);

	/*
	 * An inval_relcache callback can be called after previous checks of
	 * loaded had found it to be false. But the inval_relcache callback may
	 * load the extension setting it to true. Thus it needs to be rechecked
	 * here again by the outer call after inval_relcache completes. This is
	 * double-check locking, in effect.
	 */
	if (loaded)
		return;

	snprintf(soname, MAX_SO_NAME_LEN, "%s-%s", EXTENSION_SO, version);

	/*
	 * Set to true whether or not the load succeeds to prevent reloading if
	 * failure happened after partial load.
	 */
	loaded = true;

	/*
	 * In a parallel worker, we're not responsible for loading libraries, it's
	 * handled by the parallel worker infrastructure which restores the
	 * library state.
	 */
	if (CalledInParallelWorker())
		return;

	/*
	 * Set the config option to let versions 0.9.0 and 0.9.1 know that the
	 * loader was preloaded, newer versions use rendezvous variables instead.
	 */
	if (strcmp(version, "0.9.0") == 0 || strcmp(version, "0.9.1") == 0)
		SetConfigOption("timescaledb.loader_present", "on", PGC_USERSET, PGC_S_SESSION);

	/*
	 * we need to capture the loaded extension's post analyze hook, giving it
	 * a NULL as previous
	 */
	old_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = NULL;

	/*
	 * We want to call the post_parse_analyze_hook from the versioned
	 * extension after we've loaded the versioned so. When the file is loaded
	 * it sets post_parse_analyze_hook, which we capture and store in
	 * extension_post_parse_analyze_hook to call at the end _PG_init
	 */
	PG_TRY();
	{
		PGFunction ts_post_load_init =
			load_external_function(soname, POST_LOAD_INIT_FN, false, NULL);

		if (ts_post_load_init != NULL)
			DirectFunctionCall1(ts_post_load_init, CharGetDatum(0));
	}
	PG_CATCH();
	{
		extension_post_parse_analyze_hook = post_parse_analyze_hook;
		post_parse_analyze_hook = old_hook;
		PG_RE_THROW();
	}
	PG_END_TRY();

	extension_post_parse_analyze_hook = post_parse_analyze_hook;
	post_parse_analyze_hook = old_hook;
}

static void inline extension_check()
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

extern void
ts_loader_extension_check(void)
{
	extension_check();
}

static void
call_extension_post_parse_analyze_hook(ParseState *pstate, Query *query)
{
	if (loaded && extension_post_parse_analyze_hook != NULL)
	{
		extension_post_parse_analyze_hook(pstate, query);
	}
}
