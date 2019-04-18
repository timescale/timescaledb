/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <commands/event_trigger.h>
#include <catalog/pg_trigger.h>
#include <catalog/namespace.h>

#include "hypertable_server.h"
#include "chunk_index.h"
#include "process_utility.h"
#include "event_trigger.h"
#include "remote/dist_commands.h"
#include "remote/dist_ddl.h"

/* DDL Query execution type */
typedef enum
{
	/* Do not execute */
	DIST_DDL_EXEC_NONE,
	/* Execute on start hook */
	DIST_DDL_EXEC_ON_START,
	/* Execute on end hook */
	DIST_DDL_EXEC_ON_END
} DistDDLExecType;

/* Per-command DDL query state */
typedef struct
{
	/* Chosen query execution type */
	DistDDLExecType exec_type;
	/* Saved SQL command */
	char *query_string;
	/* Saved oid for delayed resolving */
	Oid relid;
	/* List of servers to send query to */
	List *server_list;
	/* Memory context used for server_list */
	MemoryContext mctx;
} DistDDLState;

static DistDDLState dist_ddl_state;

#define dist_ddl_scheduled_for_execution() (dist_ddl_state.exec_type != DIST_DDL_EXEC_NONE)

void
dist_ddl_init(void)
{
	dist_ddl_state.exec_type = DIST_DDL_EXEC_NONE;
	dist_ddl_state.query_string = NULL;
	dist_ddl_state.relid = InvalidOid;
	dist_ddl_state.server_list = NIL;
	dist_ddl_state.mctx = NULL;
}

void
dist_ddl_reset(void)
{
	dist_ddl_init();
}

static void
dist_ddl_error_raise_unsupported(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("operation not supported on distributed hypertable")));
}

static void
dist_ddl_block_hypertable_list(ProcessUtilityArgs *args)
{
	Cache *hcache;
	Hypertable *ht;
	ListCell *lc;

	hcache = ts_hypertable_cache_pin();

	foreach (lc, args->hypertable_list)
	{
		ht = ts_hypertable_cache_get_entry(hcache, lfirst_oid(lc));
		Assert(ht != NULL);

		if (hypertable_is_distributed(ht))
		{
			ts_cache_release(hcache);
			dist_ddl_error_raise_unsupported();
		}
	}

	ts_cache_release(hcache);
}

static void
dist_ddl_block_hypertable_relid(void)
{
	Cache *hcache;
	bool is_distributed;
	Hypertable *ht;

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry(hcache, dist_ddl_state.relid);
	Assert(ht != NULL);
	is_distributed = hypertable_is_distributed(ht);
	ts_cache_release(hcache);

	if (is_distributed)
		dist_ddl_error_raise_unsupported();
}

static void
dist_ddl_preprocess(ProcessUtilityArgs *args)
{
	NodeTag tag = nodeTag(args->parsetree);
	Cache *hcache;
	Hypertable *ht;
	Oid relid;

	/*
	 * In most cases expect only one distributed hypertable per operation.
	 */
	switch (list_length(args->hypertable_list))
	{
		case 0:
			/*
			 * For DROP TABLE and DROP SCHEMA operations hypertable_list will be
			 * empty. Wait for sql_drop events.
			 */
			if (tag == T_DropStmt)
			{
				DropStmt *stmt = (DropStmt *) args->parsetree;

				if (stmt->removeType == OBJECT_TABLE || stmt->removeType == OBJECT_SCHEMA)
					dist_ddl_state.exec_type = DIST_DDL_EXEC_ON_END;
			}
			return;

		case 1:
			break;

		default:
			/*
			 * Raise an error only if at least one of hypertables is
			 * distributed. Otherwise this makes query_string unusable for remote
			 * execution without deparsing.
			 */
			dist_ddl_block_hypertable_list(args);
			return;
	}

	relid = linitial_oid(args->hypertable_list);
	switch (tag)
	{
		/*
		 * Hypertable oid from those commands is available in hypertable_list but
		 * cannot be resolved here until standard utility hook will synchronize new
		 * relation name and schema.
		 *
		 * Save oid for dist_ddl_end execution.
		 */
		case T_AlterObjectSchemaStmt:
		case T_RenameStmt:
			dist_ddl_state.exec_type = DIST_DDL_EXEC_ON_END;
			dist_ddl_state.relid = relid;
			return;

		/* Skip COPY here, since it has its own process path using
		 * cross module API. */
		case T_CopyStmt:
			return;
		default:
			break;
	}

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry(hcache, relid);
	Assert(ht != NULL);
	if (!hypertable_is_distributed(ht))
	{
		ts_cache_release(hcache);
		return;
	}

	/* Block unsupported operations on distributed hypertables and
	 * decide on how to execute it. */
	switch (tag)
	{
		case T_AlterTableStmt:
		{
			AlterTableStmt *stmt = (AlterTableStmt *) args->parsetree;
			ListCell *lc;

			foreach (lc, stmt->cmds)
			{
				AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

				switch (cmd->subtype)
				{
					case AT_AddColumn:
					case AT_AddColumnRecurse:
					case AT_DropColumn:
					case AT_DropColumnRecurse:
					case AT_AddConstraint:
					case AT_AddConstraintRecurse:
					case AT_DropConstraint:
					case AT_DropConstraintRecurse:
					case AT_AddIndex:
						/* supported commands */
						break;
					default:
						dist_ddl_error_raise_unsupported();
						break;
				}
			}

			dist_ddl_state.exec_type = DIST_DDL_EXEC_ON_END;
			break;
		}
		case T_DropStmt:
			/*
			 * CASCADE operations of DROP TABLE and DROP SCHEMA are handled in
			 * sql_drop trigger.
			 */

			/*
			 * Handle DROP INDEX as an exception, because otherwise information
			 * about the related hypertable will be missed during sql_drop hook
			 * execution. Also expect cascade index drop to be handled in
			 * combination with table or schema drop.
			 */
			Assert(((DropStmt *) args->parsetree)->removeType == OBJECT_INDEX);

			dist_ddl_state.exec_type = DIST_DDL_EXEC_ON_END;
			break;

		case T_IndexStmt:
			/* Since we have custom CREATE INDEX implementation, currently it
			 * does not support ddl_command_end trigger. */
			dist_ddl_state.exec_type = DIST_DDL_EXEC_ON_START;
			break;

		case T_CreateTrigStmt:
			/* Allow CREATE TRIGGER on hypertable locally, but do not send it
			 * to other servers. */
			break;

		/* Currently unsupported */
		case T_TruncateStmt:
		case T_RenameStmt:
		case T_VacuumStmt:
		case T_ReindexStmt:
		case T_ClusterStmt:
			/* Those commands are also targets for execute_on_start in since they
			 * are not supported by event triggers. */
			dist_ddl_state.exec_type = DIST_DDL_EXEC_ON_START;

			/* fall through */
		default:
			dist_ddl_error_raise_unsupported();
			break;
	}

	/*
	 * Get a list of associated servers. This is done here and also
	 * during sql_drop and command_end triggers execution.
	 */
	if (dist_ddl_scheduled_for_execution())
		dist_ddl_state.server_list = ts_hypertable_get_servername_list(ht);

	ts_cache_release(hcache);
}

static void
dist_ddl_execute(void)
{
	DistCmdResult *result;

	/* Execute command on remote servers using local search_path. */
	if (list_length(dist_ddl_state.server_list) > 0)
	{
		result = ts_dist_cmd_invoke_on_servers_using_search_path(dist_ddl_state.query_string,
																 namespace_search_path,
																 dist_ddl_state.server_list);
		if (result)
			ts_dist_cmd_close_response(result);
	}

	dist_ddl_reset();
}

void
dist_ddl_start(ProcessUtilityArgs *args)
{
	/*
	 * Do not process nested DDL operations made by external
	 * event triggers.
	 */
	if (args->context != PROCESS_UTILITY_TOPLEVEL)
		return;

	Assert(dist_ddl_state.query_string == NULL);

	/* Decide if this is a distributed DDL operation and when it
	 * should be executed. */
	dist_ddl_preprocess(args);

	if (dist_ddl_scheduled_for_execution())
	{
		/* Prepare execution state. Save origin query and memory context
		 * used to save information in server_list since it will differ during
		 * event hooks execution. */
		dist_ddl_state.query_string = pstrdup(args->query_string);
		dist_ddl_state.mctx = CurrentMemoryContext;
	}

	if (dist_ddl_state.exec_type == DIST_DDL_EXEC_ON_START)
		dist_ddl_execute();
}

void
dist_ddl_end(EventTriggerData *command)
{
	if (dist_ddl_state.exec_type != DIST_DDL_EXEC_ON_END)
	{
		dist_ddl_reset();
		return;
	}

	/* Do delayed block of SET SCHEMA and RENAME commands.
	 *
	 * In the future those commands might be unblocked and server_list could
	 * be updated here as well.
	 */
	if (OidIsValid(dist_ddl_state.relid))
		dist_ddl_block_hypertable_relid();

	/* Execute command on remote servers. */
	dist_ddl_execute();
}

static bool
dist_ddl_has_server(const char *name)
{
	ListCell *lc;

	foreach (lc, dist_ddl_state.server_list)
	{
		const char *server_name = lfirst(lc);

		if (!strcmp(server_name, name))
			return true;
	}

	return false;
}

static void
dist_ddl_add_server_list(List *server_list)
{
	ListCell *lc;

	foreach (lc, server_list)
	{
		HypertableServer *hypertable_server = lfirst(lc);
		const char *server_name = NameStr(hypertable_server->fd.server_name);

		if (dist_ddl_has_server(server_name))
			continue;

		dist_ddl_state.server_list = lappend(dist_ddl_state.server_list, pstrdup(server_name));
	}
}

static void
dist_ddl_add_server_list_from_table(const char *schema, const char *name)
{
	int32 hypertable_id;
	List *server_list;
	MemoryContext mctx;

	hypertable_id = ts_hypertable_get_id_by_name(schema, name);
	if (hypertable_id == -1)
		return;

	server_list = ts_hypertable_server_scan(hypertable_id, CurrentMemoryContext);
	if (server_list == NIL)
		return;

	mctx = MemoryContextSwitchTo(dist_ddl_state.mctx);
	dist_ddl_add_server_list(server_list);
	MemoryContextSwitchTo(mctx);

	list_free(server_list);
}

void
dist_ddl_drop(List *dropped_objects)
{
	ListCell *lc;

	if (!dist_ddl_scheduled_for_execution())
		return;

	foreach (lc, dropped_objects)
	{
		EventTriggerDropObject *obj = lfirst(lc);

		switch (obj->type)
		{
			case EVENT_TRIGGER_DROP_FOREIGN_TABLE:
			case EVENT_TRIGGER_DROP_TABLE:
			{
				EventTriggerDropRelation *event = (EventTriggerDropRelation *) obj;

				dist_ddl_add_server_list_from_table(event->schema, event->name);
				break;
			}

			case EVENT_TRIGGER_DROP_INDEX:
			{
				/*
				 * Skip here, bacause we expect CASCADE case to be handled in
				 * combination with DROP TABLE.
				 */
				break;
			}

			case EVENT_TRIGGER_DROP_TABLE_CONSTRAINT:
			{
				EventTriggerDropTableConstraint *event = (EventTriggerDropTableConstraint *) obj;

				dist_ddl_add_server_list_from_table(event->schema, event->table);
				break;
			}

			case EVENT_TRIGGER_DROP_TRIGGER:
			case EVENT_TRIGGER_DROP_FOREIGN_SERVER:
			case EVENT_TRIGGER_DROP_SCHEMA:
			case EVENT_TRIGGER_DROP_VIEW:
				/* Skip */
				break;
		}
	}
}
