/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <commands/event_trigger.h>
#include <catalog/pg_trigger.h>
#include <catalog/namespace.h>

#include <guc.h>
#include "hypertable_server.h"
#include "chunk_index.h"
#include "process_utility.h"
#include "event_trigger.h"
#include "remote/dist_commands.h"
#include "remote/dist_ddl.h"
#include "dist_util.h"

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
dist_ddl_error_raise_blocked(void)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("operation is blocked on a distributed hypertable member"),
			 errdetail("This operation should be executed on the frontend server."),
			 errhint("Set timescaledb.enable_client_ddl_on_data_servers to TRUE, if you know what "
					 "you are doing.")));
}

static void
dist_ddl_inspect_hypertable_list(ProcessUtilityArgs *args, Cache *hcache, bool *has_distributed_ht,
								 bool *has_distributed_ht_member)
{
	Hypertable *ht;
	ListCell *lc;

	foreach (lc, args->hypertable_list)
	{
		ht = ts_hypertable_cache_get_entry(hcache, lfirst_oid(lc));
		Assert(ht != NULL);
		switch (ts_hypertable_get_type(ht))
		{
			case HYPERTABLE_REGULAR:
				break;
			case HYPERTABLE_DISTRIBUTED:
				*has_distributed_ht = true;
				break;
			case HYPERTABLE_DISTRIBUTED_MEMBER:
				*has_distributed_ht_member = true;
				break;
		}
	}
}

static HypertableType
dist_ddl_get_hypertable_type_from_state(void)
{
	Cache *hcache;
	Hypertable *ht;
	HypertableType type;

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry(hcache, dist_ddl_state.relid);
	Assert(ht != NULL);
	type = ts_hypertable_get_type(ht);
	ts_cache_release(hcache);
	return type;
}

static void
dist_ddl_check_session(void)
{
	if (dist_util_is_frontend_session())
		return;

	/* Check if this operation is allowed by user */
	if (ts_guc_enable_client_ddl_on_data_servers)
		return;

	dist_ddl_error_raise_blocked();
}

static void
dist_ddl_preprocess(ProcessUtilityArgs *args)
{
	NodeTag tag = nodeTag(args->parsetree);
	int hypertable_list_length = list_length(args->hypertable_list);
	Cache *hcache;
	Hypertable *ht;
	Oid relid = InvalidOid;
	bool has_distributed_ht_member = false;
	bool has_distributed_ht = false;

	/*
	 * This function is executed for any Utility/DDL operation and for any
	 * PostgreSQL tables been involved in the query.
	 *
	 * We are particulary interested in distributed hypertables and distributed
	 * hypertable members (regular hypertables created on a data servers).
	 *
	 * In most cases expect and allow only one distributed hypertable per
	 * operation to avoid query deparsing, but do not restrict the number of
	 * other hypertable types involved.
	 */
	if (hypertable_list_length == 0)
	{
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
	}

	if (hypertable_list_length == 1)
	{
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
	}

	/*
	 * Iterate over hypertable list and reason about the type of hypertables
	 * involved in the query.
	 */
	hcache = ts_hypertable_cache_pin();
	dist_ddl_inspect_hypertable_list(args, hcache, &has_distributed_ht, &has_distributed_ht_member);

	/*
	 * Allow DDL operations on data nodes executed only from frontend connection.
	 */
	if (has_distributed_ht_member)
		dist_ddl_check_session();

	if (!has_distributed_ht)
	{
		ts_cache_release(hcache);
		return;
	}

	/*
	 * Raise an error only if at least one of hypertables is
	 * distributed. Otherwise this makes query_string unusable for remote
	 * execution without deparsing.
	 */
	if (hypertable_list_length > 1)
		dist_ddl_error_raise_unsupported();

	ht = ts_hypertable_cache_get_entry(hcache, relid);
	Assert(ht != NULL);
	Assert(hypertable_is_distributed(ht));

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
	{
		HypertableType type = dist_ddl_get_hypertable_type_from_state();

		if (type == HYPERTABLE_DISTRIBUTED)
			dist_ddl_error_raise_unsupported();

		/* Ensure this operation is executed by frontend session. */
		if (type == HYPERTABLE_DISTRIBUTED_MEMBER)
			dist_ddl_check_session();
	}

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
	FormData_hypertable form;
	bool nulls[Natts_hypertable];
	List *server_list;
	MemoryContext mctx;

	if (!ts_hypertable_get_attributes_by_name(schema, name, &form, nulls))
		return;

	/* If hypertable is marked as distributed member then ensure this operation is
	 * executed by frontend session. */
	if (!nulls[AttrNumberGetAttrOffset(Anum_hypertable_replication_factor)] &&
		form.replication_factor == HYPERTABLE_DISTRIBUTED_MEMBER)
	{
		dist_ddl_check_session();
	}

	server_list = ts_hypertable_server_scan(form.id, CurrentMemoryContext);
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
