/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <commands/event_trigger.h>
#include <utils/guc.h>
#include <catalog/pg_trigger.h>
#include <catalog/namespace.h>
#include <nodes/parsenodes.h>

#include <annotations.h>
#include <guc.h>
#include "hypertable_data_node.h"
#include "chunk_index.h"
#include "chunk_api.h"
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
	/* Execute on start hook without using a transactions */
	DIST_DDL_EXEC_ON_START_NO_2PC,
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
	/* List of data nodes to send query to */
	List *data_node_list;
	/* Memory context used for data_node_list */
	MemoryContext mctx;
} DistDDLState;

static DistDDLState dist_ddl_state;

#define dist_ddl_scheduled_for_execution() (dist_ddl_state.exec_type != DIST_DDL_EXEC_NONE)

/*
 * Set the exec type for a distributed command, i.e., whether to forward the
 * DDL statement before or after PostgreSQL has processed it locally.
 *
 * In multi-command statements (e.g., ALTER), it should not be possible to
 * have a mix of sub-commands that require both START and END processing. Such
 * mixing would require splitting the original ALTER across both START and END
 * processing, which would prevent simply forwarding the original statement to
 * the data nodes. For instance, consider:
 *
 * ALTER TABLE foo SET (newoption = true), ADD CONSTRAINT mycheck CHECK (count > 0);
 *
 * which contains two sub-commands (SET and ADD CONSTRAINT), where the first
 * command (SET) is handled at START, while the latter is handled at
 * END. While we could always distribute commands at START, this would prevent
 * local validation by PostgreSQL.
 */
static void
set_dist_exec_type(DistDDLExecType type)
{
	if (dist_ddl_state.exec_type != DIST_DDL_EXEC_NONE && dist_ddl_state.exec_type != type)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("incompatible sub-commands in single statement"),
				 errdetail("The statement contains sub-commands that require different "
						   "handling to distribute to data nodes and can therefore not "
						   "be mixed in a single statement."),
				 errhint("Try executing the sub-commands in separate statements.")));

	dist_ddl_state.exec_type = type;
}

void
dist_ddl_init(void)
{
	dist_ddl_state.exec_type = DIST_DDL_EXEC_NONE;
	dist_ddl_state.query_string = NULL;
	dist_ddl_state.relid = InvalidOid;
	dist_ddl_state.data_node_list = NIL;
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
			 errdetail("This operation should be executed on the access node."),
			 errhint("Set timescaledb.enable_client_ddl_on_data_nodes to TRUE, if you know what "
					 "you are doing.")));
}

static void
dist_ddl_inspect_hypertable_list(ProcessUtilityArgs *args, Cache *hcache,
								 unsigned int *num_hypertables, unsigned int *num_dist_hypertables,
								 unsigned int *num_dist_hypertable_members)
{
	Hypertable *ht;
	ListCell *lc;

	if (NULL != num_hypertables)
		*num_hypertables = 0;

	if (NULL != num_dist_hypertables)
		*num_dist_hypertables = 0;

	if (NULL != num_dist_hypertable_members)
		*num_dist_hypertable_members = 0;

	foreach (lc, args->hypertable_list)
	{
		ht = ts_hypertable_cache_get_entry(hcache, lfirst_oid(lc), CACHE_FLAG_NONE);
		Assert(ht != NULL);

		switch (ts_hypertable_get_type(ht))
		{
			case HYPERTABLE_REGULAR:
				if (NULL != num_hypertables)
					(*num_hypertables)++;
				break;
			case HYPERTABLE_DISTRIBUTED:
				if (NULL != num_dist_hypertables)
					(*num_dist_hypertables)++;
				break;
			case HYPERTABLE_DISTRIBUTED_MEMBER:
				if (NULL != num_dist_hypertable_members)
					(*num_dist_hypertable_members)++;
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
	ht = ts_hypertable_cache_get_entry(hcache, dist_ddl_state.relid, CACHE_FLAG_NONE);
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
	if (ts_guc_enable_client_ddl_on_data_nodes)
		return;

	dist_ddl_error_raise_blocked();
}

/*
 * Process distributed VACUUM/ANALYZE.
 */
static DistDDLExecType
dist_ddl_process_vacuum(VacuumStmt *stmt)
{
	/* We do not support VERBOSE flag since it will require to print data
	 * returned from the data nodes */
	if (get_vacuum_options(stmt) & VACOPT_VERBOSE)
		dist_ddl_error_raise_unsupported();

	return DIST_DDL_EXEC_ON_START_NO_2PC;
}

static void
dist_ddl_preprocess(ProcessUtilityArgs *args)
{
	NodeTag tag = nodeTag(args->parsetree);
	int hypertable_list_length = list_length(args->hypertable_list);
	Cache *hcache;
	Hypertable *ht;
	Oid relid = InvalidOid;
	unsigned int num_hypertables;
	unsigned int num_dist_hypertables;
	unsigned int num_dist_hypertable_members;

	/*
	 * This function is executed for any Utility/DDL operation and for any
	 * PostgreSQL tables been involved in the query.
	 *
	 * We are particulary interested in distributed hypertables and distributed
	 * hypertable members (regular hypertables created on a data nodes).
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
			DropStmt *stmt = castNode(DropStmt, args->parsetree);

			if (stmt->removeType == OBJECT_TABLE || stmt->removeType == OBJECT_SCHEMA)
				set_dist_exec_type(DIST_DDL_EXEC_ON_END);
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
				set_dist_exec_type(DIST_DDL_EXEC_ON_END);
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
	dist_ddl_inspect_hypertable_list(args,
									 hcache,
									 &num_hypertables,
									 &num_dist_hypertables,
									 &num_dist_hypertable_members);
	/*
	 * Allow DDL operations on data nodes executed only from frontend connection.
	 */
	if (num_dist_hypertable_members > 0)
		dist_ddl_check_session();

	if (num_dist_hypertables == 0)
	{
		ts_cache_release(hcache);
		return;
	}

	/*
	 * Raise an error only if at least one of hypertables is
	 * distributed. Otherwise this makes query_string unusable for remote
	 * execution without deparsing.
	 *
	 * TODO: Support multiple tables inside statements.
	 */
	if (hypertable_list_length > 1)
		dist_ddl_error_raise_unsupported();

	ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_NONE);
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
						set_dist_exec_type(DIST_DDL_EXEC_ON_END);
						break;
					case AT_ReplaceRelOptions:
					case AT_ResetRelOptions:
					case AT_SetRelOptions:
					case AT_DropOids:
						/* Custom TimescaleDB options (e.g.,
						 * compression-related options) are not recognized by
						 * PostgreSQL and thus cannot mix with other (PG)
						 * options. As a consequence, custom reloptions are
						 * not forwarded/handled by PostgreSQL and thus never
						 * reach END processing. Therefore, to distributed
						 * SetRelOptions to other nodes, it needs to happen at
						 * START. */
						set_dist_exec_type(DIST_DDL_EXEC_ON_START);
						break;
					default:
						dist_ddl_error_raise_unsupported();
						break;
				}
			}

			break;
		}
		case T_DropStmt:
		{
			DropStmt *stmt = castNode(DropStmt, args->parsetree);
			/*
			 * CASCADE operations of DROP TABLE and DROP SCHEMA are handled in
			 * sql_drop trigger.
			 */
			switch (stmt->removeType)
			{
				case OBJECT_INDEX:
					/*
					 * Need to handle drop index here because otherwise
					 * information about the related hypertable will be missed
					 * during sql_drop hook execution. Also expect cascade index
					 * drop to be handled in combination with table or schema
					 * drop.
					 */
					set_dist_exec_type(DIST_DDL_EXEC_ON_END);
					break;
				case OBJECT_TRIGGER:
					set_dist_exec_type(DIST_DDL_EXEC_ON_START);
					break;
				default:
					/* Only the object types above are forwarded from
					 * process_utility processing */
					Assert(false);
					break;
			}
			break;
		}
		case T_IndexStmt:
			/* Since we have custom CREATE INDEX implementation, currently it
			 * does not support ddl_command_end trigger. */
			set_dist_exec_type(DIST_DDL_EXEC_ON_START);
			break;

		case T_CreateTrigStmt:
			set_dist_exec_type(DIST_DDL_EXEC_ON_START);
			break;

		case T_VacuumStmt:
			dist_ddl_state.exec_type =
				dist_ddl_process_vacuum(castNode(VacuumStmt, args->parsetree));
			break;

		case T_GrantStmt:
			/* If there is one or more distributed hypertables, we need to do a 2PC. */
			if (num_dist_hypertables > 0)
				set_dist_exec_type(DIST_DDL_EXEC_ON_START);
			break;
		case T_TruncateStmt:
		{
			TruncateStmt *stmt = (TruncateStmt *) args->parsetree;
			/* Calculate number of regular tables. Note that distributed
			 * hypertables are filtered from the relations list in
			 * process_utility.c, so the list might actually be empty. */
			unsigned int num_regular_tables =
				list_length(stmt->relations) - num_hypertables - num_dist_hypertable_members;

			Assert(num_regular_tables <= list_length(stmt->relations));

			/* We only support TRUNCATE on single distributed hypertables
			 * since other tables might not exist on data nodes and multiple
			 * distributed hypertables might be distributed across different
			 * sets of nodes. */
			if (num_dist_hypertables == 1 && num_regular_tables == 0 && num_hypertables == 0 &&
				num_dist_hypertable_members == 0)
				set_dist_exec_type(DIST_DDL_EXEC_ON_START);
			else
				dist_ddl_error_raise_unsupported();
			break;
		}

		case T_ReindexStmt:
			set_dist_exec_type(DIST_DDL_EXEC_ON_START);
			break;

		/* Currently unsupported */
		case T_RenameStmt:
		case T_ClusterStmt:
			/* Those commands are also targets for execute_on_start in since they
			 * are not supported by event triggers. */
			set_dist_exec_type(DIST_DDL_EXEC_ON_START);
			TS_FALLTHROUGH;
		default:
			dist_ddl_error_raise_unsupported();
			break;
	}

	/*
	 * Get a list of associated data nodes. This is done here and also
	 * during sql_drop and command_end triggers execution.
	 */
	if (dist_ddl_scheduled_for_execution())
		dist_ddl_state.data_node_list = ts_hypertable_get_data_node_name_list(ht);

	ts_cache_release(hcache);
}

static void
dist_ddl_execute(bool transactional)
{
	DistCmdResult *result;

	/* Execute command on data nodes using local search_path. */
	if (list_length(dist_ddl_state.data_node_list) > 0)
	{
		const char *search_path = GetConfigOption("search_path", false, false);

		result = ts_dist_cmd_invoke_on_data_nodes_using_search_path(dist_ddl_state.query_string,
																	search_path,
																	dist_ddl_state.data_node_list,
																	transactional);

		if (result)
			ts_dist_cmd_close_response(result);
	}

	dist_ddl_reset();
}

/* Update stats after ANALYZE execution */
static void
dist_ddl_get_analyze_stats(ProcessUtilityArgs *args, VacuumStmt *stmt)
{
	Oid relid = linitial_oid(args->hypertable_list);

	if (!(get_vacuum_options(stmt) & VACOPT_ANALYZE))
		return;

	/* Refresh the local chunk stats by fetching stats
	 * from remote data nodes. */
	chunk_api_update_distributed_hypertable_stats(relid);
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
		 * used to save information in data_node_list since it will differ during
		 * event hooks execution. */
		dist_ddl_state.query_string = pstrdup(args->query_string);
		dist_ddl_state.mctx = CurrentMemoryContext;
	}

	switch (dist_ddl_state.exec_type)
	{
		case DIST_DDL_EXEC_ON_START:
			dist_ddl_execute(true);
			break;
		case DIST_DDL_EXEC_ON_START_NO_2PC:
			dist_ddl_execute(false);
			/* Import distributed hypertable stats after executing
			 * ANALYZE command. */
			if (IsA(args->parsetree, VacuumStmt))
				dist_ddl_get_analyze_stats(args, castNode(VacuumStmt, args->parsetree));
			break;
		case DIST_DDL_EXEC_ON_END:
		case DIST_DDL_EXEC_NONE:
			break;
	}
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
	 * In the future those commands might be unblocked and data_node_list could
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

	/* Execute command on remote data nodes. */
	dist_ddl_execute(true);
}

static bool
dist_ddl_has_data_node(const char *name)
{
	ListCell *lc;

	foreach (lc, dist_ddl_state.data_node_list)
	{
		const char *data_node_name = lfirst(lc);

		if (!strcmp(data_node_name, name))
			return true;
	}

	return false;
}

static void
dist_ddl_add_data_node_list(List *data_node_list)
{
	ListCell *lc;

	foreach (lc, data_node_list)
	{
		HypertableDataNode *hypertable_data_node = lfirst(lc);
		const char *node_name = NameStr(hypertable_data_node->fd.node_name);

		if (dist_ddl_has_data_node(node_name))
			continue;

		dist_ddl_state.data_node_list = lappend(dist_ddl_state.data_node_list, pstrdup(node_name));
	}
}

static void
dist_ddl_add_data_node_list_from_table(const char *schema, const char *name)
{
	FormData_hypertable form;
	List *data_node_list;
	MemoryContext mctx;

	if (!ts_hypertable_get_attributes_by_name(schema, name, &form))
		return;

	/* If hypertable is marked as distributed member then ensure this operation is
	 * executed by frontend session. */
	if (form.replication_factor == HYPERTABLE_DISTRIBUTED_MEMBER)
	{
		dist_ddl_check_session();
	}

	data_node_list = ts_hypertable_data_node_scan(form.id, CurrentMemoryContext);
	if (data_node_list == NIL)
		return;

	mctx = MemoryContextSwitchTo(dist_ddl_state.mctx);
	dist_ddl_add_data_node_list(data_node_list);
	MemoryContextSwitchTo(mctx);

	list_free(data_node_list);
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

				dist_ddl_add_data_node_list_from_table(event->schema, event->name);
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

				dist_ddl_add_data_node_list_from_table(event->schema, event->table);
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
