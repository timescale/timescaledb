/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <miscadmin.h>
#include <catalog/pg_trigger.h>
#include <catalog/namespace.h>
#include <commands/event_trigger.h>
#include <commands/dbcommands.h>
#include <commands/extension.h>
#include <nodes/parsenodes.h>
#include <parser/parse_func.h>
#include <utils/builtins.h>
#include <utils/fmgrprotos.h>
#include <utils/guc.h>

#include <annotations.h>
#include <guc.h>
#include "compat/compat.h"
#include "data_node.h"
#include "ts_catalog/hypertable_data_node.h"
#include "chunk_index.h"
#include "chunk_api.h"
#include "process_utility.h"
#include "event_trigger.h"
#include "remote/dist_commands.h"
#include "remote/dist_ddl.h"
#include "remote/connection_cache.h"
#include "scan_iterator.h"
#include "dist_util.h"
#include "deparse.h"

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
	/* Saved SQL commands to execute on the remote data nodes */
	List *remote_commands;
	/* Saved oid for delayed resolving */
	Oid relid;
	/* List of data nodes to send query to */
	List *data_node_list;
	/* Memory context used for data_node_list */
	MemoryContext mctx;
} DistDDLState;

static DistDDLState dist_ddl_state;

#define dist_ddl_scheduled_for_execution() (dist_ddl_state.exec_type != DIST_DDL_EXEC_NONE)

void
dist_ddl_state_init(void)
{
	MemSet(&dist_ddl_state, 0, sizeof(DistDDLState));
	dist_ddl_state.exec_type = DIST_DDL_EXEC_NONE;
	dist_ddl_state.remote_commands = NIL;
	dist_ddl_state.relid = InvalidOid;
	dist_ddl_state.data_node_list = NIL;
	dist_ddl_state.mctx = NULL;
}

void
dist_ddl_state_reset(void)
{
	dist_ddl_state_init();
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

/*
 * Ensure that DDL operation is executed from the access node session on
 * data node or explicitly allowed by the guc.
 */
static void
dist_ddl_error_if_not_allowed_data_node_session(void)
{
	if (dist_util_is_access_node_session_on_data_node())
		return;

	if (ts_guc_enable_client_ddl_on_data_nodes)
		return;

	dist_ddl_error_raise_blocked();
}

static void
dist_ddl_state_add_remote_command(const char *cmd)
{
	MemoryContext mctx = MemoryContextSwitchTo(dist_ddl_state.mctx);
	dist_ddl_state.remote_commands =
		lappend(dist_ddl_state.remote_commands, makeString(pstrdup(cmd)));
	MemoryContextSwitchTo(mctx);
}

static void
dist_ddl_state_add_remote_command_list(List *list)
{
	MemoryContext mctx = MemoryContextSwitchTo(dist_ddl_state.mctx);
	dist_ddl_state.remote_commands = lappend(dist_ddl_state.remote_commands, list);
	MemoryContextSwitchTo(mctx);
}

/*
 * Set the exec type for a distributed command, i.e., whether to forward the
 * DDL statement before or after PostgreSQL has processed it locally.
 *
 * There are two ways to execute distributed DDL query: START and END.
 * START executes inside the process utility hook right after the processing is done.
 * END is execution is delayed, and done from ddl_command_start hook invokation.
 */
static inline void
dist_ddl_state_set_exec_type(DistDDLExecType type)
{
	Assert(dist_ddl_state.exec_type == DIST_DDL_EXEC_NONE);
	dist_ddl_state.exec_type = type;
}

/*
 * Set an execution type and add current query string as the command for
 * execution on data nodes.
 */
static inline void
dist_ddl_state_schedule(DistDDLExecType type, const ProcessUtilityArgs *args)
{
	Assert(dist_ddl_state.exec_type == DIST_DDL_EXEC_NONE);
	Assert(type != DIST_DDL_EXEC_NONE);
	dist_ddl_state.exec_type = type;
	dist_ddl_state_add_remote_command(args->query_string);
}

static bool
dist_ddl_state_has_data_node(const char *name)
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
dist_ddl_state_add_data_node_list(List *data_node_list)
{
	ListCell *lc;

	foreach (lc, data_node_list)
	{
		HypertableDataNode *hypertable_data_node = lfirst(lc);
		const char *node_name = NameStr(hypertable_data_node->fd.node_name);

		if (dist_ddl_state_has_data_node(node_name))
			continue;

		dist_ddl_state.data_node_list = lappend(dist_ddl_state.data_node_list, pstrdup(node_name));
	}
}

static void
dist_ddl_state_add_data_node_list_from_table(const char *schema, const char *name)
{
	FormData_hypertable form;
	List *data_node_list;
	MemoryContext mctx;

	if (!ts_hypertable_get_attributes_by_name(schema, name, &form))
		return;

	/* If hypertable is marked as distributed member then ensure this operation is
	 * executed by the access node session. */
	if (form.replication_factor == HYPERTABLE_DISTRIBUTED_MEMBER)
		dist_ddl_error_if_not_allowed_data_node_session();

	data_node_list = ts_hypertable_data_node_scan(form.id, CurrentMemoryContext);
	if (data_node_list == NIL)
		return;

	mctx = MemoryContextSwitchTo(dist_ddl_state.mctx);
	dist_ddl_state_add_data_node_list(data_node_list);
	MemoryContextSwitchTo(mctx);

	list_free(data_node_list);
}

static void
dist_ddl_state_add_data_node_list_from_ht(Hypertable *ht)
{
	dist_ddl_state.data_node_list = ts_hypertable_get_data_node_name_list(ht);
}

static void
dist_ddl_state_add_current_data_node_list(void)
{
	dist_ddl_state.data_node_list = data_node_get_node_name_list();
}

static void
dist_ddl_inspect_hypertable_list(const ProcessUtilityArgs *args, Cache *hcache,
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

/*
 * Validate hypertable list and set a distributed hypertable, update
 * the data node list accordingly.
 *
 * Return false, if no distributed hypertables found.
 */
static bool
dist_ddl_state_set_hypertable(const ProcessUtilityArgs *args)
{
	unsigned int num_hypertables = list_length(args->hypertable_list);
	unsigned int num_dist_hypertables;
	unsigned int num_dist_hypertable_members;
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht;

	dist_ddl_inspect_hypertable_list(args,
									 hcache,
									 NULL,
									 &num_dist_hypertables,
									 &num_dist_hypertable_members);

	/*
	 * Allow DDL operations on data nodes executed only from connection
	 * made by the access node.
	 */
	if (num_dist_hypertable_members > 0)
		dist_ddl_error_if_not_allowed_data_node_session();

	if (num_dist_hypertables == 0)
	{
		ts_cache_release(hcache);
		return false;
	}

	/*
	 * Raise an error only if at least one of hypertables is
	 * distributed. Otherwise this makes query_string unusable for remote
	 * execution without deparsing.
	 */
	if (num_hypertables > 1)
		dist_ddl_error_raise_unsupported();

	/* Get the distributed hypertable */
	ht =
		ts_hypertable_cache_get_entry(hcache, linitial_oid(args->hypertable_list), CACHE_FLAG_NONE);
	Assert(ht != NULL);
	Assert(hypertable_is_distributed(ht));

	/*
	 * Get a list of associated data nodes. This is done here and also can be done later
	 * during sql_drop and command_end triggers execution.
	 */
	dist_ddl_state_add_data_node_list_from_ht(ht);
	ts_cache_release(hcache);
	return true;
}

static HypertableType
dist_ddl_state_get_hypertable_type(void)
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
dist_ddl_process_create_schema(const ProcessUtilityArgs *args)
{
	dist_ddl_state_add_current_data_node_list();
	dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
}

static void
dist_ddl_process_drop_role(const ProcessUtilityArgs *args)
{
	DropRoleStmt *stmt = castNode(DropRoleStmt, args->parsetree);
	ListCell *lc;

	foreach (lc, stmt->roles)
	{
		RoleSpec *rolspec = lfirst_node(RoleSpec, lc);
		remote_connection_cache_dropped_role_callback(rolspec->rolename);
	}
}

static void
dist_ddl_process_drop(const ProcessUtilityArgs *args)
{
	DropStmt *stmt = castNode(DropStmt, args->parsetree);

	/* For DROP TABLE and DROP SCHEMA operations hypertable_list will be empty */
	if (list_length(args->hypertable_list) == 0)
	{
		switch (stmt->removeType)
		{
			case OBJECT_TABLE:
				/* Wait for further sql_drop events  */
				dist_ddl_state_schedule(DIST_DDL_EXEC_ON_END, args);
				break;
			case OBJECT_SCHEMA:
				/* Forward DROP SCHEMA command to all data nodes, following
				 * sql_drop events will be ignored */
				dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
				dist_ddl_state_add_current_data_node_list();
				break;
			default:
				break;
		}
		return;
	}

	if (!dist_ddl_state_set_hypertable(args))
		return;

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
			dist_ddl_state_schedule(DIST_DDL_EXEC_ON_END, args);
			break;

		case OBJECT_TRIGGER:
			dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
			break;

		default:
			/* Only the object types above are forwarded from
			 * process_utility processing */
			abort();
			pg_unreachable();
			break;
	}
}

static void
dist_ddl_process_alter_object_schema(const ProcessUtilityArgs *args)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, args->parsetree);

	if (stmt->objectType == OBJECT_TABLE)
	{
		/* ALTER object SET SCHEMA */
		if (list_length(args->hypertable_list) != 1)
			return;

		/*
		 * Hypertable oid is available in hypertable_list but
		 * cannot be resolved here until standard utility hook will synchronize new
		 * relation name and schema.
		 *
		 * Save oid for dist_ddl_end execution.
		 */
		dist_ddl_state.relid = linitial_oid(args->hypertable_list);
		dist_ddl_state_schedule(DIST_DDL_EXEC_ON_END, args);
	}
}

static void
dist_ddl_process_alter_owner_stmt(const ProcessUtilityArgs *args)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, args->parsetree);

	if (stmt->objectType == OBJECT_SCHEMA)
	{
		/* ALTER SCHEMA OWNER TO */
		dist_ddl_state_add_current_data_node_list();
		dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
	}
}

static void
dist_ddl_process_rename(const ProcessUtilityArgs *args)
{
	RenameStmt *stmt = castNode(RenameStmt, args->parsetree);

	switch (stmt->renameType)
	{
		case OBJECT_SCHEMA:

			/* ALTER SCHEMA RENAME TO */
			dist_ddl_state_add_current_data_node_list();
			dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
			break;

		case OBJECT_TABLE:

			/* ALTER TABLE RENAME TO */
			if (list_length(args->hypertable_list) != 1)
				break;
			/*
			 * Hypertable oid is available in hypertable_list but
			 * cannot be resolved here until standard utility hook will synchronize new
			 * relation name and schema.
			 *
			 * Save oid for dist_ddl_end execution.
			 */
			dist_ddl_state.relid = linitial_oid(args->hypertable_list);
			dist_ddl_state_schedule(DIST_DDL_EXEC_ON_END, args);
			break;

		default:
			/* Only handles other renamings here (e.g., triggers) */
			if (dist_ddl_state_set_hypertable(args))
				dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
			break;
	}
}

static void
dist_ddl_process_grant_on_database(const GrantStmt *stmt)
{
	const char *dbname;
	bool dbmatch;
	List *cmd_descriptors = NIL;
	ListCell *i, *j;

	if (dist_util_membership() != DIST_MEMBER_ACCESS_NODE)
		return;

	/*
	 * Check if the GRANT statement targets the current database.
	 *
	 * If the current database being used and it is distributed we
	 * rewrite and execute GRANT ON DATABASE statement for each data
	 * node specifically.
	 *
	 * If there are multiple databases in the list, we can't determine
	 * whether they are disributed or not, so we prevent this
	 * operation.
	 */
	dbname = get_database_name(MyDatabaseId);
	dbmatch = false;
	foreach (i, stmt->objects)
	{
		if (!strcmp(dbname, strVal(lfirst(i))))
		{
			dbmatch = true;
			break;
		}
	}

	if (!dbmatch)
		return;

	/* Prevent using and mixing several databases in the statement */
	if (list_length(stmt->objects) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change privileges on multiple databases"),
				 errdetail("It is not possible to grant privileges on multiple "
						   "databases in a single statement when one of them is a distributed "
						   "database.")));

	/* Rewrite GRANT/REVOKE ON DATABASE query per each data node, since
	 * each data node might have different database name */
	dist_ddl_state_add_current_data_node_list();

	foreach (i, dist_ddl_state.data_node_list)
	{
		const char *data_node_name = lfirst(i);
		const char *data_node_dbname = NULL;
		ForeignServer *server;
		DistCmdDescr *cmd_desc;

		/* Get data node database name from foreign server options */
		server = GetForeignServerByName(data_node_name, false);
		foreach (j, server->options)
		{
			DefElem *opt = (DefElem *) lfirst(j);
			if (!strcmp(opt->defname, "dbname"))
			{
				data_node_dbname = defGetString(opt);
				break;
			}
		}

		/* Create a query using data node's database name */
		Assert(data_node_dbname != NULL);
		cmd_desc = palloc0(sizeof(DistCmdDescr));
		cmd_desc->sql = deparse_grant_revoke_on_database(stmt, data_node_dbname);
		cmd_descriptors = lappend(cmd_descriptors, cmd_desc);

		elog(DEBUG1, "[%s]: %s", data_node_name, cmd_desc->sql);
	}

	dist_ddl_state_set_exec_type(DIST_DDL_EXEC_ON_START);
	dist_ddl_state_add_remote_command_list(cmd_descriptors);
}

static void
dist_ddl_process_grant_on_tables_in_schema(const ProcessUtilityArgs *args)
{
	GrantStmt *stmt = castNode(GrantStmt, args->parsetree);
	bool exec_on_datanodes = false;
	ListCell *cell;

	/*
	 * Check if there are any distributed hypertables in the schemas.
	 * Otherwise no need to execute the query on the datanodes
	 *
	 * If more than one schemas are specified, then we ship the query
	 * if any of the schemas contain distributed hypertables. It will
	 * be the onus of the user to ensure that all schemas exist on the
	 * datanodes as required. They can always call on an individual
	 * schema one by one in that case.
	 */
	foreach (cell, stmt->objects)
	{
		char *nspname = strVal(lfirst(cell));

		LookupExplicitNamespace(nspname, false);

		ScanIterator iterator =
			ts_scan_iterator_create(HYPERTABLE, AccessShareLock, CurrentMemoryContext);
		ts_hypertable_scan_by_name(&iterator, nspname, NULL);
		ts_scanner_foreach(&iterator)
		{
			FormData_hypertable fd;

			TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
			ts_hypertable_formdata_fill(&fd, ti);

			if (fd.replication_factor > 0)
			{
				exec_on_datanodes = true;
				break;
			}
		}
		ts_scan_iterator_close(&iterator);

		if (exec_on_datanodes)
			break;
	}

	if (exec_on_datanodes)
	{
		dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
		dist_ddl_state_add_current_data_node_list();
	}
}

static void
dist_ddl_process_grant_on_table(const ProcessUtilityArgs *args)
{
	if (!dist_ddl_state_set_hypertable(args))
		return;

	dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
}

static void
dist_ddl_process_grant_on_schema(const ProcessUtilityArgs *args)
{
	dist_ddl_state_add_current_data_node_list();
	dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
}

static void
dist_ddl_process_grant_object(const ProcessUtilityArgs *args)
{
	const GrantStmt *stmt = castNode(GrantStmt, args->parsetree);

	switch (stmt->objtype)
	{
		case OBJECT_DATABASE:
			dist_ddl_process_grant_on_database(stmt);
			break;
		case OBJECT_TABLE:
			dist_ddl_process_grant_on_table(args);
			break;
		case OBJECT_SCHEMA:
			dist_ddl_process_grant_on_schema(args);
			break;
		default:
			break;
	}
}

static void
dist_ddl_process_grant_all_in_schema(const ProcessUtilityArgs *args)
{
	const GrantStmt *stmt = castNode(GrantStmt, args->parsetree);

	switch (stmt->objtype)
	{
		case OBJECT_TABLE:
			dist_ddl_process_grant_on_tables_in_schema(args);
			break;
		default:
			break;
	}
}

static void
dist_ddl_process_grant(const ProcessUtilityArgs *args)
{
	const GrantStmt *stmt = castNode(GrantStmt, args->parsetree);

	switch (stmt->targtype)
	{
		case ACL_TARGET_OBJECT:
			dist_ddl_process_grant_object(args);
			break;
		case ACL_TARGET_ALL_IN_SCHEMA:
			dist_ddl_process_grant_all_in_schema(args);
			break;
		case ACL_TARGET_DEFAULTS:
			/* Not handled on a per-grant statement. The entire ALTER DEFAULT
			 * PRIVILEGES is forwarded and handled below. */
			break;
	}
}

/*
 * "DROP OWNED BY" and "REASSIGN OWNED BY" commands need to be shipped to the
 * datanodes as is.
 */
static void
dist_ddl_process_drop_reassign(const ProcessUtilityArgs *args)
{
	dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
	dist_ddl_state_add_current_data_node_list();
}

/*
 * Alter default privileges.
 *
 * Commands for altering default privileges are sent as is to data nodes. It
 * is assumed that any roles or schemas involved exist also on the data nodes.
 */
static void
dist_ddl_process_alter_default_privileges(const ProcessUtilityArgs *args)
{
	dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
	dist_ddl_state_add_current_data_node_list();
}

/*
 * In multi-command statements (e.g., ALTER), it should not be possible to
 * have a mix of sub-commands that require both START and END processing.
 * Such mixing would require splitting the original ALTER across both START and END
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
static inline DistDDLExecType
set_alter_table_exec_type(DistDDLExecType prev_type, DistDDLExecType type)
{
	if (prev_type != DIST_DDL_EXEC_NONE && prev_type != type)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("incompatible sub-commands in single statement"),
				 errdetail("The statement contains sub-commands that require different "
						   "handling to distribute to data nodes and can therefore not "
						   "be mixed in a single statement."),
				 errhint("Try executing the sub-commands in separate statements.")));
	return type;
}

static void
dist_ddl_process_alter_table(const ProcessUtilityArgs *args)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, args->parsetree);
	DistDDLExecType exec_type = DIST_DDL_EXEC_NONE;
	ListCell *lc;

	if (!dist_ddl_state_set_hypertable(args))
		return;

	foreach (lc, stmt->cmds)
	{
		AlterTableCmd *cmd = lfirst_node(AlterTableCmd, lc);

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
			case AT_SetNotNull:
			case AT_DropNotNull:
			case AT_AddIndex:
			case AT_AlterColumnType:
				exec_type = set_alter_table_exec_type(exec_type, DIST_DDL_EXEC_ON_END);
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
				exec_type = set_alter_table_exec_type(exec_type, DIST_DDL_EXEC_ON_START);
				break;
			default:
				dist_ddl_error_raise_unsupported();
				break;
		}
	}

	dist_ddl_state_schedule(exec_type, args);
}

static void
dist_ddl_process_index(const ProcessUtilityArgs *args)
{
	if (!dist_ddl_state_set_hypertable(args))
		return;

	/* Since we have custom CREATE INDEX implementation, currently it
	 * does not support ddl_command_end trigger. */
	dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
}

static void
dist_ddl_process_reindex(const ProcessUtilityArgs *args)
{
	if (!dist_ddl_state_set_hypertable(args))
		return;

	dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
}

static void
dist_ddl_process_create_trigger(const ProcessUtilityArgs *args)
{
	CreateTrigStmt *stmt = castNode(CreateTrigStmt, args->parsetree);

	if (!dist_ddl_state_set_hypertable(args))
		return;

	if (stmt->funcname != NIL)
	{
		Oid funcargtypes[1];
		Oid funcid = LookupFuncName(stmt->funcname,
									0 /* nargs */,
									funcargtypes /* passing NULL is not allowed in PG12 */,
									false /* missing ok */);
		Datum datum;

		Assert(OidIsValid(funcid));
		datum = DirectFunctionCall1(pg_get_functiondef, ObjectIdGetDatum(funcid));
		dist_ddl_state_add_remote_command(TextDatumGetCString(datum));
	}

	dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
}

static void
dist_ddl_process_vacuum(const ProcessUtilityArgs *args)
{
	VacuumStmt *stmt = castNode(VacuumStmt, args->parsetree);

	if (!dist_ddl_state_set_hypertable(args))
		return;

	/* We do not support VERBOSE flag since it will require to print data
	 * returned from the data nodes */
	if (get_vacuum_options(stmt) & VACOPT_VERBOSE)
		dist_ddl_error_raise_unsupported();

	dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START_NO_2PC, args);
}

static void
dist_ddl_process_truncate(const ProcessUtilityArgs *args)
{
	TruncateStmt *stmt = (TruncateStmt *) args->parsetree;

	if (!dist_ddl_state_set_hypertable(args))
		return;

	/*
	 * We only support TRUNCATE on single distributed hypertables
	 * since other tables might not exist on data nodes and multiple
	 * distributed hypertables might be distributed across different
	 * sets of nodes.
	 *
	 * Note that distributed hypertables are filtered from the relations list in
	 * process_utility.c, so the list might actually be empty.
	 */
	if (list_length(stmt->relations) != 0)
		dist_ddl_error_raise_unsupported();

	dist_ddl_state_schedule(DIST_DDL_EXEC_ON_START, args);
}

static void
dist_ddl_process_unsupported(const ProcessUtilityArgs *args)
{
	if (!dist_ddl_state_set_hypertable(args))
		return;

	dist_ddl_error_raise_unsupported();
}

/*
 * Process DDL operations on distributed hypertables.
 *
 * This function is executed for any Utility/DDL operation and for any
 * PostgreSQL tables been involved in the query.
 *
 * We are particulary interested in distributed hypertables and distributed
 * hypertable members (regular hypertables created on a data nodes).
 *
 * In most cases expect and allow only one distributed hypertable per
 * operation to avoid query deparsing, but do not restrict the number of
 * other hypertable types involved.
 *
 */
static void
dist_ddl_process(const ProcessUtilityArgs *args)
{
	NodeTag tag = nodeTag(args->parsetree);

	/* Block unsupported operations on distributed hypertables and
	 * decide on how to execute it. */
	switch (tag)
	{
		case T_CreateSchemaStmt:
			dist_ddl_process_create_schema(args);
			break;

		case T_DropRoleStmt:
			dist_ddl_process_drop_role(args);
			break;

		case T_DropStmt:
			dist_ddl_process_drop(args);
			break;

		case T_AlterObjectSchemaStmt:
			dist_ddl_process_alter_object_schema(args);
			break;

		case T_AlterOwnerStmt:
			dist_ddl_process_alter_owner_stmt(args);
			break;

		case T_RenameStmt:
			dist_ddl_process_rename(args);
			break;

		case T_GrantStmt:
			dist_ddl_process_grant(args);
			break;

		case T_DropOwnedStmt:
		case T_ReassignOwnedStmt:
			dist_ddl_process_drop_reassign(args);
			break;

		case T_AlterTableStmt:
			dist_ddl_process_alter_table(args);
			break;

		case T_AlterDefaultPrivilegesStmt:
			dist_ddl_process_alter_default_privileges(args);
			break;

		case T_IndexStmt:
			dist_ddl_process_index(args);
			break;

		case T_ReindexStmt:
			dist_ddl_process_reindex(args);
			break;

		case T_CreateTrigStmt:
			dist_ddl_process_create_trigger(args);
			break;

		case T_VacuumStmt:
			dist_ddl_process_vacuum(args);
			break;

		case T_TruncateStmt:
			dist_ddl_process_truncate(args);
			break;

		default:
			/* currently unsupported commands */
			dist_ddl_process_unsupported(args);
			break;
	}
}

static void
dist_ddl_execute(bool transactional)
{
	const char *search_path;
	ListCell *lc;

	/* Execute command on data nodes using local search_path. */
	if (list_length(dist_ddl_state.data_node_list) == 0)
	{
		dist_ddl_state_reset();
		return;
	}

	search_path = GetConfigOption("search_path", false, false);

	foreach (lc, dist_ddl_state.remote_commands)
	{
		void *command = lfirst(lc);
		NodeTag command_tag = nodeTag(command);
		DistCmdResult *result = NULL;

		switch (command_tag)
		{
			case T_String:
			{
				/* Execute single SQL command on each data node from the list */
				const char *sql = strVal(command);

				result = ts_dist_cmd_invoke_on_data_nodes_using_search_path(sql,
																			search_path,
																			dist_ddl_state
																				.data_node_list,
																			transactional);
				break;
			}
			case T_List:
			{
				/* Execute separate SQL command on each data node from the list */
				List *cmd_descriptors = command;
				Assert(list_length(dist_ddl_state.data_node_list) == list_length(cmd_descriptors));

				result =
					ts_dist_multi_cmds_invoke_on_data_nodes_using_search_path(cmd_descriptors,
																			  search_path,
																			  dist_ddl_state
																				  .data_node_list,
																			  transactional);
				break;
			}
			default:
				pg_unreachable();
				break;
		}

		if (result)
		{
			ts_dist_cmd_close_response(result);
			result = NULL;
		}
	}

	dist_ddl_state_reset();
}

/* Update stats after ANALYZE execution */
static void
dist_ddl_get_analyze_stats(const ProcessUtilityArgs *args)
{
	VacuumStmt *stmt = castNode(VacuumStmt, args->parsetree);
	Oid relid = linitial_oid(args->hypertable_list);

	if (!(get_vacuum_options(stmt) & VACOPT_ANALYZE))
		return;

	/* Refresh the local chunk stats by fetching stats
	 * from remote data nodes. */
	chunk_api_update_distributed_hypertable_stats(relid);
}

static bool
dist_ddl_enable_distributed_ddl(void)
{
	const char *enable_distributed_ddl =
		GetConfigOption("timescaledb_experimental.enable_distributed_ddl", true, false);

	/* Default to disabled */
	if (NULL == enable_distributed_ddl)
		return false;

	return strcmp(enable_distributed_ddl, "true") == 0 || strcmp(enable_distributed_ddl, "on") == 0;
}

/*
 * Check if it is allowed to execute distributed DDL operation on a
 * database objects such as SCHEMA, DATABASE, etc.
 */
static bool
dist_ddl_process_database_object(ProcessUtilityArgs *args)
{
	if (dist_ddl_enable_distributed_ddl())
		return true;

	/* disable this functionality while any extension being
	 * created or upgraded */
	if (creating_extension)
		return false;

	switch (nodeTag(args->parsetree))
	{
		case T_CreateSchemaStmt:
		case T_DropOwnedStmt:
		case T_ReassignOwnedStmt:
		case T_AlterDefaultPrivilegesStmt:
			return false;

		case T_DropStmt:
		{
			const DropStmt *stmt = castNode(DropStmt, args->parsetree);
			if (stmt->removeType == OBJECT_SCHEMA)
				return false;
			break;
		}

		case T_RenameStmt:
		{
			const RenameStmt *stmt = castNode(RenameStmt, args->parsetree);
			if (stmt->renameType == OBJECT_SCHEMA)
				return false;
			break;
		}

		case T_AlterOwnerStmt:
		{
			const AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, args->parsetree);
			if (stmt->objectType == OBJECT_SCHEMA)
				return false;
			break;
		}

		case T_GrantStmt:
		{
			const GrantStmt *stmt = castNode(GrantStmt, args->parsetree);
			if (stmt->targtype == ACL_TARGET_ALL_IN_SCHEMA || stmt->targtype == ACL_TARGET_DEFAULTS)
				return false;
			if (stmt->objtype == OBJECT_DATABASE || stmt->objtype == OBJECT_SCHEMA)
				return false;
			break;
		}

		default:
			break;
	}

	return true;
}

void
dist_ddl_start(ProcessUtilityArgs *args)
{
	/* Certain utility commands we know to not process here */
	switch (nodeTag(args->parsetree))
	{
		case T_CopyStmt:
		case T_CallStmt:
			return;
		default:
			break;
	}

	/* Do not process nested DDL operations */
	if (dist_ddl_scheduled_for_execution())
		return;

	/* Process remote DDL only on the distributed database
	 *
	 * Since the dist ddl code executed after being already executed from main process
	 * utility hook in src/process_utility.c we could end in state, when transaction
	 * is already completed by ROLLBACK statement. Since dist_util_membership() function
	 * reads meta data catalog, it is required to have an active transaction there
	 * otherwise it will hit an assert.
	 */
	if (!IsTransactionState() || dist_util_membership() == DIST_MEMBER_NONE)
		return;

	/* Do not process DDL command on a database object, unless it is
	 * allowed by a session variable */
	if (!dist_ddl_process_database_object(args))
	{
		elog(DEBUG1, "skipping dist DDL on object: %s", args->query_string);
		return;
	}

	/* Save origin query and memory context used to save information in
	 * data_node_list since it will differ during event hooks execution. */
	dist_ddl_state.mctx = CurrentMemoryContext;

	/* Decide if this is a distributed DDL operation and when it
	 * should be executed. */
	dist_ddl_process(args);

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
				dist_ddl_get_analyze_stats(args);
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
		dist_ddl_state_reset();
		return;
	}

	/* Do delayed block of SET SCHEMA and RENAME commands.
	 *
	 * In the future those commands might be unblocked and data_node_list could
	 * be updated here as well.
	 */
	if (OidIsValid(dist_ddl_state.relid))
	{
		HypertableType type = dist_ddl_state_get_hypertable_type();

		/* Ensure this operation is executed by the access node session. */
		if (type == HYPERTABLE_DISTRIBUTED_MEMBER)
			dist_ddl_error_if_not_allowed_data_node_session();
	}

	/* Execute command on remote data nodes. */
	dist_ddl_execute(true);
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

				dist_ddl_state_add_data_node_list_from_table(event->schema, event->name);
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

				dist_ddl_state_add_data_node_list_from_table(event->schema, event->table);
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
