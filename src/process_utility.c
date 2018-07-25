#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/nodes.h>
#include <nodes/makefuncs.h>
#include <tcop/utility.h>
#include <catalog/namespace.h>
#include <catalog/pg_inherits_fn.h>
#include <catalog/index.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_trigger.h>
#include <catalog/pg_constraint_fn.h>
#include <catalog/pg_constraint.h>
#include <commands/copy.h>
#include <commands/vacuum.h>
#include <commands/defrem.h>
#include <commands/trigger.h>
#include <commands/tablecmds.h>
#include <commands/cluster.h>
#include <commands/event_trigger.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/snapmgr.h>
#include <parser/parse_utilcmd.h>

#include <miscadmin.h>

#include "process_utility.h"
#include "catalog.h"
#include "chunk.h"
#include "chunk_index.h"
#include "compat.h"
#include "copy.h"
#include "errors.h"
#include "event_trigger.h"
#include "extension.h"
#include "hypercube.h"
#include "hypertable_cache.h"
#include "dimension_vector.h"
#include "indexing.h"
#include "trigger.h"
#include "utils.h"

void		_process_utility_init(void);
void		_process_utility_fini(void);

static ProcessUtility_hook_type prev_ProcessUtility_hook;

static bool expect_chunk_modification = false;

typedef struct ProcessUtilityArgs
{
#if PG10
	PlannedStmt *pstmt;
	QueryEnvironment *queryEnv;
#endif
	Node	   *parsetree;
	const char *query_string;
	ProcessUtilityContext context;
	ParamListInfo params;
	DestReceiver *dest;
	char	   *completion_tag;
} ProcessUtilityArgs;

/* Call the default ProcessUtility and handle PostgreSQL version differences */
static void
prev_ProcessUtility(ProcessUtilityArgs *args)
{
	if (prev_ProcessUtility_hook != NULL)
	{
#if PG10
		/* Call any earlier hooks */
		(prev_ProcessUtility_hook) (args->pstmt,
									args->query_string,
									args->context,
									args->params,
									args->queryEnv,
									args->dest,
									args->completion_tag);
#elif PG96
		(prev_ProcessUtility_hook) (args->parsetree,
									args->query_string,
									args->context,
									args->params,
									args->dest,
									args->completion_tag);
#endif

	}
	else
	{
		/* Call the standard */
#if PG10
		standard_ProcessUtility(args->pstmt,
								args->query_string,
								args->context,
								args->params,
								args->queryEnv,
								args->dest,
								args->completion_tag);
#elif PG96
		standard_ProcessUtility(args->parsetree,
								args->query_string,
								args->context,
								args->params,
								args->dest,
								args->completion_tag);
#endif
	}
}

static void
check_chunk_alter_table_operation_allowed(Oid relid, AlterTableStmt *stmt)
{
	if (expect_chunk_modification)
		return;

	if (chunk_exists_relid(relid))
	{
		bool		all_allowed = true;
		ListCell   *lc;

		/* only allow if all commands are allowed */
		foreach(lc, stmt->cmds)
		{
			AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

			switch (cmd->subtype)
			{
				case AT_SetOptions:
				case AT_ResetOptions:
				case AT_SetStatistics:
				case AT_SetStorage:
				case AT_DropCluster:
				case AT_ClusterOn:
					/* allowed on chunks */
					break;
				default:
					/* disable by default */
					all_allowed = false;
					break;
			}
		}

		if (!all_allowed)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("operation not supported on chunk tables")));
	}
}

static void
relation_not_only(RangeVar *rv)
{
#if PG10
	bool		only = !rv->inh;
#elif PG96
	bool		only = (rv->inhOpt == INH_NO);
#endif
	if (only)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ONLY option not supported on hypertable operations")));
}

/* Change the schema of a hypertable or a chunk */
static void
process_alterobjectschema(Node *parsetree)
{
	AlterObjectSchemaStmt *alterstmt = (AlterObjectSchemaStmt *) parsetree;
	Oid			relid;
	Cache	   *hcache;
	Hypertable *ht;

	if (alterstmt->objectType != OBJECT_TABLE || NULL == alterstmt->relation)
		return;

	relid = RangeVarGetRelid(alterstmt->relation, NoLock, true);

	if (!OidIsValid(relid))
		return;

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, relid);

	if (ht == NULL)
	{
		Chunk	   *chunk = chunk_get_by_relid(relid, 0, false);

		if (NULL != chunk)
			chunk_set_schema(chunk, alterstmt->newschema);
	}
	else
		hypertable_set_schema(ht, alterstmt->newschema);

	cache_release(hcache);
}

static bool
process_copy(Node *parsetree, const char *query_string, char *completion_tag)
{
	CopyStmt   *stmt = (CopyStmt *) parsetree;

	/*
	 * Needed to add the appropriate number of tuples to the completion tag
	 */
	uint64		processed;
	Hypertable *ht;
	Cache	   *hcache;
	Oid			relid;

	if (!stmt->is_from || NULL == stmt->relation)
		return false;

	relid = RangeVarGetRelid(stmt->relation, NoLock, true);

	if (!OidIsValid(relid))
		return false;

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, relid);

	if (ht == NULL)
	{
		cache_release(hcache);
		return false;
	}

	timescaledb_DoCopy(stmt, query_string, &processed, ht);

	if (completion_tag)
		snprintf(completion_tag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processed);

	cache_release(hcache);

	return true;
}

typedef void (*process_chunk_t) (Hypertable *ht, Oid chunk_relid, void *arg);

/*
 * Applies a function to each chunk of a hypertable.
 *
 * Returns the number of processed chunks, or -1 if the table was not a
 * hypertable.
 */
static int
foreach_chunk(Hypertable *ht, process_chunk_t process_chunk, void *arg)
{
	List	   *chunks;
	ListCell   *lc;
	int			n = 0;

	if (NULL == ht)
		return -1;

	chunks = find_inheritance_children(ht->main_table_relid, NoLock);

	foreach(lc, chunks)
	{
		process_chunk(ht, lfirst_oid(lc), arg);
		n++;
	}

	return n;
}

static int
foreach_chunk_relid(Oid relid, process_chunk_t process_chunk, void *arg)
{
	Cache	   *hcache = hypertable_cache_pin();
	Hypertable *ht = hypertable_cache_get_entry(hcache, relid);
	int			ret;

	if (NULL == ht)
	{
		cache_release(hcache);
		return -1;
	}

	hcache->release_on_commit = false;
	ret = foreach_chunk(ht, process_chunk, arg);
	hcache->release_on_commit = true;

	cache_release(hcache);

	return ret;
}

static int
foreach_chunk_relation(RangeVar *rv, process_chunk_t process_chunk, void *arg)
{
	return foreach_chunk_relid(RangeVarGetRelid(rv, NoLock, true), process_chunk, arg);
}

typedef struct VacuumCtx
{
	VacuumStmt *stmt;
	bool		is_toplevel;
} VacuumCtx;

/* Vacuums a single chunk */
static void
vacuum_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	VacuumCtx  *ctx = (VacuumCtx *) arg;
	Chunk	   *chunk = chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	ctx->stmt->relation->relname = NameStr(chunk->fd.table_name);
	ctx->stmt->relation->schemaname = NameStr(chunk->fd.schema_name);
	ExecVacuum(ctx->stmt, ctx->is_toplevel);
}

/* Vacuums each chunk of a hypertable */
static bool
process_vacuum(Node *parsetree, ProcessUtilityContext context)
{
	VacuumStmt *stmt = (VacuumStmt *) parsetree;
	bool		is_toplevel = (context == PROCESS_UTILITY_TOPLEVEL);
	VacuumCtx	ctx = {
		.stmt = stmt,
		.is_toplevel = is_toplevel,
	};
	Oid			hypertable_oid;
	Cache	   *hcache;
	Hypertable *ht;

	if (stmt->relation == NULL)
		/* Vacuum is for all tables */
		return false;

	hypertable_oid = hypertable_relid(stmt->relation);

	if (!OidIsValid(hypertable_oid))
		return false;

	PreventCommandDuringRecovery((stmt->options & VACOPT_VACUUM) ?
								 "VACUUM" : "ANALYZE");

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, hypertable_oid);

	/* allow vacuum to be cross-commit */
	hcache->release_on_commit = false;
	foreach_chunk(ht, vacuum_chunk, &ctx);
	hcache->release_on_commit = true;

	cache_release(hcache);

	/*
	 * You still want the parent to be vacuumed in order to update statistics
	 * if necessary
	 *
	 * Note that in the VERBOSE output this will appear to re-analyze the
	 * child tables. However, this actually just re-acquires the sample from
	 * the child table to use this statistics in the parent table. It will
	 * /not/ write the appropriate statistics in pg_stats for the child table,
	 * so both the per-chunk analyze above and this parent-table vacuum run is
	 * necessary. Later, we can optimize this further, if necessary.
	 */
	stmt->relation->relname = NameStr(ht->fd.table_name);
	stmt->relation->schemaname = NameStr(ht->fd.schema_name);
	ExecVacuum(stmt, is_toplevel);

	return true;
}

static void
process_truncate_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	TruncateStmt *stmt = arg;
	ObjectAddress objaddr = {
		.classId = RelationRelationId,
		.objectId = chunk_relid,
	};

	performDeletion(&objaddr, stmt->behavior, 0);
}

static bool
relation_should_recurse(RangeVar *rv)
{
#if PG10
	return rv->inh;
#elif PG96
	if (rv->inhOpt == INH_DEFAULT)
	{
		char	   *inherit_guc = GetConfigOptionByName("SQL_inheritance", NULL, false);

		return strncmp(inherit_guc, "on", 2) == 0;
	}
	return rv->inhOpt == INH_YES;
#endif
}

/*
 * Truncate a hypertable.
 */
static bool
process_truncate(ProcessUtilityArgs *args)
{
	TruncateStmt *stmt = (TruncateStmt *) args->parsetree;
	Cache	   *hcache = hypertable_cache_pin();
	ListCell   *cell;

	/* Call standard process utility first to truncate all tables */
	prev_ProcessUtility(args);

	/* For all hypertables, we drop the now empty chunks */
	foreach(cell, stmt->relations)
	{
		RangeVar   *rv = lfirst(cell);
		Oid			relid;

		if (NULL == rv)
			continue;

		relid = RangeVarGetRelid(rv, NoLock, true);

		if (OidIsValid(relid))
		{
			Hypertable *ht = hypertable_cache_get_entry(hcache, relid);

			if (ht != NULL)
			{
				if (!relation_should_recurse(rv))
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot truncate only a hypertable"),
							 errhint("Do not specify the ONLY keyword, or use truncate"
									 " only on the chunks directly.")));

				/* Delete the metadata */
				chunk_delete_by_hypertable_id(ht->fd.id);

				/* Drop the chunk tables */
				foreach_chunk(ht, process_truncate_chunk, stmt);
			}
		}
	}

	cache_release(hcache);

	return true;
}


static void
process_drop_table_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	DropStmt   *stmt = arg;
	ObjectAddress objaddr = {
		.classId = RelationRelationId,
		.objectId = chunk_relid,
	};

	performDeletion(&objaddr, stmt->behavior, 0);
}

/*
 *  We need to drop hypertable chunks before the hypertable to avoid the need
 *  to CASCADE such drops;
 */
static bool
process_drop_hypertable_chunks(DropStmt *stmt)
{
	Cache	   *hcache = hypertable_cache_pin();
	ListCell   *lc;
	bool		handled = false;

	foreach(lc, stmt->objects)
	{
		List	   *object = lfirst(lc);
		RangeVar   *relation = makeRangeVarFromNameList(object);
		Oid			relid;

		if (NULL == relation)
			continue;

		relid = RangeVarGetRelid(relation, NoLock, true);

		if (OidIsValid(relid))
		{
			Hypertable *ht;

			ht = hypertable_cache_get_entry(hcache, relid);

			if (NULL != ht)
			{
				if (list_length(stmt->objects) != 1)
					elog(ERROR, "cannot drop a hypertable along with other objects");

				/* Drop each chunk table */
				foreach_chunk(ht, process_drop_table_chunk, stmt);
			}

			handled = true;
		}
	}

	cache_release(hcache);

	return handled;
}

/* Note that DROP TABLESPACE does not have a hook in event triggers so cannot go
 * through process_ddl_sql_drop */
static void
process_drop_tablespace(Node *parsetree)
{
	DropTableSpaceStmt *stmt = (DropTableSpaceStmt *) parsetree;
	int			count = tablespace_count_attached(stmt->tablespacename);

	if (count > 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("tablespace \"%s\" is still attached to %d hypertables",
						stmt->tablespacename, count),
				 errhint("Detach the tablespace from all hypertables before removing it.")));
}

/*
 * Handle GRANT / REVOKE.
 *
 * A revoke is a GrantStmt with 'is_grant' set to false.
 */
static bool
process_grant_and_revoke(ProcessUtilityArgs *args)
{
	GrantStmt  *stmt = (GrantStmt *) args->parsetree;

	/*
	 * Need to apply the REVOKE first to be able to check remaining
	 * permissions
	 */
	prev_ProcessUtility(args);

	/* We only care about revokes and setting privileges on a specific object */
	if (stmt->is_grant || stmt->targtype != ACL_TARGET_OBJECT)
		return true;

	switch (stmt->objtype)
	{
		case ACL_OBJECT_TABLESPACE:
			tablespace_validate_revoke(stmt);
			break;
		default:
			break;
	}

	return true;
}

static bool
process_grant_and_revoke_role(ProcessUtilityArgs *args)
{
	GrantRoleStmt *stmt = (GrantRoleStmt *) args->parsetree;

	/*
	 * Need to apply the REVOKE first to be able to check remaining
	 * permissions
	 */
	prev_ProcessUtility(args);

	/* We only care about revokes and setting privileges on a specific object */
	if (stmt->is_grant)
		return true;

	tablespace_validate_revoke_role(stmt);

	return true;
}

static void
process_drop(Node *parsetree)
{
	DropStmt   *stmt = (DropStmt *) parsetree;

	switch (stmt->removeType)
	{
		case OBJECT_TABLE:
			process_drop_hypertable_chunks(stmt);
			break;
		default:
			break;
	}
}

static void
reindex_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	ReindexStmt *stmt = (ReindexStmt *) arg;
	Chunk	   *chunk = chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	switch (stmt->kind)
	{
		case REINDEX_OBJECT_TABLE:
			stmt->relation->relname = NameStr(chunk->fd.table_name);
			stmt->relation->schemaname = NameStr(chunk->fd.schema_name);
			ReindexTable(stmt->relation, stmt->options);
			break;
		case REINDEX_OBJECT_INDEX:
			/* Not supported, a.t.m. See note in process_reindex(). */
			break;
		default:
			break;
	}
}

/*
 * Reindex a hypertable and all its chunks. Currently works only for REINDEX
 * TABLE.
 */
static bool
process_reindex(Node *parsetree)
{
	ReindexStmt *stmt = (ReindexStmt *) parsetree;
	Oid			relid;
	Cache	   *hcache;
	Hypertable *ht;
	bool		ret = false;

	if (NULL == stmt->relation)
		/* Not a case we are interested in */
		return false;

	relid = RangeVarGetRelid(stmt->relation, NoLock, true);

	if (!OidIsValid(relid))
		return false;

	hcache = hypertable_cache_pin();

	switch (stmt->kind)
	{
		case REINDEX_OBJECT_TABLE:
			ht = hypertable_cache_get_entry(hcache, relid);

			if (NULL != ht)
			{
				PreventCommandDuringRecovery("REINDEX");

				if (foreach_chunk(ht, reindex_chunk, stmt) >= 0)
					ret = true;
			}
			break;
		case REINDEX_OBJECT_INDEX:
			ht = hypertable_cache_get_entry(hcache, IndexGetRelation(relid, true));

			if (NULL != ht)
			{
				/*
				 * Recursing to chunks is currently not supported. Need to
				 * look up all chunk indexes that corresponds to the
				 * hypertable's index.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("reindexing of a specific index on a hypertable is unsupported"),
						 errhint("As a workaround, it is possible to run REINDEX TABLE to reindex all "
								 "indexes on a hypertable, including all indexes on chunks.")));
			}
			break;
		default:
			break;
	}

	cache_release(hcache);

	return ret;
}

/*
 * Rename a hypertable or a chunk.
 */
static void
process_rename_table(Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht = hypertable_cache_get_entry(hcache, relid);

	if (NULL == ht)
	{
		Chunk	   *chunk = chunk_get_by_relid(relid, 0, false);

		if (NULL != chunk)
			chunk_set_name(chunk, stmt->newname);
	}
	else
		hypertable_set_name(ht, stmt->newname);
}

static void
process_rename_column(Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht = hypertable_cache_get_entry(hcache, relid);
	Dimension  *dim;

	if (NULL == ht)
	{
		Chunk	   *chunk = chunk_get_by_relid(relid, 0, false);

		if (NULL != chunk)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot rename column \"%s\" of hypertable chunk \"%s\"",
							stmt->subname, get_rel_name(relid)),
					 errhint("Rename the hypertable column instead.")));
		return;
	}

	dim = hyperspace_get_dimension_by_name(ht->space, DIMENSION_TYPE_ANY, stmt->subname);

	if (NULL == dim)
		return;

	dimension_set_name(dim, stmt->newname);
}

static void
process_rename_index(Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Oid			tablerelid = IndexGetRelation(relid, true);
	Hypertable *ht;

	if (!OidIsValid(tablerelid))
		return;

	ht = hypertable_cache_get_entry(hcache, tablerelid);

	if (NULL != ht)
		chunk_index_rename_parent(ht, relid, stmt->newname);
	else
	{
		Chunk	   *chunk = chunk_get_by_relid(tablerelid, 0, false);

		if (NULL != chunk)
			chunk_index_rename(chunk, relid, stmt->newname);
	}
}

static void
rename_hypertable_constraint(Hypertable *ht, Oid chunk_relid, void *arg)
{
	RenameStmt *stmt = (RenameStmt *) arg;
	Chunk	   *chunk = chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	chunk_constraint_rename_hypertable_constraint(chunk->fd.id, stmt->subname, stmt->newname);
}

static void
alter_hypertable_constraint(Hypertable *ht, Oid chunk_relid, void *arg)
{
	AlterTableCmd *cmd = (AlterTableCmd *) arg;
	Constraint *cmd_constraint;
	char	   *hypertable_constraint_name;

	Assert(IsA(cmd->def, Constraint));
	cmd_constraint = (Constraint *) cmd->def;
	hypertable_constraint_name = cmd_constraint->conname;

	cmd_constraint->conname = chunk_constraint_get_name_from_hypertable_constraint(chunk_relid, hypertable_constraint_name);

	AlterTableInternal(chunk_relid, list_make1(cmd), false);

	/* Restore for next iteration */
	cmd_constraint->conname = hypertable_constraint_name;
}

static void
validate_hypertable_constraint(Hypertable *ht, Oid chunk_relid, void *arg)
{
	AlterTableCmd *cmd = (AlterTableCmd *) arg;
	AlterTableCmd *chunk_cmd = copyObject(cmd);

	chunk_cmd->name = chunk_constraint_get_name_from_hypertable_constraint(chunk_relid, cmd->name);

	/* do not pass down the VALIDATE RECURSE subtype */
	chunk_cmd->subtype = AT_ValidateConstraint;
	AlterTableInternal(chunk_relid, list_make1(chunk_cmd), false);
}

static void
process_rename_constraint(Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht;

	ht = hypertable_cache_get_entry(hcache, relid);

	if (NULL != ht)
	{
		relation_not_only(stmt->relation);
		foreach_chunk(ht, rename_hypertable_constraint, stmt);
	}
	else
	{
		Chunk	   *chunk = chunk_get_by_relid(relid, 0, false);

		if (NULL != chunk)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("renaming constraints on chunks is not supported")));

	}
}


static void
process_rename(Node *parsetree)
{
	RenameStmt *stmt = (RenameStmt *) parsetree;
	Oid			relid;
	Cache	   *hcache;

	if (NULL == stmt->relation)
		/* Not an object we are interested in */
		return;

	relid = RangeVarGetRelid(stmt->relation, NoLock, true);

	if (!OidIsValid(relid))
		return;

	/* TODO: forbid all rename op on chunk table */

	hcache = hypertable_cache_pin();

	switch (stmt->renameType)
	{
		case OBJECT_TABLE:
			process_rename_table(hcache, relid, stmt);
			break;
		case OBJECT_COLUMN:
			process_rename_column(hcache, relid, stmt);
			break;
		case OBJECT_INDEX:
			process_rename_index(hcache, relid, stmt);
			break;
		case OBJECT_TABCONSTRAINT:
			process_rename_constraint(hcache, relid, stmt);
			break;
		default:
			break;
	}

	cache_release(hcache);
}

static void
process_altertable_change_owner_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	AlterTableCmd *cmd = arg;
	Oid			roleid = get_rolespec_oid(cmd->newowner, false);

	ATExecChangeOwner(chunk_relid, roleid, false, AccessExclusiveLock);
}

static void
process_altertable_change_owner(Hypertable *ht, AlterTableCmd *cmd)
{
	Assert(IsA(cmd->newowner, RoleSpec));

	foreach_chunk(ht, process_altertable_change_owner_chunk, cmd);
}

static void
process_add_constraint_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	Oid			hypertable_constraint_oid = *((Oid *) arg);
	Chunk	   *chunk = chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	chunk_constraint_create_on_chunk(chunk, hypertable_constraint_oid);
}

static void
process_altertable_add_constraint(Hypertable *ht, const char *constraint_name)
{
	Oid			hypertable_constraint_oid = get_relation_constraint_oid(ht->main_table_relid, constraint_name, false);

	Assert(constraint_name != NULL);

	foreach_chunk(ht, process_add_constraint_chunk, &hypertable_constraint_oid);
}

static void
process_altertable_alter_constraint_end(Hypertable *ht, AlterTableCmd *cmd)
{
	foreach_chunk(ht, alter_hypertable_constraint, cmd);
}

static void
process_altertable_validate_constraint_end(Hypertable *ht, AlterTableCmd *cmd)
{
	foreach_chunk(ht, validate_hypertable_constraint, cmd);
}

static void
process_altertable_drop_not_null(Hypertable *ht, AlterTableCmd *cmd)
{
	int			i;

	for (i = 0; i < ht->space->num_dimensions; i++)
	{
		Dimension  *dim = &ht->space->dimensions[i];

		if (IS_OPEN_DIMENSION(dim) &&
			strncmp(NameStr(dim->fd.column_name), cmd->name, NAMEDATALEN) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_IO_OPERATION_NOT_SUPPORTED),
					 errmsg("cannot drop not-null constraint from a time-partitioned column")));
	}
}

static void
process_altertable_drop_column(Hypertable *ht, AlterTableCmd *cmd)
{
	int			i;

	for (i = 0; i < ht->space->num_dimensions; i++)
	{
		Dimension  *dim = &ht->space->dimensions[i];

		if (namestrcmp(&dim->fd.column_name, cmd->name) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot drop column named in partition key"),
					 errdetail("cannot drop column that is a hypertable partitioning (space or time) dimension")));
	}
}

/* process all regular-table alter commands to make sure they aren't adding
 * foreign-key constraints to hypertables */
static void
verify_constraint_plaintable(RangeVar *relation, Constraint *constr)
{
	Cache	   *hcache;
	Hypertable *ht;

	Assert(IsA(constr, Constraint));

	hcache = hypertable_cache_pin();

	switch (constr->contype)
	{
		case CONSTR_FOREIGN:
			ht = hypertable_cache_get_entry_rv(hcache, constr->pktable);

			if (NULL != ht)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("foreign keys to hypertables are not supported")));
			break;
		default:
			break;
	}

	cache_release(hcache);
}

/*
 * Verify that a constraint is supported on a hypertable.
 */
static void
verify_constraint_hypertable(Hypertable *ht, Node *constr_node)
{
	ConstrType	contype;
	const char *indexname;
	List	   *keys;

	if (IsA(constr_node, Constraint))
	{
		Constraint *constr = (Constraint *) constr_node;

		contype = constr->contype;
		keys = (contype == CONSTR_EXCLUSION) ? constr->exclusions : constr->keys;
		indexname = constr->indexname;

		/* NO INHERIT constraints do not really make sense on a hypertable */
		if (constr->is_no_inherit)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot have NO INHERIT constraints on hypertable \"%s\"",
							get_rel_name(ht->main_table_relid))));
	}
	else if (IsA(constr_node, IndexStmt))
	{
		IndexStmt  *stmt = (IndexStmt *) constr_node;

		contype = stmt->primary ? CONSTR_PRIMARY : CONSTR_UNIQUE;
		keys = stmt->indexParams;
		indexname = stmt->idxname;
	}
	else
	{
		elog(ERROR, "unexpected constraint type");
		return;
	}

	switch (contype)
	{
		case CONSTR_FOREIGN:
			break;
		case CONSTR_UNIQUE:
		case CONSTR_PRIMARY:

			/*
			 * If this constraints is created using an existing index we need
			 * not re-verify it's columns
			 */
			if (indexname != NULL)
				return;

			indexing_verify_columns(ht->space, keys);
			break;
		case CONSTR_EXCLUSION:
			indexing_verify_columns(ht->space, keys);
			break;
		default:
			break;
	}
}

static void
verify_constraint(RangeVar *relation, Constraint *constr)
{
	Cache	   *hcache = hypertable_cache_pin();
	Hypertable *ht = hypertable_cache_get_entry_rv(hcache, relation);

	if (NULL == ht)
		verify_constraint_plaintable(relation, constr);
	else
		verify_constraint_hypertable(ht, (Node *) constr);

	cache_release(hcache);
}

static void
verify_constraint_list(RangeVar *relation, List *constraint_list)
{
	ListCell   *lc;

	foreach(lc, constraint_list)
	{
		Constraint *constraint = lfirst(lc);

		verify_constraint(relation, constraint);
	}
}

typedef struct CreateIndexInfo
{
	IndexStmt  *stmt;
	ObjectAddress obj;
} CreateIndexInfo;

/*
 * Create index on a chunk.
 *
 * A chunk index is created based on the original IndexStmt that created the
 * "parent" index on the hypertable.
 */
static void
process_index_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	CreateIndexInfo *info = (CreateIndexInfo *) arg;
	IndexStmt  *stmt = transformIndexStmt(chunk_relid, info->stmt, NULL);
	Chunk	   *chunk = chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	chunk_index_create_from_stmt(stmt, chunk->fd.id, chunk_relid, ht->fd.id, info->obj.objectId);
}

static void
process_index_start(Node *parsetree)
{
	IndexStmt  *stmt = (IndexStmt *) parsetree;
	Cache	   *hcache;
	Hypertable *ht;

	Assert(IsA(stmt, IndexStmt));

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry_rv(hcache, stmt->relation);

	if (NULL != ht)
	{
		/* Make sure this index is allowed */
		if (stmt->concurrent)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertables do not support concurrent "
							"index creation")));

		indexing_verify_index(ht->space, stmt);
	}

	cache_release(hcache);
}

static bool
process_index_end(Node *parsetree, CollectedCommand *cmd)
{
	IndexStmt  *stmt = (IndexStmt *) parsetree;
	CatalogSecurityContext sec_ctx;
	Cache	   *hcache;
	Hypertable *ht;
	bool		handled = false;

	Assert(IsA(stmt, IndexStmt));

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry_rv(hcache, stmt->relation);

	if (NULL != ht)
	{
		CreateIndexInfo info = {
			.stmt = stmt,
		};

		switch (cmd->type)
		{
			case SCT_Simple:
				info.obj = cmd->d.simple.address;
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("hypertables do not support this operation: "
								"parsetree %s, type %d",
								nodeToString(parsetree), cmd->type)));
				break;
		}

		/*
		 * Change user since chunk's are typically located in an internal
		 * schema and chunk indexes require metadata changes
		 */
		catalog_become_owner(catalog_get(), &sec_ctx);

		/* Recurse to each chunk and create a corresponding index */
		foreach_chunk(ht, process_index_chunk, &info);

		catalog_restore_user(&sec_ctx);
		handled = true;
	}

	cache_release(hcache);

	return handled;
}

static Oid
find_clustered_index(Oid table_relid)
{
	Relation	rel;
	ListCell   *index;
	Oid			index_relid = InvalidOid;

	rel = heap_open(table_relid, NoLock);

	/* We need to find the index that has indisclustered set. */
	foreach(index, RelationGetIndexList(rel))
	{
		HeapTuple	idxtuple;
		Form_pg_index indexForm;

		index_relid = lfirst_oid(index);
		idxtuple = SearchSysCache1(INDEXRELID,
								   ObjectIdGetDatum(index_relid));
		if (!HeapTupleIsValid(idxtuple))
			elog(ERROR, "cache lookup failed for index %u", index_relid);
		indexForm = (Form_pg_index) GETSTRUCT(idxtuple);

		if (indexForm->indisclustered)
		{
			ReleaseSysCache(idxtuple);
			break;
		}
		ReleaseSysCache(idxtuple);
		index_relid = InvalidOid;
	}

	heap_close(rel, NoLock);

	if (!OidIsValid(index_relid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("there is no previously clustered index for table \"%s\"",
						get_rel_name(table_relid))));

	return index_relid;
}

/*
 * Cluster a hypertable.
 *
 * The functionality to cluster all chunks of a hypertable is based on the
 * regular cluster function's mode to cluster multiple tables. Since clustering
 * involves taking exclusive locks on all tables for extensive periods of time,
 * each subtable is clustered in its own transaction. This will release all
 * locks on subtables once they are done.
 */
static bool
process_cluster_start(Node *parsetree, ProcessUtilityContext context)
{
	ClusterStmt *stmt = (ClusterStmt *) parsetree;
	Cache	   *hcache;
	Hypertable *ht;

	Assert(IsA(stmt, ClusterStmt));

	/* If this is a re-cluster on all tables, there is nothing we need to do */
	if (NULL == stmt->relation)
		return false;

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry_rv(hcache, stmt->relation);

	if (NULL != ht)
	{
		bool		is_top_level = (context == PROCESS_UTILITY_TOPLEVEL);
		Oid			index_relid;
		List	   *chunk_indexes;
		ListCell   *lc;
		MemoryContext old,
					mcxt;

		if (!pg_class_ownercheck(ht->main_table_relid, GetUserId()))
			aclcheck_error(ACLCHECK_NOT_OWNER, ACL_KIND_CLASS,
						   get_rel_name(ht->main_table_relid));

		/*
		 * If CLUSTER is run inside a user transaction block; we bail out or
		 * otherwise we'd be holding locks way too long.
		 */
		PreventTransactionChain(is_top_level, "CLUSTER");

		if (NULL == stmt->indexname)
			index_relid = find_clustered_index(ht->main_table_relid);
		else
			index_relid = get_relname_relid(stmt->indexname,
											get_rel_namespace(ht->main_table_relid));

		if (!OidIsValid(index_relid))
		{
			/* Let regular process utility handle */
			cache_release(hcache);
			return false;
		}

		/*
		 * The list of chunks and their indexes need to be on a memory context
		 * that will survive moving to a new transaction for each chunk
		 */
		mcxt = AllocSetContextCreate(PortalContext,
									 "Hypertable cluster",
									 ALLOCSET_DEFAULT_SIZES);

		/*
		 * Get a list of chunks and indexes that correspond to the
		 * hypertable's index
		 */
		old = MemoryContextSwitchTo(mcxt);
		chunk_indexes = chunk_index_get_mappings(ht, index_relid);
		MemoryContextSwitchTo(old);

		hcache->release_on_commit = false;

		/* Commit to get out of starting transaction */
		PopActiveSnapshot();
		CommitTransactionCommand();

		foreach(lc, chunk_indexes)
		{
			ChunkIndexMapping *cim = lfirst(lc);

			/* Start a new transaction for each relation. */
			StartTransactionCommand();
			/* functions in indexes may want a snapshot set */
			PushActiveSnapshot(GetTransactionSnapshot());

			/*
			 * We must mark each chunk index as clustered before calling
			 * cluster_rel() because it expects indexes that need to be
			 * rechecked (due to new transaction) to already have that mark
			 * set
			 */
			chunk_index_mark_clustered(cim->chunkoid, cim->indexoid);

			/* Do the job. */
			cluster_rel(cim->chunkoid, cim->indexoid, true, stmt->verbose);
			PopActiveSnapshot();
			CommitTransactionCommand();
		}

		hcache->release_on_commit = true;
		/* Start a new transaction for the cleanup work. */
		StartTransactionCommand();

		/* Clean up working storage */
		MemoryContextDelete(mcxt);
	}

	cache_release(hcache);
	return false;
}

/*
 * Process create table statements.
 *
 * For regular tables, we need to ensure that they don't have any foreign key
 * constraints that point to hypertables.
 *
 * NOTE that this function should be called after parse analysis (in an end DDL
 * trigger or by running parse analysis manually).
 */
static void
process_create_table_end(Node *parsetree)
{
	CreateStmt *stmt = (CreateStmt *) parsetree;
	ListCell   *lc;

	verify_constraint_list(stmt->relation, stmt->constraints);

	/*
	 * Only after parse analyis does tableElts contain only ColumnDefs. So, if
	 * we capture this in processUtility, we should be prepared to have
	 * constraint nodes and TableLikeClauses intermixed
	 */
	foreach(lc, stmt->tableElts)
	{
		ColumnDef  *coldef;

		switch (nodeTag(lfirst(lc)))
		{
			case T_ColumnDef:
				coldef = lfirst(lc);
				verify_constraint_list(stmt->relation, coldef->constraints);
				break;
			case T_Constraint:

				/*
				 * There should be no Constraints in the list after parse
				 * analysis, but this case is included anyway for completeness
				 */
				verify_constraint(stmt->relation, lfirst(lc));
				break;
			case T_TableLikeClause:
				/* Some as above case */
				break;
			default:
				break;
		}
	}
}

static inline const char *
typename_get_unqual_name(TypeName *tn)
{
	Value	   *name = llast(tn->names);

	return name->val.str;
}

static void
process_alter_column_type_start(Hypertable *ht, AlterTableCmd *cmd)
{
	int			i;

	for (i = 0; i < ht->space->num_dimensions; i++)
	{
		Dimension  *dim = &ht->space->dimensions[i];

		if (IS_CLOSED_DIMENSION(dim) &&
			strncmp(NameStr(dim->fd.column_name), cmd->name, NAMEDATALEN) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_IO_OPERATION_NOT_SUPPORTED),
					 errmsg("cannot change the type of a hash-partitioned column")));
	}
}

static void
process_alter_column_type_end(Hypertable *ht, AlterTableCmd *cmd)
{
	ColumnDef  *coldef = (ColumnDef *) cmd->def;
	Oid			new_type = TypenameGetTypid(typename_get_unqual_name(coldef->typeName));
	Dimension  *dim = hyperspace_get_dimension_by_name(ht->space, DIMENSION_TYPE_ANY, cmd->name);

	if (NULL == dim)
		return;

	dimension_set_type(dim, new_type);
	process_utility_set_expect_chunk_modification(true);
	chunk_recreate_all_constraints_for_dimension(ht->space, dim->fd.id);
	process_utility_set_expect_chunk_modification(false);
}

static void
process_altertable_clusteron_end(Hypertable *ht, AlterTableCmd *cmd)
{
	Oid			index_relid = get_relname_relid(cmd->name, get_namespace_oid(NameStr(ht->fd.schema_name), false));
	List	   *chunk_indexes = chunk_index_get_mappings(ht, index_relid);
	ListCell   *lc;

	foreach(lc, chunk_indexes)
	{
		ChunkIndexMapping *cim = lfirst(lc);

		chunk_index_mark_clustered(cim->chunkoid, cim->indexoid);
	}
}

/*
 * Generic function to recurse ALTER TABLE commands to chunks.
 *
 * Call with foreach_chunk().
 */
static void
process_altertable_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	AlterTableCmd *cmd = arg;

	AlterTableInternal(chunk_relid, list_make1(cmd), false);
}

static void
process_altertable_end_index(Node *parsetree, CollectedCommand *cmd)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	Oid			indexrelid = AlterTableLookupRelation(stmt, NoLock);
	Oid			tablerelid = IndexGetRelation(indexrelid, false);
	Cache	   *hcache;
	Hypertable *ht;

	if (!OidIsValid(tablerelid))
		return;

	hcache = hypertable_cache_pin();

	ht = hypertable_cache_get_entry(hcache, tablerelid);

	if (NULL != ht)
	{
		ListCell   *lc;

		foreach(lc, stmt->cmds)
		{
			AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

			switch (cmd->subtype)
			{
				case AT_SetTableSpace:
					chunk_index_set_tablespace(ht, indexrelid, cmd->name);
					break;
				default:
					break;
			}
		}
	}

	cache_release(hcache);
}

static void
process_altertable_start_table(Node *parsetree)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	Oid			relid = AlterTableLookupRelation(stmt, NoLock);
	Cache	   *hcache;
	Hypertable *ht;
	ListCell   *lc;

	if (!OidIsValid(relid))
		return;

	check_chunk_alter_table_operation_allowed(relid, stmt);

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, relid);
	if (ht != NULL)
		relation_not_only(stmt->relation);

	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_AddIndex:
				{
					IndexStmt  *istmt = (IndexStmt *) cmd->def;

					Assert(IsA(cmd->def, IndexStmt));

					if (NULL != ht && istmt->isconstraint)
						verify_constraint_hypertable(ht, cmd->def);
				}
				break;
			case AT_DropNotNull:
				if (ht != NULL)
					process_altertable_drop_not_null(ht, cmd);
				break;
			case AT_AddColumn:
			case AT_AddColumnRecurse:
				{
					ColumnDef  *col;
					ListCell   *constraint_lc;

					Assert(IsA(cmd->def, ColumnDef));
					col = (ColumnDef *) cmd->def;

					if (NULL == ht)
						foreach(constraint_lc, col->constraints)
							verify_constraint_plaintable(stmt->relation, lfirst(constraint_lc));
					else
						foreach(constraint_lc, col->constraints)
							verify_constraint_hypertable(ht, lfirst(constraint_lc));
					break;
				}
			case AT_DropColumn:
			case AT_DropColumnRecurse:
				if (NULL != ht)
					process_altertable_drop_column(ht, cmd);
				break;
			case AT_AddConstraint:
			case AT_AddConstraintRecurse:
				Assert(IsA(cmd->def, Constraint));

				if (NULL == ht)
					verify_constraint_plaintable(stmt->relation, (Constraint *) cmd->def);
				else
					verify_constraint_hypertable(ht, cmd->def);
				break;
			case AT_AlterColumnType:
				Assert(IsA(cmd->def, ColumnDef));

				if (ht != NULL)
					process_alter_column_type_start(ht, cmd);
				break;
#if PG10
			case AT_AttachPartition:
				{
					RangeVar   *relation;
					PartitionCmd *partstmt;

					partstmt = (PartitionCmd *) cmd->def;
					relation = partstmt->name;
					Assert(NULL != relation);

					if (InvalidOid != hypertable_relid(relation))
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("hypertables do not support native "
										"postgres partitioning")));
					}
				}
#endif
			default:
				break;
		}
	}

	cache_release(hcache);
}

static void
process_altertable_start(Node *parsetree)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;

	switch (stmt->relkind)
	{
		case OBJECT_TABLE:
			process_altertable_start_table(parsetree);
			break;
		default:
			break;
	}
}

static void
process_altertable_end_subcmd(Hypertable *ht, Node *parsetree, ObjectAddress *obj)
{
	AlterTableCmd *cmd = (AlterTableCmd *) parsetree;

	Assert(IsA(parsetree, AlterTableCmd));

	switch (cmd->subtype)
	{
		case AT_ChangeOwner:
			process_altertable_change_owner(ht, cmd);
			break;
		case AT_AddIndexConstraint:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertables do not support adding a constraint "
							"using an existing index")));
			break;
		case AT_AddIndex:
			{
				IndexStmt  *stmt = (IndexStmt *) cmd->def;
				const char *idxname = stmt->idxname;

				Assert(IsA(cmd->def, IndexStmt));

				Assert(stmt->isconstraint);

				if (idxname == NULL)
					idxname = get_rel_name(obj->objectId);

				process_altertable_add_constraint(ht, idxname);
			}
			break;
		case AT_AddConstraint:
		case AT_AddConstraintRecurse:
			{
				Constraint *stmt = (Constraint *) cmd->def;
				const char *conname = stmt->conname;

				Assert(IsA(cmd->def, Constraint));

				/* Check constraints are recursed to chunks by default */
				if (stmt->contype == CONSTR_CHECK)
					break;

				if (conname == NULL)
					conname = get_rel_name(obj->objectId);

				process_altertable_add_constraint(ht, conname);
			}
			break;
		case AT_AlterColumnType:
			Assert(IsA(cmd->def, ColumnDef));
			process_alter_column_type_end(ht, cmd);
			break;
		case AT_EnableTrig:
		case AT_EnableAlwaysTrig:
		case AT_EnableReplicaTrig:
		case AT_DisableTrig:
		case AT_EnableTrigAll:
		case AT_DisableTrigAll:
		case AT_EnableTrigUser:
		case AT_DisableTrigUser:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertables do not support  "
							"enabling or disabling triggers.")));
			break;
		case AT_ClusterOn:
			process_altertable_clusteron_end(ht, cmd);
			break;
		case AT_SetUnLogged:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("logging cannot be turned off for hypertables")));
			break;
		case AT_ReplicaIdentity:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertables do not support logical replication")));
		case AT_EnableRule:
		case AT_EnableAlwaysRule:
		case AT_EnableReplicaRule:
		case AT_DisableRule:
			/* should never actually get here but just in case */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertables do not support rules")));
			break;
		case AT_AlterConstraint:
			process_altertable_alter_constraint_end(ht, cmd);
			break;
		case AT_ValidateConstraint:
		case AT_ValidateConstraintRecurse:
			process_altertable_validate_constraint_end(ht, cmd);
			break;
		case AT_SetRelOptions:
		case AT_ResetRelOptions:
		case AT_ReplaceRelOptions:
		case AT_AddOids:
		case AT_DropOids:
		case AT_SetOptions:
		case AT_ResetOptions:
		case AT_DropCluster:
			foreach_chunk(ht, process_altertable_chunk, cmd);
			break;
		case AT_AddInherit:
		case AT_DropInherit:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertables do not support inheritance")));
		case AT_SetStatistics:
		case AT_SetLogged:
			/* handled by default recursion */
			break;
		default:
			break;
	}
}

static void
process_altertable_end_simple_cmd(Hypertable *ht, CollectedCommand *cmd)
{
	AlterTableStmt *stmt = (AlterTableStmt *) cmd->parsetree;

	Assert(IsA(stmt, AlterTableStmt));
	process_altertable_end_subcmd(ht, linitial(stmt->cmds), &cmd->d.simple.secondaryObject);
}

static void
process_altertable_end_subcmds(Hypertable *ht, List *cmds)
{
	ListCell   *lc;

	foreach(lc, cmds)
	{
		CollectedATSubcmd *cmd = lfirst(lc);

		process_altertable_end_subcmd(ht, cmd->parsetree, &cmd->address);
	}
}

static void
process_altertable_end_table(Node *parsetree, CollectedCommand *cmd)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	Oid			relid;
	Cache	   *hcache;
	Hypertable *ht;

	Assert(IsA(stmt, AlterTableStmt));

	relid = AlterTableLookupRelation(stmt, NoLock);

	if (!OidIsValid(relid))
		return;

	hcache = hypertable_cache_pin();

	/* TODO: forbid all alter_table on chunk table */

	ht = hypertable_cache_get_entry(hcache, relid);

	if (NULL != ht)
	{
		switch (cmd->type)
		{
			case SCT_Simple:
				process_altertable_end_simple_cmd(ht, cmd);
				break;
			case SCT_AlterTable:
				process_altertable_end_subcmds(ht, cmd->d.alterTable.subcmds);
				break;
			default:
				break;
		}
	}

	cache_release(hcache);
}

static void
process_altertable_end(Node *parsetree, CollectedCommand *cmd)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;

	switch (stmt->relkind)
	{
		case OBJECT_TABLE:
			process_altertable_end_table(parsetree, cmd);
			break;
		case OBJECT_INDEX:
			process_altertable_end_index(parsetree, cmd);
			break;
		default:
			break;
	}
}

static void
create_trigger_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	CreateTrigStmt *stmt = arg;
	Oid			trigger_oid = get_trigger_oid(ht->main_table_relid, stmt->trigname, false);
	char	   *relschema = get_namespace_name(get_rel_namespace(chunk_relid));
	char	   *relname = get_rel_name(chunk_relid);

	trigger_create_on_chunk(trigger_oid, relschema, relname);
}

static void
process_create_trigger_start(Node *parsetree)
{
	CreateTrigStmt *stmt = (CreateTrigStmt *) parsetree;

	if (!stmt->row)
		return;

	if (hypertable_relid(stmt->relation) == InvalidOid)
		return;

#if PG10
	if (stmt->transitionRels != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support transition tables in triggers")));
#endif
}

static void
process_create_rule_start(Node *parsetree)
{
	RuleStmt   *stmt = (RuleStmt *) parsetree;

	if (hypertable_relid(stmt->relation) == InvalidOid)
		return;


	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("hypertables do not support rules")));
}

static void
process_create_trigger_end(Node *parsetree)
{
	CreateTrigStmt *stmt = (CreateTrigStmt *) parsetree;

	if (!stmt->row)
		return;

	foreach_chunk_relation(stmt->relation, create_trigger_chunk, stmt);
}

/*
 * Handle DDL commands before they have been processed by PostgreSQL.
 */
static bool
process_ddl_command_start(ProcessUtilityArgs *args)
{
	bool		handled = false;

	switch (nodeTag(args->parsetree))
	{
		case T_AlterObjectSchemaStmt:
			process_alterobjectschema(args->parsetree);
			break;
		case T_TruncateStmt:
			handled = process_truncate(args);
			break;
		case T_AlterTableStmt:
			process_altertable_start(args->parsetree);
			break;
		case T_RenameStmt:
			process_rename(args->parsetree);
			break;
		case T_IndexStmt:
			process_index_start(args->parsetree);
			break;
		case T_CreateTrigStmt:
			process_create_trigger_start(args->parsetree);
			break;
		case T_RuleStmt:
			process_create_rule_start(args->parsetree);
			break;
		case T_DropStmt:

			/*
			 * Drop associated metadata/chunks but also continue on to drop
			 * the main table. Because chunks are deleted before the main
			 * table is dropped, the drop respects CASCADE in the expected
			 * way.
			 */
			process_drop(args->parsetree);
			break;
		case T_DropTableSpaceStmt:
			process_drop_tablespace(args->parsetree);
			break;
		case T_GrantStmt:
			handled = process_grant_and_revoke(args);
			break;
		case T_GrantRoleStmt:
			handled = process_grant_and_revoke_role(args);
			break;
		case T_CopyStmt:
			handled = process_copy(args->parsetree, args->query_string, args->completion_tag);
			break;
		case T_VacuumStmt:
			handled = process_vacuum(args->parsetree, args->context);
			break;
		case T_ReindexStmt:
			handled = process_reindex(args->parsetree);
			break;
		case T_ClusterStmt:
			handled = process_cluster_start(args->parsetree, args->context);
			break;
		default:
			break;
	}

	return handled;
}

/*
 * Handle DDL commands after they've been processed by PostgreSQL.
 */
static void
process_ddl_command_end(CollectedCommand *cmd)
{
	switch (nodeTag(cmd->parsetree))
	{
		case T_CreateStmt:
			process_create_table_end(cmd->parsetree);
			break;
		case T_IndexStmt:
			process_index_end(cmd->parsetree, cmd);
			break;
		case T_AlterTableStmt:
			process_altertable_end(cmd->parsetree, cmd);
			break;
		case T_CreateTrigStmt:
			process_create_trigger_end(cmd->parsetree);
			break;
		default:
			break;
	}
}

static void
process_drop_constraint_on_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	char	   *hypertable_constraint_name = arg;
	Chunk	   *chunk = chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	/* drop both metadata and table; sql_drop won't be called recursively */
	chunk_constraint_delete_by_hypertable_constraint_name(chunk->fd.id, hypertable_constraint_name, true, true);
}

static void
process_drop_table_constraint(EventTriggerDropObject *obj)
{
	EventTriggerDropTableConstraint *constraint;
	Hypertable *ht;

	Assert(obj->type == EVENT_TRIGGER_DROP_TABLE_CONSTRAINT);
	constraint = (EventTriggerDropTableConstraint *) obj;

	/* do not use relids because underlying table could be gone */
	ht = hypertable_get_by_name(constraint->schema, constraint->table);

	if (ht != NULL)
	{
		CatalogSecurityContext sec_ctx;

		catalog_become_owner(catalog_get(), &sec_ctx);

		/* Recurse to each chunk and drop the corresponding constraint */
		foreach_chunk(ht, process_drop_constraint_on_chunk, constraint->constraint_name);

		catalog_restore_user(&sec_ctx);
	}
	else
	{
		Chunk	   *chunk = chunk_get_by_name(constraint->schema, constraint->table, 0, false);

		if (NULL != chunk)
		{
			chunk_constraint_delete_by_constraint_name(chunk->fd.id, constraint->constraint_name, true, false);
		}
	}
}

static void
process_drop_index(EventTriggerDropObject *obj)
{
	EventTriggerDropIndex *index;

	Assert(obj->type == EVENT_TRIGGER_DROP_INDEX);
	index = (EventTriggerDropIndex *) obj;

	chunk_index_delete_by_name(index->schema, index->index_name, true);
}

static void
process_drop_table(EventTriggerDropObject *obj)
{
	EventTriggerDropTable *table;

	Assert(obj->type == EVENT_TRIGGER_DROP_TABLE);
	table = (EventTriggerDropTable *) obj;

	hypertable_delete_by_name(table->schema, table->table_name);
	chunk_delete_by_name(table->schema, table->table_name);
}

static void
process_drop_schema(EventTriggerDropObject *obj)
{
	EventTriggerDropSchema *schema;
	int			count;

	Assert(obj->type == EVENT_TRIGGER_DROP_SCHEMA);
	schema = (EventTriggerDropSchema *) obj;

	if (strcmp(schema->schema, INTERNAL_SCHEMA_NAME) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop the internal schema for extension \"%s\"",
						EXTENSION_NAME),
				 errhint("Use DROP EXTENSION to remove the extension and the schema.")));

	/*
	 * Check for any remaining hypertables that use the schema as its
	 * associated schema. For matches, we reset their associated schema to the
	 * INTERNAL schema
	 */
	count = hypertable_reset_associated_schema_name(schema->schema);

	if (count > 0)
		ereport(NOTICE,
				(errmsg("the chunk storage schema changed to \"%s\" for %d hypertable%c",
						INTERNAL_SCHEMA_NAME, count, (count > 1) ? 's' : '\0')));
}

static void
process_drop_trigger_on_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	const char *trigger_name = arg;
	ObjectAddress objaddr = {
		.classId = TriggerRelationId,
		.objectId = get_trigger_oid(chunk_relid, trigger_name, true),
	};

	if (OidIsValid(objaddr.objectId))
		performDeletion(&objaddr, DROP_RESTRICT, 0);
}

static void
process_drop_trigger(EventTriggerDropObject *obj)
{
	EventTriggerDropTrigger *trigger_event;
	Hypertable *ht;

	Assert(obj->type == EVENT_TRIGGER_DROP_TRIGGER);
	trigger_event = (EventTriggerDropTrigger *) obj;

	/* do not use relids because underlying table could be gone */
	ht = hypertable_get_by_name(trigger_event->schema, trigger_event->table);

	if (ht != NULL)
	{
		/* Recurse to each chunk and drop the corresponding trigger */
		foreach_chunk(ht, process_drop_trigger_on_chunk, trigger_event->trigger_name);
	}
}


static void
process_ddl_sql_drop(EventTriggerDropObject *obj)
{
	switch (obj->type)
	{
		case EVENT_TRIGGER_DROP_TABLE_CONSTRAINT:
			process_drop_table_constraint(obj);
			break;
		case EVENT_TRIGGER_DROP_INDEX:
			process_drop_index(obj);
			break;
		case EVENT_TRIGGER_DROP_TABLE:
			process_drop_table(obj);
			break;
		case EVENT_TRIGGER_DROP_SCHEMA:
			process_drop_schema(obj);
			break;
		case EVENT_TRIGGER_DROP_TRIGGER:
			process_drop_trigger(obj);
			break;
	}
}

/*
 * ProcessUtility hook for DDL commands that have not yet been processed by
 * PostgreSQL.
 */
static void
timescaledb_ddl_command_start(
#if PG10
							  PlannedStmt *pstmt,
#elif PG96
							  Node *parsetree,
#endif
							  const char *query_string,
							  ProcessUtilityContext context,
							  ParamListInfo params,
#if PG10
							  QueryEnvironment *queryEnv,
#endif
							  DestReceiver *dest,
							  char *completion_tag)
{
	ProcessUtilityArgs args = {
		.query_string = query_string,
		.context = context,
		.params = params,
		.dest = dest,
		.completion_tag = completion_tag,
#if PG10
		.pstmt = pstmt,
		.parsetree = pstmt->utilityStmt,
		.queryEnv = queryEnv,
#elif PG96
		.parsetree = parsetree,
#endif
	};
	bool		altering_timescaledb = false;

	if (IsA(args.parsetree, AlterExtensionStmt))
	{
		AlterExtensionStmt *stmt = (AlterExtensionStmt *) args.parsetree;

		altering_timescaledb = (strcmp(stmt->extname, EXTENSION_NAME) == 0);
	}

	/*
	 * We don't want to load the extension if we just got the command to alter
	 * it.
	 */
	if (altering_timescaledb || !extension_is_loaded())
	{
		prev_ProcessUtility(&args);
		return;
	}

	if (!process_ddl_command_start(&args))
		prev_ProcessUtility(&args);
}

static
void
process_ddl_event_command_end(EventTriggerData *trigdata)
{
	ListCell   *lc;

	/* Inhibit collecting new commands while in the trigger */
	EventTriggerInhibitCommandCollection();

	switch (nodeTag(trigdata->parsetree))
	{
		case T_AlterTableStmt:
		case T_CreateTrigStmt:
		case T_CreateStmt:
		case T_IndexStmt:
			foreach(lc, event_trigger_ddl_commands())
				process_ddl_command_end(lfirst(lc));
			break;
		default:
			break;
	}

	EventTriggerUndoInhibitCommandCollection();
}

TS_FUNCTION_INFO_V1(timescaledb_process_ddl_event);
/*
 * Event trigger hook for DDL commands that have alread been handled by
 * PostgreSQL (i.e., "ddl_command_end" and "sql_drop" events).
 */
Datum
timescaledb_process_ddl_event(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata = (EventTriggerData *) fcinfo->context;
	ListCell   *lc;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	if (!extension_is_loaded())
		PG_RETURN_NULL();

	if (strcmp("ddl_command_end", trigdata->event) == 0)
		process_ddl_event_command_end(trigdata);
	else if (strcmp("sql_drop", trigdata->event) == 0)
	{
		foreach(lc, event_trigger_dropped_objects())
			process_ddl_sql_drop(lfirst(lc));
	}

	PG_RETURN_NULL();
}

extern void
process_utility_set_expect_chunk_modification(bool expect)
{
	expect_chunk_modification = expect;
}

static void
process_utility_xact_abort(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:

			/*
			 * Reset the expect_chunk_modification flag because it this is an
			 * internal safety flag that is set to true only temporarily
			 * during chunk operations. It should never remain true across
			 * transactions.
			 */
			expect_chunk_modification = false;
		default:
			break;
	}
}

static void
process_utility_subxact_abort(SubXactEvent event, SubTransactionId mySubid,
							  SubTransactionId parentSubid, void *arg)
{
	switch (event)
	{
		case SUBXACT_EVENT_ABORT_SUB:
			/* see note in process_utility_xact_abort */
			expect_chunk_modification = false;
		default:
			break;
	}
}

void
_process_utility_init(void)
{
	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = timescaledb_ddl_command_start;
	RegisterXactCallback(process_utility_xact_abort, NULL);
	RegisterSubXactCallback(process_utility_subxact_abort, NULL);
}

void
_process_utility_fini(void)
{
	ProcessUtility_hook = prev_ProcessUtility_hook;
	UnregisterXactCallback(process_utility_xact_abort, NULL);
	UnregisterSubXactCallback(process_utility_subxact_abort, NULL);
}
