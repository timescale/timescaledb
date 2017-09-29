#include <postgres.h>
#include <nodes/parsenodes.h>
#include <tcop/utility.h>
#include <catalog/namespace.h>
#include <catalog/pg_inherits_fn.h>
#include <catalog/index.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_trigger.h>
#include <commands/copy.h>
#include <commands/vacuum.h>
#include <commands/defrem.h>
#include <commands/trigger.h>
#include <commands/tablecmds.h>
#include <commands/event_trigger.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <utils/rel.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <miscadmin.h>

#include "process_utility.h"
#include "utils.h"
#include "hypertable_cache.h"
#include "extension.h"
#include "executor.h"
#include "metadata_queries.h"
#include "copy.h"
#include "chunk.h"
#include "guc.h"
#include "trigger.h"
#include "event_trigger.h"

void		_process_utility_init(void);
void		_process_utility_fini(void);

static ProcessUtility_hook_type prev_ProcessUtility_hook;

static bool expect_chunk_modification = false;

/* Macros for DDL upcalls to PL/pgSQL */
#define process_rename_hypertable_schema(hypertable, new_schema)		\
	CatalogInternalCall4(DDL_RENAME_HYPERTABLE,							\
						 NameGetDatum(&(hypertable)->fd.schema_name),	\
						 NameGetDatum(&(hypertable)->fd.table_name),	\
						 DirectFunctionCall1(namein, CStringGetDatum(new_schema)), \
						 NameGetDatum(&(hypertable)->fd.table_name))

#define process_drop_hypertable(hypertable, cascade)					\
	CatalogInternalCall2(DDL_DROP_HYPERTABLE,							\
						 Int32GetDatum((hypertable)->fd.id),			\
						 BoolGetDatum(cascade))

#define process_rename_hypertable(hypertable, new_name)					\
	CatalogInternalCall4(DDL_RENAME_HYPERTABLE,							\
						 NameGetDatum(&(hypertable)->fd.schema_name),	\
						 NameGetDatum(&(hypertable)->fd.table_name),	\
						 NameGetDatum(&(hypertable)->fd.schema_name),	\
						 DirectFunctionCall1(namein, CStringGetDatum(new_name)))

#define process_drop_chunk(chunk, cascade)				\
	CatalogInternalCall3(DDL_DROP_CHUNK,				\
						 Int32GetDatum((chunk)->fd.id), \
						 BoolGetDatum(cascade),			\
						 BoolGetDatum(false))

#define process_rename_hypertable_column(hypertable, old_name, new_name) \
	CatalogInternalCall3(DDL_RENAME_COLUMN,								\
						 Int32GetDatum((hypertable)->fd.id),			\
						 NameGetDatum(old_name),						\
						 NameGetDatum(new_name))

#define process_change_hypertable_owner(hypertable, rolename )			\
	CatalogInternalCall2(DDL_CHANGE_OWNER,								\
						 ObjectIdGetDatum((hypertable)->main_table_relid), \
						 DirectFunctionCall1(namein, CStringGetDatum(rolename)))

#define process_add_hypertable_constraint(hypertable, constraint_name) \
	CatalogInternalCall2(DDL_ADD_CONSTRAINT,						   \
						 Int32GetDatum((hypertable)->fd.id),		   \
						 DirectFunctionCall1(namein, CStringGetDatum(constraint_name)))

#define process_drop_hypertable_constraint(hypertable, constraint_name) \
	CatalogInternalCall2(DDL_DROP_CONSTRAINT,							\
						 Int32GetDatum((hypertable)->fd.id),			\
						 DirectFunctionCall1(namein, CStringGetDatum(constraint_name)))

#define process_change_hypertable_column_type(hypertable, column_name, new_type) \
	CatalogInternalCall3(DDL_CHANGE_COLUMN_TYPE,						\
						 Int32GetDatum((hypertable)->fd.id),			\
						 DirectFunctionCall1(namein, CStringGetDatum(column_name)), \
						 ObjectIdGetDatum(new_type))

/* Calls the default ProcessUtility */
static void
prev_ProcessUtility(Node *parsetree,
					const char *query_string,
					ProcessUtilityContext context,
					ParamListInfo params,
					DestReceiver *dest,
					char *completion_tag)
{
	if (prev_ProcessUtility_hook != NULL)
	{
		/* Call any earlier hooks */
		(prev_ProcessUtility_hook) (parsetree, query_string, context, params, dest, completion_tag);
	}
	else
	{
		/* Call the standard */
		standard_ProcessUtility(parsetree, query_string, context, params, dest, completion_tag);
	}
}

static void
check_chunk_operation_allowed(Oid relid)
{
	if (expect_chunk_modification)
		return;

	if (chunk_exists_relid(relid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Operation not supported on chunk tables.")));
	}
}

/* Truncate a hypertable */
static void
process_truncate(Node *parsetree)
{
	TruncateStmt *truncatestmt = (TruncateStmt *) parsetree;
	Cache	   *hcache = hypertable_cache_pin();
	ListCell   *cell;

	foreach(cell, truncatestmt->relations)
	{
		Oid			relId = RangeVarGetRelid(lfirst(cell), NoLock, true);

		if (OidIsValid(relId))
		{
			Hypertable *ht = hypertable_cache_get_entry(hcache, relId);

			if (ht != NULL)
			{
				executor_level_enter();
				spi_hypertable_truncate(ht);
				executor_level_exit();
			}
		}
	}
	cache_release(hcache);
}

/* Change the schema of a hypertable */
static void
process_alterobjectschema(Node *parsetree)
{
	AlterObjectSchemaStmt *alterstmt = (AlterObjectSchemaStmt *) parsetree;
	Oid			relid;
	Cache	   *hcache;
	Hypertable *ht;

	if (alterstmt->objectType != OBJECT_TABLE)
		return;

	relid = RangeVarGetRelid(alterstmt->relation, NoLock, true);

	if (!OidIsValid(relid))
		return;

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, relid);

	if (ht != NULL)
		process_rename_hypertable_schema(ht, alterstmt->newschema);

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

	if (!stmt->is_from)
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

	executor_level_enter();
	timescaledb_DoCopy(stmt, query_string, &processed, ht);
	executor_level_exit();

	processed += executor_get_additional_tuples_processed();

	if (completion_tag)
		snprintf(completion_tag, COMPLETION_TAG_BUFSIZE,
				 "COPY " UINT64_FORMAT, processed);

	cache_release(hcache);

	return true;
}

typedef void (*process_chunk_t) (Oid hypertable_relid, Oid chunk_relid, void *arg);

/*
 * Applies a function to each chunk of a hypertable.
 *
 * Returns the number of processed chunks, or -1 if the table was not a
 * hypertable.
 */
static int
foreach_chunk_relid(Oid relid, process_chunk_t process_chunk, void *arg)
{
	List	   *chunks;
	ListCell   *lc;
	int			n = 0;

	if (!OidIsValid(relid))
		/* Not a hypertable */
		return -1;

	chunks = find_inheritance_children(relid, NoLock);

	foreach(lc, chunks)
	{
		process_chunk(relid, lfirst_oid(lc), arg);
		n++;
	}

	return n;
}

static int
foreach_chunk(RangeVar *rv, process_chunk_t process_chunk, void *arg)
{
	Oid			relid = hypertable_relid(rv);

	if (!OidIsValid(relid))
		return -1;

	return foreach_chunk_relid(relid, process_chunk, arg);
}

typedef struct VacuumCtx
{
	VacuumStmt *stmt;
	bool		is_toplevel;
} VacuumCtx;

/* Vacuums a single chunk */
static void
vacuum_chunk(Oid hypertable_relid, Oid chunk_relid, void *arg)
{
	VacuumCtx  *ctx = (VacuumCtx *) arg;
	char	   *relschema = get_namespace_name(get_rel_namespace(chunk_relid));
	char	   *relname = get_rel_name(chunk_relid);

	ctx->stmt->relation->relname = relname;
	ctx->stmt->relation->schemaname = relschema;
	ExecVacuum(ctx->stmt, ctx->is_toplevel);
}

/* Vacuums each chunk of a hypertable */
static bool
process_vacuum(Node *parsetree, ProcessUtilityContext context)
{
	VacuumStmt *stmt = (VacuumStmt *) parsetree;
	VacuumCtx	ctx = {
		.stmt = stmt,
		.is_toplevel = (context == PROCESS_UTILITY_TOPLEVEL),
	};

	if (stmt->relation == NULL)
		/* Vacuum is for all tables */
		return false;

	if (!OidIsValid(hypertable_relid(stmt->relation)))
		return false;

	PreventCommandDuringRecovery((stmt->options & VACOPT_VACUUM) ?
								 "VACUUM" : "ANALYZE");

	return foreach_chunk(stmt->relation, vacuum_chunk, &ctx) >= 0;
}

static bool
process_drop_table(DropStmt *stmt)
{
	ListCell   *cell1;
	Cache	   *hcache = NULL;
	bool		handled = false;

	hcache = hypertable_cache_pin();

	foreach(cell1, stmt->objects)
	{
		List	   *object = lfirst(cell1);
		Oid			relid = RangeVarGetRelid(makeRangeVarFromNameList(object), NoLock, true);

		if (OidIsValid(relid))
		{
			Hypertable *ht;

			ht = hypertable_cache_get_entry(hcache, relid);
			if (NULL != ht)
			{
				if (list_length(stmt->objects) != 1)
					elog(ERROR, "Cannot drop a hypertable along with other objects");

				process_drop_hypertable(ht, stmt->behavior == DROP_CASCADE);
				handled = true;
			}
			else
			{
				Chunk	   *chunk = chunk_get_by_relid(relid, 0, false);

				if (chunk != NULL)
				{
					process_drop_chunk(chunk, stmt->behavior == DROP_CASCADE);
					handled = true;
				}
			}
		}
	}

	cache_release(hcache);
	return handled;
}

static void
process_drop_trigger_chunk(Oid hypertable_relid, Oid chunk_relid, void *arg)
{
	const char *trigger_name = arg;
	Oid			trigger_oid = get_trigger_oid(chunk_relid, trigger_name, false);

	RemoveTriggerById(trigger_oid);
}

static bool
process_drop_trigger(DropStmt *stmt)
{
	ListCell   *cell1;
	bool		handled = false;

	foreach(cell1, stmt->objects)
	{
		List	   *objname = lfirst(cell1);
		List	   *objargs = NIL;
		Relation	relation = NULL;
		ObjectAddress address;

		address = get_object_address(stmt->removeType,
									 objname, objargs,
									 &relation,
									 AccessExclusiveLock,
									 stmt->missing_ok);

		if (!OidIsValid(address.objectId))
		{
			Assert(stmt->missing_ok);
			continue;
		}

		if (NULL != relation)
		{
			if (is_hypertable(relation->rd_id))
			{
				Form_pg_trigger trigger = trigger_by_oid(address.objectId, stmt->missing_ok);

				if (trigger_is_chunk_trigger(trigger))
				{
					foreach_chunk_relid(relation->rd_id,
										process_drop_trigger_chunk,
										trigger_name(trigger));
					handled = true;
				}
			}

			heap_close(relation, AccessShareLock);
		}
	}

	return handled;
}

static bool
process_drop(Node *parsetree)
{
	DropStmt   *stmt = (DropStmt *) parsetree;

	switch (stmt->removeType)
	{
		case OBJECT_TABLE:
			return process_drop_table(stmt);
		case OBJECT_TRIGGER:
			return process_drop_trigger(stmt);
		default:
			return false;
	}
}

static void
reindex_chunk(Oid hypertable_relid, Oid chunk_relid, void *arg)
{
	ReindexStmt *stmt = (ReindexStmt *) arg;
	char	   *relschema = get_namespace_name(get_rel_namespace(chunk_relid));
	char	   *relname = get_rel_name(chunk_relid);

	switch (stmt->kind)
	{
		case REINDEX_OBJECT_TABLE:
			stmt->relation->relname = relname;
			stmt->relation->schemaname = relschema;
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
	Oid			relid = RangeVarGetRelid(stmt->relation, NoLock, true);

	switch (stmt->kind)
	{
		case REINDEX_OBJECT_TABLE:
			if (!is_hypertable(relid))
				return false;

			PreventCommandDuringRecovery("REINDEX");

			if (foreach_chunk(stmt->relation, reindex_chunk, stmt) >= 0)
				return true;
			break;
		case REINDEX_OBJECT_INDEX:
			if (!is_hypertable(IndexGetRelation(relid, true)))
				return false;

			/*
			 * Recursing to chunks is currently not supported. Need to look up
			 * all chunk indexes that corresponds to the hypertable's index.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Reindexing of a specific index on a hypertable is currently unsupported."),
					 errhint("As a workaround, it is possible to run REINDEX TABLE to reindex all "
			  "indexes on a hypertable, including all indexes on chunks.")));
			break;
		default:
			break;
	}

	return false;
}

static void
process_rename_table(Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht = hypertable_cache_get_entry(hcache, relid);

	if (NULL != ht)
		process_rename_hypertable(ht, stmt->newname);
}

static void
process_rename_column(Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht = hypertable_cache_get_entry(hcache, relid);
	CatalogSecurityContext sec_ctx;

	if (NULL == ht)
		return;

	catalog_become_owner(catalog_get(), &sec_ctx);
	process_rename_hypertable_column(ht, stmt->subname, stmt->newname);
	catalog_restore_user(&sec_ctx);
}

static void
process_rename(Node *parsetree)
{
	RenameStmt *stmt = (RenameStmt *) parsetree;
	Oid			relid = RangeVarGetRelid(stmt->relation, NoLock, true);
	Cache	   *hcache;

	/* TODO: forbid all rename op on chunk table */

	if (!OidIsValid(relid))
		return;

	hcache = hypertable_cache_pin();

	switch (stmt->renameType)
	{
		case OBJECT_TABLE:
			process_rename_table(hcache, relid, stmt);
			break;
		case OBJECT_COLUMN:
			process_rename_column(hcache, relid, stmt);
			break;
		default:
			break;
	}

	cache_release(hcache);
}


static void
process_altertable_change_owner(Hypertable *ht, AlterTableCmd *cmd)
{
	RoleSpec   *role;

	Assert(IsA(cmd->newowner, RoleSpec));
	role = (RoleSpec *) cmd->newowner;

	process_utility_set_expect_chunk_modification(true);
	process_change_hypertable_owner(ht, role->rolename);
	process_utility_set_expect_chunk_modification(false);
}

static void
process_altertable_add_constraint(Hypertable *ht, const char *constraint_name)
{
	Assert(constraint_name != NULL);
	process_utility_set_expect_chunk_modification(true);
	process_add_hypertable_constraint(ht, constraint_name);
	process_utility_set_expect_chunk_modification(false);
}


static void
process_altertable_drop_constraint(Hypertable *ht, AlterTableCmd *cmd)
{
	char	   *constraint_name = NULL;

	constraint_name = cmd->name;
	Assert(constraint_name != NULL);
	process_utility_set_expect_chunk_modification(true);
	process_drop_hypertable_constraint(ht, constraint_name);
	process_utility_set_expect_chunk_modification(false);
}



/* foreign-key constraints to hypertables are not allowed */
static void
verify_constraint(Constraint *constr)
{
	Assert(IsA(constr, Constraint));

	if (constr->contype == CONSTR_FOREIGN)
	{
		RangeVar   *primary_table = constr->pktable;
		Oid			primary_oid = RangeVarGetRelid(primary_table, NoLock, true);

		if (OidIsValid(primary_oid))
		{
			Cache	   *hcache = hypertable_cache_pin();
			Hypertable *ht = hypertable_cache_get_entry(hcache, primary_oid);

			if (NULL != ht)
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				  errmsg("Foreign keys to hypertables are not supported.")));
			}
			cache_release(hcache);
		}
	}
}

static void
verify_constraint_list(List *constraint_list)
{
	ListCell   *lc;

	foreach(lc, constraint_list)
	{
		Constraint *constraint = lfirst(lc);

		verify_constraint(constraint);
	}
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

	verify_constraint_list(stmt->constraints);

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
				verify_constraint_list(coldef->constraints);
				break;
			case T_Constraint:

				/*
				 * There should be no Constraints in the list after parse
				 * analysis, but this case is included anyway for completeness
				 */
				verify_constraint(lfirst(lc));
				break;
			case T_TableLikeClause:
				/* Some as above case */
				break;
			default:
				break;
		}
	}
}

/* process all regular-table alter commands to make sure they aren't adding
 * foreign-key constraints to hypertables */
static void
process_altertable_plain_table(Node *parsetree)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	ListCell   *lc;

	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_AddConstraint:
			case AT_AddConstraintRecurse:
				{
					Constraint *constraint = (Constraint *) cmd->def;

					Assert(IsA(cmd->def, Constraint));

					verify_constraint(constraint);
				}
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
process_alter_column_type(Hypertable *ht, AlterTableCmd *cmd)
{
	ColumnDef  *coldef = (ColumnDef *) cmd->def;
	Oid			new_type = TypenameGetTypid(typename_get_unqual_name(coldef->typeName));
	CatalogSecurityContext sec_ctx;

	catalog_become_owner(catalog_get(), &sec_ctx);
	process_utility_set_expect_chunk_modification(true);
	process_change_hypertable_column_type(ht, cmd->name, new_type);
	process_utility_set_expect_chunk_modification(false);
	catalog_restore_user(&sec_ctx);
}

static void
process_altertable_start(Node *parsetree)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	Oid			relid = AlterTableLookupRelation(stmt, NoLock);

	if (!OidIsValid(relid) || is_hypertable(relid))
		return;

	check_chunk_operation_allowed(relid);
	process_altertable_plain_table(parsetree);
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
					 errmsg("Hypertables currently do not support adding "
							"a constraint using an existing index.")));
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
		case AT_DropConstraint:
		case AT_DropConstraintRecurse:
			process_altertable_drop_constraint(ht, cmd);
			break;
		case AT_AlterColumnType:
			Assert(IsA(cmd->def, ColumnDef));
			process_alter_column_type(ht, cmd);
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
process_altertable_end(Node *parsetree, CollectedCommand *cmd)
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
create_trigger_chunk(Oid hypertable_relid, Oid chunk_relid, void *arg)
{
	CreateTrigStmt *stmt = arg;
	Oid			trigger_oid = get_trigger_oid(hypertable_relid, stmt->trigname, false);
	char	   *relschema = get_namespace_name(get_rel_namespace(chunk_relid));
	char	   *relname = get_rel_name(chunk_relid);

	trigger_create_on_chunk(trigger_oid, relschema, relname);
}

static void
process_create_trigger_end(Node *parsetree)
{
	CreateTrigStmt *stmt = (CreateTrigStmt *) parsetree;

	if (!stmt->row)
		return;

	foreach_chunk(stmt->relation, create_trigger_chunk, stmt);
}

/*
 * Handle DDL commands before they have been processed by PostgreSQL.
 */
static bool
process_ddl_command_start(Node *parsetree,
						  const char *query_string,
						  ProcessUtilityContext context,
						  char *completion_tag)
{
	bool		handled = false;

	switch (nodeTag(parsetree))
	{
		case T_TruncateStmt:
			process_truncate(parsetree);
			break;
		case T_AlterObjectSchemaStmt:
			process_alterobjectschema(parsetree);
			break;
		case T_AlterTableStmt:
			process_altertable_start(parsetree);
			break;
		case T_RenameStmt:
			process_rename(parsetree);
			break;
		case T_DropStmt:

			/*
			 * Drop associated metadata/chunks but also continue on to drop
			 * the main table. Because chunks are deleted before the main
			 * table is dropped, the drop respects CASCADE in the expected
			 * way.
			 */
			process_drop(parsetree);
			break;
		case T_CopyStmt:
			handled = process_copy(parsetree, query_string, completion_tag);
			break;
		case T_VacuumStmt:
			handled = process_vacuum(parsetree, context);
			break;
		case T_ReindexStmt:
			handled = process_reindex(parsetree);
			break;
		case T_ClusterStmt:

			/*
			 * CLUSTER command on hypertables do not yet recurse to chunks.
			 * This requires mapping a hypertable index to corresponding
			 * indexes on each chunk.
			 */
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

/*
 * ProcessUtility hook for DDL commands that have not yet been processed by
 * PostgreSQL.
 */
static void
timescaledb_ddl_command_start(Node *parsetree,
							  const char *query_string,
							  ProcessUtilityContext context,
							  ParamListInfo params,
							  DestReceiver *dest,
							  char *completion_tag)
{
	/*
	 * If we are restoring, we don't want to recurse to chunks or block
	 * operations on chunks. If we do, the restore will fail.
	 */
	if (!extension_is_loaded() || guc_restoring)
	{
		prev_ProcessUtility(parsetree, query_string, context, params, dest, completion_tag);
		return;
	}

	if (!process_ddl_command_start(parsetree, query_string, context, completion_tag))
		prev_ProcessUtility(parsetree, query_string, context, params, dest, completion_tag);
}

PG_FUNCTION_INFO_V1(timescaledb_ddl_command_end);

/*
 * Event trigger hook for DDL commands that have alread been handled by
 * PostgreSQL (i.e., "ddl_command_end" events).
 */
Datum
timescaledb_ddl_command_end(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata = (EventTriggerData *) fcinfo->context;
	ListCell   *lc;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	if (!extension_is_loaded() || guc_restoring)
		PG_RETURN_NULL();

	Assert(strcmp("ddl_command_end", trigdata->event) == 0);

	switch (nodeTag(trigdata->parsetree))
	{
		case T_AlterTableStmt:
		case T_CreateTrigStmt:
		case T_CreateStmt:
			foreach(lc, event_trigger_ddl_commands())
				process_ddl_command_end(lfirst(lc));
			break;
		default:
			break;
	}

	PG_RETURN_NULL();
}

extern void
process_utility_set_expect_chunk_modification(bool expect)
{
	expect_chunk_modification = expect;
}

static void
process_utility_AtEOXact_abort(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
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
	RegisterXactCallback(process_utility_AtEOXact_abort, NULL);
}

void
_process_utility_fini(void)
{
	ProcessUtility_hook = prev_ProcessUtility_hook;
}
