#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/nodes.h>
#include <tcop/utility.h>
#include <catalog/namespace.h>
#include <catalog/pg_inherits_fn.h>
#include <catalog/index.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_trigger.h>
#include <catalog/pg_constraint_fn.h>
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
#include <parser/parse_utilcmd.h>

#include <miscadmin.h>

#include "process_utility.h"
#include "utils.h"
#include "catalog.h"
#include "hypertable_cache.h"
#include "hypercube.h"
#include "chunk_index.h"
#include "extension.h"
#include "executor.h"
#include "copy.h"
#include "chunk.h"
#include "guc.h"
#include "trigger.h"
#include "event_trigger.h"
#include "indexing.h"
#include "errors.h"
#include "guc.h"
#include "compat.h"

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

#define process_truncate_hypertable(hypertable, cascade)				\
	CatalogInternalCall3(TRUNCATE_HYPERTABLE,							\
						 NameGetDatum(&(hypertable)->fd.schema_name),	\
						 NameGetDatum(&(hypertable)->fd.table_name),	\
						 BoolGetDatum(cascade))


#define process_rename_hypertable(hypertable, new_name)					\
	CatalogInternalCall4(DDL_RENAME_HYPERTABLE,							\
						 NameGetDatum(&(hypertable)->fd.schema_name),	\
						 NameGetDatum(&(hypertable)->fd.table_name),	\
						 NameGetDatum(&(hypertable)->fd.schema_name),	\
						 DirectFunctionCall1(namein, CStringGetDatum(new_name)))

#define drop_chunk_metadata(chunk)				\
	CatalogInternalCall1(DDL_DROP_CHUNK_METADATA,				\
						 Int32GetDatum((chunk)->fd.id)) \

#define process_rename_hypertable_column(hypertable, old_name, new_name) \
	CatalogInternalCall3(DDL_RENAME_COLUMN,								\
						 Int32GetDatum((hypertable)->fd.id),			\
						 DirectFunctionCall1(namein, CStringGetDatum(old_name)), \
						 DirectFunctionCall1(namein, CStringGetDatum(new_name)))

#define process_change_hypertable_owner(hypertable, rolename )			\
	CatalogInternalCall2(DDL_CHANGE_OWNER,								\
						 ObjectIdGetDatum((hypertable)->main_table_relid), \
						 DirectFunctionCall1(namein, CStringGetDatum(rolename)))

#define process_drop_hypertable_constraint(hypertable, constraint_name) \
	CatalogInternalCall2(DDL_DROP_CONSTRAINT,							\
						 Int32GetDatum((hypertable)->fd.id),			\
						 DirectFunctionCall1(namein, CStringGetDatum(constraint_name)))

#define process_change_hypertable_column_type(hypertable, column_name, new_type) \
	CatalogInternalCall3(DDL_CHANGE_COLUMN_TYPE,						\
						 Int32GetDatum((hypertable)->fd.id),			\
						 DirectFunctionCall1(namein, CStringGetDatum(column_name)), \
						 ObjectIdGetDatum(new_type))

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
}	ProcessUtilityArgs;

/* Call the default ProcessUtility and handle PostgreSQL version differences */
static void
prev_ProcessUtility(ProcessUtilityArgs * args)
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
		RangeVar   *relation = lfirst(cell);
		Oid			relid;

		if (NULL == relation)
			continue;

		relid = RangeVarGetRelid(relation, NoLock, true);

		if (OidIsValid(relid))
		{
			Hypertable *ht = hypertable_cache_get_entry(hcache, relid);

			if (ht != NULL)
			{
				process_truncate_hypertable(ht, truncatestmt->behavior == DROP_CASCADE);
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

	if (alterstmt->objectType != OBJECT_TABLE || NULL == alterstmt->relation)
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
		return -1;

	ret = foreach_chunk(ht, process_chunk, arg);

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

	return foreach_chunk_relation(stmt->relation, vacuum_chunk, &ctx) >= 0;
}

static bool
process_drop_table(DropStmt *stmt)
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
			CatalogSecurityContext sec_ctx;

			catalog_become_owner(catalog_get(), &sec_ctx);
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
					drop_chunk_metadata(chunk);
					handled = true;
				}
			}
			catalog_restore_user(&sec_ctx);
		}
	}

	cache_release(hcache);

	return handled;
}

static void
process_drop_trigger_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	Trigger    *trigger = arg;
	Oid			trigger_oid = get_trigger_oid(chunk_relid, trigger->tgname, false);

	RemoveTriggerById(trigger_oid);
}

static void
process_drop_trigger(DropStmt *stmt)
{
	Cache	   *hcache = hypertable_cache_pin();
	ListCell   *lc;

	foreach(lc, stmt->objects)
	{
		List	   *object = list_copy(lfirst(lc));
		const char *trigname = strVal(llast(object));
		List	   *relname = list_truncate(object, list_length(object) - 1);

		if (relname != NIL)
		{
			RangeVar   *relation = makeRangeVarFromNameList(relname);
			Hypertable *ht = hypertable_cache_get_entry_rv(hcache, relation);

			if (NULL != ht)
			{
				Trigger    *trigger = trigger_by_name(ht->main_table_relid,
													  trigname,
													  stmt->missing_ok);

				if (trigger_is_chunk_trigger(trigger))
					foreach_chunk(ht, process_drop_trigger_chunk, trigger);
			}
		}
	}

	cache_release(hcache);
}

static void
process_drop_index(DropStmt *stmt)
{
	ListCell   *lc;

	foreach(lc, stmt->objects)
	{
		List	   *object = lfirst(lc);
		RangeVar   *relation = makeRangeVarFromNameList(object);
		Oid			idxrelid;

		if (NULL == relation)
			continue;

		idxrelid = RangeVarGetRelid(relation, NoLock, true);

		if (OidIsValid(idxrelid))
		{
			Oid			tblrelid = IndexGetRelation(idxrelid, false);
			Cache	   *hcache = hypertable_cache_pin();
			Hypertable *ht = hypertable_cache_get_entry(hcache, tblrelid);

			/*
			 * Drop a hypertable index, i.e., all corresponding indexes on all
			 * chunks
			 */
			if (NULL != ht)
				chunk_index_delete_children_of(ht, idxrelid, true);
			else
			{
				/* Drop an index on a chunk */
				Chunk	   *chunk = chunk_get_by_relid(tblrelid, 0, false);

				if (NULL != chunk)

					/*
					 * No need DROP the index here since DDL statement drops
					 * (hence 'false' parameter), just delete the metadata
					 */
					chunk_index_delete(chunk, idxrelid, false);
			}
		}
	}
}

static void
process_drop(Node *parsetree)
{
	DropStmt   *stmt = (DropStmt *) parsetree;

	switch (stmt->removeType)
	{
		case OBJECT_TABLE:
			process_drop_table(stmt);
			break;
		case OBJECT_TRIGGER:
			process_drop_trigger(stmt);
			break;
		case OBJECT_INDEX:
			process_drop_index(stmt);
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
						 errmsg("Reindexing of a specific index on a hypertable is currently unsupported."),
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
process_add_constraint_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	Oid			hypertable_constraint_oid = *((Oid *) arg);
	Chunk	   *chunk = chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	chunk_constraint_create_on_chunk(ht, chunk, hypertable_constraint_oid);
}

static void
process_altertable_add_constraint(Hypertable *ht, const char *constraint_name)
{
	Oid			hypertable_constraint_oid = get_relation_constraint_oid(ht->main_table_relid, constraint_name, false);

	Assert(constraint_name != NULL);

	foreach_chunk(ht, process_add_constraint_chunk, &hypertable_constraint_oid);
}

static void
process_altertable_drop_constraint(Hypertable *ht, AlterTableCmd *cmd)
{
	char	   *constraint_name = NULL;
	CatalogSecurityContext sec_ctx;
	Oid			hypertable_constraint_oid,
				hypertable_constraint_index_oid;

	constraint_name = cmd->name;
	Assert(constraint_name != NULL);

	hypertable_constraint_oid = get_relation_constraint_oid(ht->main_table_relid, constraint_name, false);
	hypertable_constraint_index_oid = get_constraint_index(hypertable_constraint_oid);

	catalog_become_owner(catalog_get(), &sec_ctx);

	process_utility_set_expect_chunk_modification(true);
	process_drop_hypertable_constraint(ht, constraint_name);
	process_utility_set_expect_chunk_modification(false);

	if (OidIsValid(hypertable_constraint_index_oid))
	{
		chunk_index_delete_children_of(ht, hypertable_constraint_index_oid, false);
	}
	catalog_restore_user(&sec_ctx);
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
				  errmsg("Foreign keys to hypertables are not supported.")));
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
		elog(ERROR, "Unexpected constraint type");
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
					 errmsg("Hypertables currently do not support concurrent "
							"index creation.")));

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
				elog(ERROR,
					 "%s:%d Operation not yet supported on hypertables: parsetree %s, type %d",
					 __FILE__, __LINE__, nodeToString(parsetree), cmd->type);
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
			 errmsg("Cannot change the type of a hash-partitioned column")));
	}
}

static void
process_alter_column_type_end(Hypertable *ht, AlterTableCmd *cmd)
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

	check_chunk_operation_allowed(relid);

	hcache = hypertable_cache_pin();
	ht = hypertable_cache_get_entry(hcache, relid);

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
			case AT_DropConstraint:
			case AT_DropConstraintRecurse:
				if (ht != NULL)
					process_altertable_drop_constraint(ht, cmd);
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
		case AT_AlterColumnType:
			Assert(IsA(cmd->def, ColumnDef));
			process_alter_column_type_end(ht, cmd);
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
		errmsg("Hypertables do not support transition tables in triggers.")));
#endif
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
		case T_IndexStmt:
			process_index_start(parsetree);
			break;
		case T_CreateTrigStmt:
			process_create_trigger_start(parsetree);
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

	/*
	 * If we are restoring, we don't want to recurse to chunks or block
	 * operations on chunks. If we do, the restore will fail.
	 */
	if (!extension_is_loaded() || guc_restoring)
	{
		prev_ProcessUtility(&args);
		return;
	}

	if (!process_ddl_command_start(args.parsetree,
								   args.query_string,
								   args.context,
								   args.completion_tag))
		prev_ProcessUtility(&args);
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
