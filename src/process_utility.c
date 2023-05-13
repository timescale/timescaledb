/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <catalog/pg_class_d.h>
#include <postgres.h>
#include <foreign/foreign.h>
#include <nodes/parsenodes.h>
#include <nodes/nodes.h>
#include <nodes/makefuncs.h>
#include <tcop/utility.h>
#include <catalog/namespace.h>
#include <catalog/index.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_trigger.h>
#include <catalog/pg_authid.h>
#include <commands/copy.h>
#include <commands/vacuum.h>
#include <commands/defrem.h>
#include <commands/trigger.h>
#include <commands/tablecmds.h>
#include <commands/cluster.h>
#include <commands/event_trigger.h>
#include <commands/prepare.h>
#include <access/htup_details.h>
#include <commands/alter.h>
#include <access/xact.h>
#include <storage/lmgr.h>
#include <utils/acl.h>
#include <utils/rel.h>
#include <utils/inval.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/snapmgr.h>
#include <parser/parse_utilcmd.h>
#include <commands/tablespace.h>

#include <catalog/pg_constraint.h>
#include <catalog/pg_inherits.h>
#include "compat/compat.h"

#include <miscadmin.h>

#include "annotations.h"
#include "export.h"
#include "extension_constants.h"
#include "process_utility.h"
#include "ts_catalog/catalog.h"
#include "chunk.h"
#include "chunk_index.h"
#include "ts_catalog/chunk_data_node.h"
#include "compat/compat.h"
#include "copy.h"
#include "errors.h"
#include "event_trigger.h"
#include "extension.h"
#include "hypercube.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "ts_catalog/hypertable_data_node.h"
#include "dimension_vector.h"
#include "indexing.h"
#include "scan_iterator.h"
#include "time_utils.h"
#include "trigger.h"
#include "utils.h"
#include "with_clause_parser.h"
#include "cross_module_fn.h"
#include "ts_catalog/continuous_agg.h"
#include "compression_with_clause.h"
#include "partitioning.h"
#include "debug_point.h"
#include "debug_assert.h"

#ifdef USE_TELEMETRY
#include "telemetry/functions.h"
#endif

#include "cross_module_fn.h"

void _process_utility_init(void);
void _process_utility_fini(void);

static ProcessUtility_hook_type prev_ProcessUtility_hook;

static bool expect_chunk_modification = false;
static DDLResult process_altertable_set_options(AlterTableCmd *cmd, Hypertable *ht);
static DDLResult process_altertable_reset_options(AlterTableCmd *cmd, Hypertable *ht);

/* Call the default ProcessUtility and handle PostgreSQL version differences */
static void
prev_ProcessUtility(ProcessUtilityArgs *args)
{
	ProcessUtility_hook_type hook =
		prev_ProcessUtility_hook ? prev_ProcessUtility_hook : standard_ProcessUtility;

	hook(args->pstmt,
		 args->query_string,
#if PG14_GE
		 args->readonly_tree,
#endif
		 args->context,
		 args->params,
		 args->queryEnv,
		 args->dest,
		 args->completion_tag);
}

static ObjectType
get_altertable_objecttype(AlterTableStmt *stmt)
{
#if PG14_GE
	return stmt->objtype;
#else
	return stmt->relkind;
#endif
}

static ObjectType
get_createtableas_objecttype(CreateTableAsStmt *stmt)
{
#if PG14_GE
	return stmt->objtype;
#else
	return stmt->relkind;
#endif
}

static void
check_chunk_alter_table_operation_allowed(Oid relid, AlterTableStmt *stmt)
{
	if (expect_chunk_modification)
		return;

	if (ts_chunk_exists_relid(relid))
	{
		bool all_allowed = true;
		ListCell *lc;

		/* only allow if all commands are allowed */
		foreach (lc, stmt->cmds)
		{
			AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

			switch (cmd->subtype)
			{
				case AT_SetOptions:
				case AT_ResetOptions:
				case AT_SetRelOptions:
				case AT_ResetRelOptions:
				case AT_ReplaceRelOptions:
				case AT_SetStatistics:
				case AT_SetStorage:
				case AT_DropCluster:
				case AT_ClusterOn:
				case AT_EnableRowSecurity:
				case AT_DisableRowSecurity:
				case AT_SetTableSpace:
#if PG14_GE
				case AT_ReAddStatistics:
				case AT_SetCompression:
#endif
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

/* we block some ALTER commands on continuous aggregate materialization tables
 */
static void
check_continuous_agg_alter_table_allowed(Hypertable *ht, AlterTableStmt *stmt)
{
	ListCell *lc;
	ContinuousAggHypertableStatus status = ts_continuous_agg_hypertable_status(ht->fd.id);
	if ((status & HypertableIsMaterialization) == 0)
		return;

	/* only allow if all commands are allowed */
	foreach (lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_AddIndex:
			case AT_ReAddIndex:
			case AT_SetRelOptions:
			case AT_ReplicaIdentity:
				/* allowed on materialization tables */
				continue;
			default:
				/* disable by default */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("operation not supported on materialization tables")));
				break;
		}
	}
}

static void
check_alter_table_allowed_on_ht_with_compression(Hypertable *ht, AlterTableStmt *stmt)
{
	ListCell *lc;
	if (!TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
		return;

	/* only allow if all commands are allowed */
	foreach (lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			/*
			 * ALLOWED:
			 *
			 * This is a whitelist of allowed commands.
			 */
			case AT_AddIndex:
			case AT_ReAddIndex:
			case AT_ResetRelOptions:
			case AT_ReplaceRelOptions:
			case AT_SetRelOptions:
			case AT_ClusterOn:
			case AT_DropCluster:
			case AT_ChangeOwner:
				/* this is passed down in `process_altertable_change_owner` */
			case AT_SetTableSpace:
				/* this is passed down in `process_altertable_set_tablespace_end` */
			case AT_SetStatistics: /* should this be pushed down in some way? */
			case AT_AddColumn:	   /* this is passed down */
			case AT_ColumnDefault: /* this is passed down */
			case AT_DropColumn:	   /* this is passed down */
#if PG14_GE
			case AT_ReAddStatistics:
			case AT_SetCompression:
#endif
				continue;
				/*
				 * BLOCKED:
				 *
				 * List things that we want to explicitly block for documentation purposes
				 * But also block everything else as well.
				 */
			case AT_EnableRowSecurity:
			case AT_DisableRowSecurity:
			case AT_ForceRowSecurity:
			case AT_NoForceRowSecurity:
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("operation not supported on hypertables that have compression "
								"enabled")));
				break;
		}
	}
}

static void
check_altertable_add_column_for_compressed(Hypertable *ht, ColumnDef *col)
{
	if (col->constraints)
	{
		bool has_default = false;
		bool has_notnull = col->is_not_null;
		ListCell *lc;
		bool is_bool = false;
		foreach (lc, col->constraints)
		{
			Constraint *constraint = lfirst_node(Constraint, lc);
			switch (constraint->contype)
			{
				/*
				 * We can safelly ignore NULL constraints because it does nothing
				 * and according to Postgres docs is useless and exist just for
				 * compatibility with other database systems
				 * https://www.postgresql.org/docs/current/ddl-constraints.html#id-1.5.4.6.6
				 */
				case CONSTR_NULL:
					continue;
				case CONSTR_NOTNULL:
					has_notnull = true;
					continue;
				case CONSTR_DEFAULT:
					/*
					 * Since default expressions might trigger a table rewrite we
					 * only allow Const here for now.
					 */
					if (!IsA(constraint->raw_expr, A_Const))
					{
						if (IsA(constraint->raw_expr, TypeCast) &&
							IsA(castNode(TypeCast, constraint->raw_expr)->arg, A_Const))
						{
							/*
							 * Ignore error only for boolean column, as values like
							 * 'True' or 'False' are treated as TypeCast.
							 */
							char *name =
								strVal(llast(((TypeCast *) constraint->raw_expr)->typeName->names));
							is_bool = strstr(name, "bool") ? true : false;
						}
						if (!is_bool)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg(
										 "cannot add column with non-constant default expression "
										 "to a hypertable that has compression enabled")));
					}
					has_default = true;
					continue;

				default:
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot add column with constraints "
									"to a hypertable that has compression enabled")));
					break;
			}
		}
		/* require a default for columns added with NOT NULL */
		if (has_notnull && !has_default)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot add column with NOT NULL constraint without default "
							"to a hypertable that has compression enabled")));
		}
	}
	if (col->is_not_null || col->identitySequence != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot add column with constraints to a hypertable that has "
						"compression enabled")));
	}
	/* not possible to get non-null value here this is set when
	 * ALTER TABLE ALTER COLUMN ... SET TYPE < > USING ...
	 * but check anyway.
	 */
	Assert(col->raw_default == NULL && col->cooked_default == NULL);
}

static void
relation_not_only(RangeVar *rv)
{
	if (!rv->inh)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ONLY option not supported on hypertable operations")));
}

static void
add_hypertable_to_process_args(ProcessUtilityArgs *args, const Hypertable *ht)
{
	args->hypertable_list = lappend_oid(args->hypertable_list, ht->main_table_relid);
}

static bool
check_table_in_rangevar_list(List *rvlist, Name schema_name, Name table_name)
{
	ListCell *l;

	foreach (l, rvlist)
	{
		RangeVar *rvar = lfirst_node(RangeVar, l);

		if (strcmp(rvar->relname, NameStr(*table_name)) == 0 &&
			strcmp(rvar->schemaname, NameStr(*schema_name)) == 0)
			return true;
	}

	return false;
}

static void
add_chunk_oid(Hypertable *ht, Oid chunk_relid, void *vargs)
{
	ProcessUtilityArgs *args = vargs;
	GrantStmt *stmt = castNode(GrantStmt, args->parsetree);
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	/*
	 * If chunk is in the same schema as the hypertable it could already be part of
	 * the objects list in the case of "GRANT ALL IN SCHEMA" for example
	 */
	if (!check_table_in_rangevar_list(stmt->objects, &chunk->fd.schema_name, &chunk->fd.table_name))
	{
		RangeVar *rv =
			makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), -1);
		stmt->objects = lappend(stmt->objects, rv);
	}
}

static bool
block_on_foreign_server(const char *const server_name)
{
	const ForeignServer *server;

	Assert(server_name != NULL);
	server = GetForeignServerByName(server_name, true);
	if (NULL != server)
	{
		Oid ts_fdwid = get_foreign_data_wrapper_oid(EXTENSION_FDW_NAME, false);
		if (server->fdwid == ts_fdwid)
			return true;
	}
	return false;
}

static DDLResult
process_create_foreign_server_start(ProcessUtilityArgs *args)
{
	CreateForeignServerStmt *stmt = (CreateForeignServerStmt *) args->parsetree;

	if (strcmp(EXTENSION_FDW_NAME, stmt->fdwname) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("operation not supported for a TimescaleDB data node"),
				 errhint("Use add_data_node() to add data nodes to a "
						 "distributed database.")));

	return DDL_CONTINUE;
}

static void
process_drop_foreign_server_start(DropStmt *stmt)
{
	ListCell *lc;

	foreach (lc, stmt->objects)
	{
		const char *servername = strVal(lfirst(lc));

		if (block_on_foreign_server(servername))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("operation not supported on a TimescaleDB data node"),
					 errhint("Use delete_data_node() to remove data nodes from a "
							 "distributed database.")));
	}
}

static void
process_drop_trigger_start(ProcessUtilityArgs *args, DropStmt *stmt)
{
	Cache *hcache = ts_hypertable_cache_pin();
	ListCell *lc;

	foreach (lc, stmt->objects)
	{
		Node *object = lfirst(lc);
		Relation rel = NULL;
		ObjectAddress objaddr;

		/* Get the relation of the trigger */
		objaddr =
			get_object_address(stmt->removeType, object, &rel, AccessShareLock, stmt->missing_ok);

		if (OidIsValid(objaddr.objectId))
		{
			const Hypertable *ht;

			Assert(NULL != rel);
			Assert(objaddr.classId == TriggerRelationId);
			ht =
				ts_hypertable_cache_get_entry(hcache, RelationGetRelid(rel), CACHE_FLAG_MISSING_OK);

			if (NULL != ht)
				add_hypertable_to_process_args(args, ht);

			/* Lock from get_object_address must be held until transaction end */
			table_close(rel, NoLock);
		}
	}

	ts_cache_release(hcache);
}

static DDLResult
process_create_foreign_table_start(ProcessUtilityArgs *args)
{
	CreateForeignTableStmt *stmt = (CreateForeignTableStmt *) args->parsetree;

	if (block_on_foreign_server(stmt->servername))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("operation not supported"),
				 errdetail(
					 "It is not possible to create stand-alone TimescaleDB foreign tables.")));

	return DDL_CONTINUE;
}

static DDLResult
process_alter_foreign_server(ProcessUtilityArgs *args)
{
	AlterForeignServerStmt *stmt = (AlterForeignServerStmt *) args->parsetree;
	ForeignServer *server = GetForeignServerByName(stmt->servername, true);
	Oid fdwid = get_foreign_data_wrapper_oid(EXTENSION_FDW_NAME, false);
	ListCell *lc;

	if (server != NULL && server->fdwid == fdwid)
	{
		if (stmt->has_version)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("version not supported"),
					 errdetail(
						 "It is not possible to set a version on the data node configuration.")));

		/* Options are validated by the FDW, but we need to block available option
		 * since that must be handled via alter_data_node(). */
		foreach (lc, stmt->options)
		{
			DefElem *elem = lfirst(lc);

			if (strcmp(elem->defname, "available") == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot set \"available\" using ALTER SERVER"),
						 errhint("Use alter_data_node() to set \"available\".")));
		}
	}

	return DDL_CONTINUE;
}

static void
process_altertableschema(ProcessUtilityArgs *args)
{
	AlterObjectSchemaStmt *alterstmt = (AlterObjectSchemaStmt *) args->parsetree;
	Oid relid;
	Cache *hcache;
	Hypertable *ht;

	Assert(alterstmt->objectType == OBJECT_TABLE);

	if (NULL == alterstmt->relation)
		return;

	relid = RangeVarGetRelid(alterstmt->relation, NoLock, true);

	if (!OidIsValid(relid))
		return;

	ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_MISSING_OK, &hcache);

	if (ht == NULL)
	{
		Chunk *chunk = ts_chunk_get_by_relid(relid, false);

		if (NULL != chunk)
			ts_chunk_set_schema(chunk, alterstmt->newschema);
	}
	else
	{
		ts_hypertable_set_schema(ht, alterstmt->newschema);

		add_hypertable_to_process_args(args, ht);
	}

	ts_cache_release(hcache);
}

/* We use this for both materialized views and views. */
static void
process_alterviewschema(ProcessUtilityArgs *args)
{
	AlterObjectSchemaStmt *stmt = (AlterObjectSchemaStmt *) args->parsetree;
	Oid relid;
	char *schema;
	char *name;

	Assert(stmt->objectType == OBJECT_MATVIEW || stmt->objectType == OBJECT_VIEW);

	if (NULL == stmt->relation)
		return;

	relid = RangeVarGetRelid(stmt->relation, NoLock, true);
	if (!OidIsValid(relid))
		return;

	schema = get_namespace_name(get_rel_namespace(relid));
	name = get_rel_name(relid);

	ts_continuous_agg_rename_view(schema, name, stmt->newschema, name, &stmt->objectType);
}

/* Change the schema of a hypertable or a chunk */
static DDLResult
process_alterobjectschema(ProcessUtilityArgs *args)
{
	AlterObjectSchemaStmt *alterstmt = (AlterObjectSchemaStmt *) args->parsetree;

	switch (alterstmt->objectType)
	{
		case OBJECT_TABLE:
			process_altertableschema(args);
			break;
		case OBJECT_MATVIEW:
		case OBJECT_VIEW:
			process_alterviewschema(args);
			break;
		default:
			break;
	}
	return DDL_CONTINUE;
}

static DDLResult
process_copy(ProcessUtilityArgs *args)
{
	CopyStmt *stmt = (CopyStmt *) args->parsetree;

	/*
	 * Needed to add the appropriate number of tuples to the completion tag
	 */
	uint64 processed;
	Hypertable *ht = NULL;
	Cache *hcache = NULL;
	Oid relid;

	if (stmt->relation)
	{
		relid = RangeVarGetRelid(stmt->relation, NoLock, true);

		if (!OidIsValid(relid))
			return DDL_CONTINUE;

		ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_MISSING_OK, &hcache);

		if (ht == NULL)
		{
			ts_cache_release(hcache);
			return DDL_CONTINUE;
		}
	}

	/* We only copy for COPY FROM (which copies into a hypertable). Since
	 * hypertable data are in the hypertable chunks and no data would be
	 * copied, we skip the copy for COPY TO, but print an informative
	 * message. */
	if (!stmt->is_from || NULL == stmt->relation)
	{
		if (ht && stmt->relation)
			ereport(NOTICE,
					(errmsg("hypertable data are in the chunks, no data will be copied"),
					 errdetail("Data for hypertables are stored in the chunks of a hypertable so "
							   "COPY TO of a hypertable will not copy any data."),
					 errhint("Use \"COPY (SELECT * FROM <hypertable>) TO ...\" to copy all data in "
							 "hypertable, or copy each chunk individually.")));
		if (hcache)
			ts_cache_release(hcache);

		return DDL_CONTINUE;
	}

	PreventCommandIfReadOnly("COPY FROM");

	/* Performs acl check in here inside `copy_security_check` */
	timescaledb_DoCopy(stmt, args->query_string, &processed, ht);

	args->completion_tag->commandTag = CMDTAG_COPY;
	args->completion_tag->nprocessed = processed;

	add_hypertable_to_process_args(args, ht);

	ts_cache_release(hcache);

	return DDL_DONE;
}

typedef void (*process_chunk_t)(Hypertable *ht, Oid chunk_relid, void *arg);
typedef void (*mt_process_chunk_t)(int32 hypertable_id, Oid chunk_relid, void *arg);

/*
 * Applies a function to each chunk of a hypertable.
 *
 * Returns the number of processed chunks, or -1 if the table was not a
 * hypertable.
 */
static int
foreach_chunk(Hypertable *ht, process_chunk_t process_chunk, void *arg)
{
	List *chunks;
	ListCell *lc;
	int n = 0;

	if (NULL == ht)
		return -1;

	chunks = find_inheritance_children(ht->main_table_relid, NoLock);

	foreach (lc, chunks)
	{
		process_chunk(ht, lfirst_oid(lc), arg);
		n++;
	}

	return n;
}

static int
foreach_chunk_multitransaction(Oid relid, MemoryContext mctx, mt_process_chunk_t process_chunk,
							   void *arg)
{
	Cache *hcache;
	Hypertable *ht;
	int32 hypertable_id;
	List *chunks;
	ListCell *lc;
	int num_chunks = -1;

	StartTransactionCommand();
	MemoryContextSwitchTo(mctx);
	LockRelationOid(relid, AccessShareLock);

	ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_MISSING_OK, &hcache);
	if (NULL == ht)
	{
		ts_cache_release(hcache);
		CommitTransactionCommand();
		return -1;
	}

	hypertable_id = ht->fd.id;
	chunks = find_inheritance_children(ht->main_table_relid, NoLock);

	ts_cache_release(hcache);
	CommitTransactionCommand();

	num_chunks = list_length(chunks);
	foreach (lc, chunks)
	{
		process_chunk(hypertable_id, lfirst_oid(lc), arg);
	}

	list_free(chunks);

	return num_chunks;
}

typedef struct VacuumCtx
{
	VacuumRelation *ht_vacuum_rel;
	List *chunk_rels;
} VacuumCtx;

/* Adds a chunk to the list of tables to be vacuumed */
static void
add_chunk_to_vacuum(Hypertable *ht, Oid chunk_relid, void *arg)
{
	VacuumCtx *ctx = (VacuumCtx *) arg;
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
	VacuumRelation *chunk_vacuum_rel;
	RangeVar *chunk_range_var;

	chunk_range_var = copyObject(ctx->ht_vacuum_rel->relation);
	chunk_range_var->relname = NameStr(chunk->fd.table_name);
	chunk_range_var->schemaname = NameStr(chunk->fd.schema_name);
	chunk_vacuum_rel =
		makeVacuumRelation(chunk_range_var, chunk_relid, ctx->ht_vacuum_rel->va_cols);
	ctx->chunk_rels = lappend(ctx->chunk_rels, chunk_vacuum_rel);

	/* If we have a compressed chunk, make sure to analyze it as well */
	if (chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
	{
		Chunk *comp_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, false);
		/* Compressed chunk might be missing due to concurrent operations */
		if (comp_chunk)
		{
			chunk_vacuum_rel = makeVacuumRelation(NULL, comp_chunk->table_id, NIL);
			ctx->chunk_rels = lappend(ctx->chunk_rels, chunk_vacuum_rel);
		}
	}
}

/*
 * Construct a list of VacuumRelations for all vacuumable rels in
 * the current database.  This is similar to the PostgresQL get_all_vacuum_rels
 * from vacuum.c, only it filters out distributed hypertables and chunks
 * that have been compressed.
 */
static List *
ts_get_all_vacuum_rels(bool is_vacuumcmd)
{
	List *vacrels = NIL;
	Relation pgclass;
	TableScanDesc scan;
	HeapTuple tuple;
	Cache *hcache = ts_hypertable_cache_pin();

	pgclass = table_open(RelationRelationId, AccessShareLock);

	scan = table_beginscan_catalog(pgclass, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classform = (Form_pg_class) GETSTRUCT(tuple);
		Hypertable *ht;
		Oid relid;

		relid = classform->oid;

		/* check permissions of relation */
		if (!vacuum_is_relation_owner(relid,
									  classform,
									  is_vacuumcmd ? VACOPT_VACUUM : VACOPT_ANALYZE))
			continue;

		/*
		 * We include partitioned tables here; depending on which operation is
		 * to be performed, caller will decide whether to process or ignore
		 * them.
		 */
		if (classform->relkind != RELKIND_RELATION && classform->relkind != RELKIND_MATVIEW &&
			classform->relkind != RELKIND_PARTITIONED_TABLE)
			continue;

		ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);
		if (ht)
			if (hypertable_is_distributed(ht))
				continue;

		/*
		 * Build VacuumRelation(s) specifying the table OIDs to be processed.
		 * We omit a RangeVar since it wouldn't be appropriate to complain
		 * about failure to open one of these relations later.
		 */
		vacrels = lappend(vacrels, makeVacuumRelation(NULL, relid, NIL));
	}

	table_endscan(scan);
	table_close(pgclass, AccessShareLock);
	ts_cache_release(hcache);

	return vacrels;
}

/* Vacuums/Analyzes a hypertable and all of it's chunks */
static DDLResult
process_vacuum(ProcessUtilityArgs *args)
{
	VacuumStmt *stmt = (VacuumStmt *) args->parsetree;
	bool is_toplevel = (args->context == PROCESS_UTILITY_TOPLEVEL);
	VacuumCtx ctx = {
		.ht_vacuum_rel = NULL,
		.chunk_rels = NIL,
	};
	ListCell *lc;
	Hypertable *ht;
	List *vacuum_rels = NIL;
	bool is_vacuumcmd;
	/* save original VacuumRelation list */
	List *saved_stmt_rels = stmt->rels;

	is_vacuumcmd = stmt->is_vacuumcmd;

	if (stmt->rels == NIL)
		vacuum_rels = ts_get_all_vacuum_rels(is_vacuumcmd);
	else
	{
		Cache *hcache = ts_hypertable_cache_pin();

		foreach (lc, stmt->rels)
		{
			VacuumRelation *vacuum_rel = lfirst_node(VacuumRelation, lc);
			Oid table_relid = vacuum_rel->oid;

			if (!OidIsValid(table_relid) && vacuum_rel->relation != NULL)
				table_relid = RangeVarGetRelid(vacuum_rel->relation, NoLock, true);

			if (OidIsValid(table_relid))
			{
				ht = ts_hypertable_cache_get_entry(hcache, table_relid, CACHE_FLAG_MISSING_OK);

				if (ht)
				{
					add_hypertable_to_process_args(args, ht);

					/* Exclude distributed hypertables from the list of relations
					 * to vacuum and analyze since they contain no local tuples.
					 *
					 * Support for VACUUM/ANALYZE operations on a distributed hypertable
					 * is implemented as a part of distributed ddl and remote
					 * statistics import functions.
					 */
					if (hypertable_is_distributed(ht))
						continue;
					ctx.ht_vacuum_rel = vacuum_rel;
					foreach_chunk(ht, add_chunk_to_vacuum, &ctx);
				}
			}
			vacuum_rels = lappend(vacuum_rels, vacuum_rel);
		}
		ts_cache_release(hcache);
	}

	stmt->rels = list_concat(ctx.chunk_rels, vacuum_rels);

	/* The list of rels to vacuum could be empty if we are only vacuuming a
	 * distributed hypertable. In that case, we don't want to vacuum locally. */
	if (list_length(stmt->rels) > 0)
	{
		PreventCommandDuringRecovery(is_vacuumcmd ? "VACUUM" : "ANALYZE");

		/* ACL permission checks inside vacuum_rel and analyze_rel called by this ExecVacuum */
		ExecVacuum(args->parse_state, stmt, is_toplevel);
	}
	/*
	Restore original list. stmt->rels which has references to
	VacuumRelation list is freed up, however VacuumStmt is not
	cleaned up because of which there is a crash.
	*/
	stmt->rels = saved_stmt_rels;
	return DDL_DONE;
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
	return rv->inh;
}

/* handle forwarding TRUNCATEs to the chunks of a hypertable */
static void
handle_truncate_hypertable(ProcessUtilityArgs *args, TruncateStmt *stmt, Hypertable *ht)
{
	add_hypertable_to_process_args(args, ht);

	/* Delete the metadata */
	ts_chunk_delete_by_hypertable_id(ht->fd.id);

	/* Drop the chunk tables */
	foreach_chunk(ht, process_truncate_chunk, stmt);
}

/*
 * Truncate a hypertable.
 */
static DDLResult
process_truncate(ProcessUtilityArgs *args)
{
	TruncateStmt *stmt = (TruncateStmt *) args->parsetree;
	Cache *hcache = ts_hypertable_cache_pin();
	ListCell *cell;
	List *hypertables = NIL;
	List *relations = NIL;
	bool list_changed = false;
	MemoryContext oldctx, parsetreectx = GetMemoryChunkContext(args->parsetree);

	/* For all hypertables, we drop the now empty chunks. We also propagate the
	 * TRUNCATE call to the compressed version of the hypertable, if it exists.
	 */
	/* Preprocess and filter out distributed hypertables */
	foreach (cell, stmt->relations)
	{
		RangeVar *rv = lfirst(cell);
		Oid relid;
		bool list_append = false;

		if (!rv)
			continue;

		/* Grab AccessExclusiveLock, same as regular TRUNCATE processing grabs
		 * below. We just do it preemptively here. */
		relid = RangeVarGetRelid(rv, AccessExclusiveLock, true);

		if (!OidIsValid(relid))
		{
			/* We should add invalid relations to the list to raise error on the
			 * standard_ProcessUtility when we're trying to TRUNCATE a nonexistent relation */
			list_append = true;
		}
		else
		{
			switch (get_rel_relkind(relid))
			{
				case RELKIND_VIEW:
				{
					ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(relid);

					if (cagg)
					{
						Hypertable *mat_ht, *raw_ht;

						if (!relation_should_recurse(rv))
							ereport(ERROR,
									(errcode(ERRCODE_WRONG_OBJECT_TYPE),
									 errmsg("cannot truncate only a continuous aggregate")));

						mat_ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);
						Assert(mat_ht != NULL);

						/* Create list item into the same context of the list */
						oldctx = MemoryContextSwitchTo(parsetreectx);
						rv = makeRangeVar(NameStr(mat_ht->fd.schema_name),
										  NameStr(mat_ht->fd.table_name),
										  -1);
						MemoryContextSwitchTo(oldctx);

						/* Invalidate the entire continuous aggregate since it no
						 * longer has any data */
						raw_ht = ts_hypertable_get_by_id(cagg->data.raw_hypertable_id);
						Assert(raw_ht != NULL);
						ts_cm_functions->continuous_agg_invalidate_mat_ht(raw_ht,
																		  mat_ht,
																		  TS_TIME_NOBEGIN,
																		  TS_TIME_NOEND);

						/* Additionally, this cagg's materialization hypertable could be the
						 * underlying hypertable for other caggs defined on top of it, in that case
						 * we must update the hypertable invalidation log */
						ContinuousAggHypertableStatus agg_status;

						agg_status = ts_continuous_agg_hypertable_status(mat_ht->fd.id);
						if (agg_status & HypertableIsRawTable)
							ts_cm_functions->continuous_agg_invalidate_raw_ht(mat_ht,
																			  TS_TIME_NOBEGIN,
																			  TS_TIME_NOEND);

						/* mark list as changed because we'll add the materialization hypertable */
						list_changed = true;
					}

					list_append = true;
					break;
				}
				case RELKIND_RELATION:
				/* TRUNCATE for foreign tables not implemented yet. This will raise an error. */
				case RELKIND_FOREIGN_TABLE:
				{
					Hypertable *ht =
						ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);
					Chunk *chunk;

					if (ht)
					{
						ContinuousAggHypertableStatus agg_status;

						agg_status = ts_continuous_agg_hypertable_status(ht->fd.id);

						if ((agg_status & HypertableIsMaterialization) != 0)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("cannot TRUNCATE a hypertable underlying a continuous "
											"aggregate"),
									 errhint("TRUNCATE the continuous aggregate instead.")));

						if (agg_status == HypertableIsRawTable)
						{
							/* The truncation invalidates all associated continuous aggregates */
							ts_cm_functions->continuous_agg_invalidate_raw_ht(ht,
																			  TS_TIME_NOBEGIN,
																			  TS_TIME_NOEND);
						}

						if (!relation_should_recurse(rv))
							ereport(ERROR,
									(errcode(ERRCODE_WRONG_OBJECT_TYPE),
									 errmsg("cannot truncate only a hypertable"),
									 errhint("Do not specify the ONLY keyword, or use truncate"
											 " only on the chunks directly.")));

						hypertables = lappend(hypertables, ht);

						if (!hypertable_is_distributed(ht))
							list_append = true;
						else
							/* mark list as changed because we'll not add the distributed hypertable
							 */
							list_changed = true;
					}
					else if ((chunk = ts_chunk_get_by_relid(relid, false)) != NULL)
					{ /* this is a chunk */
						ht = ts_hypertable_cache_get_entry(hcache,
														   chunk->hypertable_relid,
														   CACHE_FLAG_NONE);

						/*
						 * Block direct TRUNCATE on frozen chunk.
						 */
#if PG14_GE
						if (ts_chunk_is_frozen(chunk))
							elog(ERROR, "cannot TRUNCATE frozen chunk \"%s\"", get_rel_name(relid));
#endif

						Assert(ht != NULL);

						/* If the hypertable has continuous aggregates, then invalidate
						 * the truncated region. */
						if (ts_continuous_agg_hypertable_status(ht->fd.id) == HypertableIsRawTable)
							ts_continuous_agg_invalidate_chunk(ht, chunk);
						/* Truncate the compressed chunk too. */
						if (chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
						{
							Chunk *compressed_chunk =
								ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, false);
							if (compressed_chunk != NULL && !compressed_chunk->fd.dropped)
							{
								/* Create list item into the same context of the list. */
								oldctx = MemoryContextSwitchTo(parsetreectx);
								rv = makeRangeVar(NameStr(compressed_chunk->fd.schema_name),
												  NameStr(compressed_chunk->fd.table_name),
												  -1);
								MemoryContextSwitchTo(oldctx);
								list_changed = true;
							}
						}
						list_append = true;
					}
					else
						list_append = true;
					break;
				}
			}
		}

		/* Append the relation to the list in the same parse tree memory context */
		if (list_append)
		{
			MemoryContext oldctx = MemoryContextSwitchTo(parsetreectx);
			relations = lappend(relations, rv);
			MemoryContextSwitchTo(oldctx);
		}
	}

	/* Update relations list just when changed to include only tables
	 * that hold data. On an access node, distributed hypertables hold
	 * no data and chunks are foreign tables, so those tables are excluded. */
	if (list_changed)
		stmt->relations = relations;

	if (stmt->relations != NIL)
	{
		/* Call standard PostgreSQL handler for remaining tables */
		prev_ProcessUtility(args);
	}

	/* For all hypertables, we drop the now empty chunks */
	foreach (cell, hypertables)
	{
		Hypertable *ht = lfirst(cell);

		Assert(ht != NULL);

		handle_truncate_hypertable(args, stmt, ht);

		/* propagate to the compressed hypertable */
		if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
		{
			Hypertable *compressed_ht =
				ts_hypertable_cache_get_entry_by_id(hcache, ht->fd.compressed_hypertable_id);
			TruncateStmt compressed_stmt = *stmt;
			compressed_stmt.relations =
				list_make1(makeRangeVar(NameStr(compressed_ht->fd.schema_name),
										NameStr(compressed_ht->fd.table_name),
										-1));

			/* TRUNCATE the compressed hypertable */
			ExecuteTruncate(&compressed_stmt);

			handle_truncate_hypertable(args, stmt, compressed_ht);
		}
	}

	ts_cache_release(hcache);

	return DDL_DONE;
}

static void
process_drop_table_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	DropStmt *stmt = arg;
	ObjectAddress objaddr = {
		.classId = RelationRelationId,
		.objectId = chunk_relid,
	};

	performDeletion(&objaddr, stmt->behavior, 0);
}

/* Block drop compressed chunks directly and drop corresponding compressed chunks if
 * cascade is on. */
static void
process_drop_chunk(ProcessUtilityArgs *args, DropStmt *stmt)
{
	ListCell *lc;
	Cache *hcache = ts_hypertable_cache_pin();

	foreach (lc, stmt->objects)
	{
		List *object = lfirst(lc);
		RangeVar *relation = makeRangeVarFromNameList(object);
		Oid relid;
		Chunk *chunk;

		if (NULL == relation)
			continue;

		relid = RangeVarGetRelid(relation, NoLock, true);
		chunk = ts_chunk_get_by_relid(relid, false);

		if (chunk != NULL)
		{
			Hypertable *ht;

			if (ts_chunk_contains_compressed_data(chunk))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("dropping compressed chunks not supported"),
						 errhint(
							 "Please drop the corresponding chunk on the uncompressed hypertable "
							 "instead.")));

			/* if cascade is enabled, delete the compressed chunk with cascade too. Otherwise
			 *  it would be blocked if there are dependent objects */
			if (stmt->behavior == DROP_CASCADE && chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
			{
				Chunk *compressed_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, false);
				/* The chunk may have been delete by a CASCADE */
				if (compressed_chunk != NULL)
					ts_chunk_drop(compressed_chunk, stmt->behavior, DEBUG1);
			}

			ht = ts_hypertable_cache_get_entry(hcache, chunk->hypertable_relid, CACHE_FLAG_NONE);

			Assert(ht != NULL);

			/* If the hypertable has continuous aggregates, then invalidate
			 * the dropped region. */
			if (ts_continuous_agg_hypertable_status(ht->fd.id) == HypertableIsRawTable)
				ts_continuous_agg_invalidate_chunk(ht, chunk);
		}
	}

	ts_cache_release(hcache);
}

/*
 * We need to drop hypertable chunks and associated compressed hypertables
 * when dropping hypertables to maintain correct semantics wrt CASCADE modifiers.
 * Also block dropping compressed hypertables directly.
 */
static DDLResult
process_drop_hypertable(ProcessUtilityArgs *args, DropStmt *stmt)
{
	Cache *hcache = ts_hypertable_cache_pin();
	ListCell *lc;
	DDLResult result = DDL_CONTINUE;

	foreach (lc, stmt->objects)
	{
		List *object = lfirst(lc);
		RangeVar *relation = makeRangeVarFromNameList(object);
		Oid relid;

		if (NULL == relation)
			continue;

		relid = RangeVarGetRelid(relation, NoLock, true);

		if (OidIsValid(relid))
		{
			Hypertable *ht;

			ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);

			if (NULL != ht)
			{
				if (list_length(stmt->objects) != 1)
					elog(ERROR, "cannot drop a hypertable along with other objects");

				if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("dropping compressed hypertables not supported"),
							 errhint("Please drop the corresponding uncompressed hypertable "
									 "instead.")));

				/*
				 *  We need to drop hypertable chunks before the hypertable to avoid the need
				 *  to CASCADE such drops;
				 */
				foreach_chunk(ht, process_drop_table_chunk, stmt);
				/* The usual path for deleting an associated compressed hypertable uses
				 * DROP_RESTRICT But if we are using DROP_CASCADE we should propagate that down to
				 * the compressed hypertable.
				 */
				if (stmt->behavior == DROP_CASCADE && TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
				{
					Hypertable *compressed_hypertable =
						ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
					ts_hypertable_drop(compressed_hypertable, DROP_CASCADE);
				}
			}

			result = DDL_DONE;
		}
	}

	ts_cache_release(hcache);

	return result;
}

/*
 *  We need to ensure that DROP INDEX uses only one hypertable per query,
 *  otherwise query string might not be reusable for execution on a
 *  data node.
 */
static void
process_drop_hypertable_index(ProcessUtilityArgs *args, DropStmt *stmt)
{
	Cache *hcache = ts_hypertable_cache_pin();
	ListCell *lc;

	foreach (lc, stmt->objects)
	{
		List *object = lfirst(lc);
		RangeVar *relation = makeRangeVarFromNameList(object);
		Oid relid;
		Hypertable *ht;

		if (NULL == relation)
			continue;

		relid = RangeVarGetRelid(relation, NoLock, true);
		if (!OidIsValid(relid))
			continue;

		relid = IndexGetRelation(relid, true);
		if (!OidIsValid(relid))
			continue;

		ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);
		if (NULL != ht)
		{
			if (list_length(stmt->objects) != 1)
				elog(ERROR, "cannot drop a hypertable index along with other objects");

			add_hypertable_to_process_args(args, ht);
		}
	}

	ts_cache_release(hcache);
}

/* Note that DROP TABLESPACE does not have a hook in event triggers so cannot go
 * through process_ddl_sql_drop */
static DDLResult
process_drop_tablespace(ProcessUtilityArgs *args)
{
	DropTableSpaceStmt *stmt = (DropTableSpaceStmt *) args->parsetree;
	int count = ts_tablespace_count_attached(stmt->tablespacename);

	if (count > 0)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("tablespace \"%s\" is still attached to %d hypertables",
						stmt->tablespacename,
						count),
				 errhint("Detach the tablespace from all hypertables before removing it.")));

	return DDL_CONTINUE;
}

static void
process_grant_add_by_rel(GrantStmt *stmt, RangeVar *relation)
{
	stmt->objects = lappend(stmt->objects, relation);
}

/*
 * If it is a "GRANT/REVOKE ON ALL TABLES IN SCHEMA" operation then we need to check if
 * the rangevar was already added when we added all objects inside the SCHEMA
 *
 * This could get a little expensive for schemas containing a lot of objects..
 */
static void
process_grant_add_by_name(GrantStmt *stmt, bool was_schema_op, Name schema_name, Name table_name)
{
	bool already_added = false;

	if (was_schema_op)
		already_added = check_table_in_rangevar_list(stmt->objects, schema_name, table_name);

	if (!already_added)
		process_grant_add_by_rel(stmt,
								 makeRangeVar(NameStr(*schema_name), NameStr(*table_name), -1));
}

static void
process_relations_in_namespace(GrantStmt *stmt, Name schema_name, Oid namespaceId, char relkind)
{
	ScanKeyData key[2];
	Relation rel;
	TableScanDesc scan;
	HeapTuple tuple;

	ScanKeyInit(&key[0],
				Anum_pg_class_relnamespace,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(namespaceId));
	ScanKeyInit(&key[1],
				Anum_pg_class_relkind,
				BTEqualStrategyNumber,
				F_CHAREQ,
				CharGetDatum(relkind));

	rel = table_open(RelationRelationId, AccessShareLock);
	scan = table_beginscan_catalog(rel, 2, key);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Name relname = palloc(NAMEDATALEN);
		namestrcpy(relname, NameStr(((Form_pg_class) GETSTRUCT(tuple))->relname));

		/* these are being added for the first time into this list */
		process_grant_add_by_name(stmt, false, schema_name, relname);
	}

	table_endscan(scan);
	table_close(rel, AccessShareLock);
}

/*
 * For "GRANT ALL ON ALL TABLES IN SCHEMA" GrantStmt, the targtype field is ACL_TARGET_ALL_IN_SCHEMA
 * whereas in regular "GRANT ON TABLE table_name", the targtype field is ACL_TARGET_OBJECT. In the
 * latter case the objects list contains a list of relation range vars whereas in the former it is
 * the list of schema names.
 *
 * To make things work we change the targtype field from ACL_TARGET_ALL_IN_SCHEMA to
 * ACL_TARGET_OBJECT and then create a new list of rangevars of all relation type entities in it and
 * assign it to the "stmt->objects" field.
 *
 */
static void
process_grant_add_by_schema(GrantStmt *stmt)
{
	ListCell *cell;
	List *nspnames = stmt->objects;

	/*
	 * We will be adding rangevars to the "stmt->objects" field in the loop below. So
	 * we track the nspnames separately above and NIL out the objects list
	 */
	stmt->objects = NIL;
	foreach (cell, nspnames)
	{
		char *nspname = strVal(lfirst(cell));
		Oid namespaceId = LookupExplicitNamespace(nspname, false);
		Name schema;

		schema = (Name) palloc(NAMEDATALEN);

		namestrcpy(schema, nspname);

		/* Inspired from objectsInSchemaToOids PG function */
		process_relations_in_namespace(stmt, schema, namespaceId, RELKIND_RELATION);
		process_relations_in_namespace(stmt, schema, namespaceId, RELKIND_VIEW);
		process_relations_in_namespace(stmt, schema, namespaceId, RELKIND_MATVIEW);
		process_relations_in_namespace(stmt, schema, namespaceId, RELKIND_FOREIGN_TABLE);
		process_relations_in_namespace(stmt, schema, namespaceId, RELKIND_PARTITIONED_TABLE);
	}

	/* change targtype to ACL_TARGET_OBJECT now */
	stmt->targtype = ACL_TARGET_OBJECT;
}

/*
 * Handle GRANT / REVOKE.
 *
 * A revoke is a GrantStmt with 'is_grant' set to false.
 */
static DDLResult
process_grant_and_revoke(ProcessUtilityArgs *args)
{
	GrantStmt *stmt = (GrantStmt *) args->parsetree;
	DDLResult result = DDL_CONTINUE;

	/* We let the calling function handle anything that is not
	 * ACL_TARGET_OBJECT or ACL_TARGET_ALL_IN_SCHEMA */
	if (stmt->targtype != ACL_TARGET_OBJECT && stmt->targtype != ACL_TARGET_ALL_IN_SCHEMA)
		return DDL_CONTINUE;

	switch (stmt->objtype)
	{
		case OBJECT_TABLESPACE:
			/*
			 * If we are granting on a tablespace, we need to apply the REVOKE
			 * first to be able to check remaining permissions.
			 */
			prev_ProcessUtility(args);
			ts_tablespace_validate_revoke(stmt);
			result = DDL_DONE;
			break;

		case OBJECT_TABLE:
			/*
			 * Collect the hypertables in the grant statement. We only need to
			 * consider those when sending grants to other data nodes.
			 */
			{
				Cache *hcache;
				ListCell *cell;
				List *saved_schema_objects = NIL;
				bool was_schema_op = false;

				/*
				 * If it's a GRANT/REVOKE ALL IN SCHEMA then we need to collect all
				 * objects in this schema and convert this into an ACL_TARGET_OBJECT
				 * entry with its objects field pointing to rangevars
				 */
				if (stmt->targtype == ACL_TARGET_ALL_IN_SCHEMA)
				{
					saved_schema_objects = stmt->objects;
					process_grant_add_by_schema(stmt);
					was_schema_op = true;
				}

				hcache = ts_hypertable_cache_pin();
				/* First process all continuous aggregates in the list and add
				 * the associated hypertables and views to the list of objects
				 * to process */
				foreach (cell, stmt->objects)
				{
					RangeVar *relation = lfirst_node(RangeVar, cell);
					ContinuousAgg *const cagg = ts_continuous_agg_find_by_rv(relation);
					if (cagg)
					{
						Hypertable *mat_hypertable =
							ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);
						process_grant_add_by_name(stmt,
												  was_schema_op,
												  &mat_hypertable->fd.schema_name,
												  &mat_hypertable->fd.table_name);
						process_grant_add_by_name(stmt,
												  was_schema_op,
												  &cagg->data.direct_view_schema,
												  &cagg->data.direct_view_name);
						process_grant_add_by_name(stmt,
												  was_schema_op,
												  &cagg->data.partial_view_schema,
												  &cagg->data.partial_view_name);
					}

					/*
					 * If this is a hypertable and it has a compressed
					 * hypertable associated with it, add it to the list of
					 * hypertables to process.
					 */
					Hypertable *hypertable = ts_hypertable_cache_get_entry_rv(hcache, relation);
					if (hypertable && TS_HYPERTABLE_HAS_COMPRESSION_TABLE(hypertable))
					{
						Hypertable *compressed_hypertable =
							ts_hypertable_get_by_id(hypertable->fd.compressed_hypertable_id);
						Assert(compressed_hypertable);
						process_grant_add_by_name(stmt,
												  was_schema_op,
												  &compressed_hypertable->fd.schema_name,
												  &compressed_hypertable->fd.table_name);
					}
				}

				/* Process all hypertables, including those added in the loop above */
				foreach (cell, stmt->objects)
				{
					RangeVar *relation = lfirst_node(RangeVar, cell);
					Hypertable *ht = ts_hypertable_cache_get_entry_rv(hcache, relation);

					if (ht)
					{
						add_hypertable_to_process_args(args, ht);
						foreach_chunk(ht, add_chunk_oid, args);
					}
				}

				ts_cache_release(hcache);

				/* Execute command right away, to check any permission errors before propagating
				 * it to the distributed DDL */
				result = DDL_DONE;
				if (stmt->objects != NIL)
					prev_ProcessUtility(args);

				/* Restore ALL IN SCHEMA command type and it's objects */
				if (was_schema_op)
				{
					stmt->targtype = ACL_TARGET_ALL_IN_SCHEMA;
					stmt->objects = saved_schema_objects;
				}

				break;
			}
		default:
			break;
	}

	return result;
}

static DDLResult
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
		return DDL_DONE;

	ts_tablespace_validate_revoke_role(stmt);

	return DDL_DONE;
}

static void
process_drop_view_start(ProcessUtilityArgs *args, DropStmt *stmt)
{
	ListCell *cell;
	foreach (cell, stmt->objects)
	{
		List *const object = lfirst(cell);
		RangeVar *const rv = makeRangeVarFromNameList(object);
		ContinuousAgg *const cagg = ts_continuous_agg_find_by_rv(rv);

		if (cagg)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot drop continuous aggregate using DROP VIEW"),
					 errhint("Use DROP MATERIALIZED VIEW to drop a continuous aggregate.")));
	}
}

static void
process_drop_continuous_aggregates(ProcessUtilityArgs *args, DropStmt *stmt)
{
	ListCell *lc;
	int caggs_count = 0;

	foreach (lc, stmt->objects)
	{
		List *const object = lfirst(lc);
		RangeVar *const rv = makeRangeVarFromNameList(object);
		ContinuousAgg *const cagg = ts_continuous_agg_find_by_rv(rv);

		if (cagg)
		{
			/* If there is at least one cagg, the drop should be treated as a
			 * DROP VIEW. */
			stmt->removeType = OBJECT_VIEW;
			++caggs_count;
		}
	}

	/* We check that there were only continuous aggregates or that there were
	   no continuous aggregates. Otherwise, we have a mixture of tables and
	   views and are looking for views only.*/
	if (caggs_count > 0 && caggs_count < list_length(stmt->objects))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("mixing continuous aggregates and other objects not allowed"),
				 errhint("Drop continuous aggregates and other objects in separate statements.")));
}

static bool
fetch_role_info(RoleSpec *rolespec, Oid *roleid)
{
	/* Special role specifiers should not be present when dropping a role,
	 * but if they are, we just ignore them */
	if (rolespec->roletype != ROLESPEC_CSTRING)
		return false;

	/* Fetch the heap tuple from system table. If heaptuple is not valid it
	 * means we did not find a role. We ignore it since the real execution
	 * will handle this. */
	HeapTuple tuple = SearchSysCache1(AUTHNAME, PointerGetDatum(rolespec->rolename));
	if (!HeapTupleIsValid(tuple))
		return false;

	Form_pg_authid roleform = (Form_pg_authid) GETSTRUCT(tuple);
	*roleid = roleform->oid;
	ReleaseSysCache(tuple);
	return true;
}

static DDLResult
process_drop_role(ProcessUtilityArgs *args)
{
	DropRoleStmt *stmt = (DropRoleStmt *) args->parsetree;
	ListCell *cell;
	foreach (cell, stmt->roles)
	{
		RoleSpec *rolespec = lfirst(cell);
		Oid roleid;

		if (!fetch_role_info(rolespec, &roleid))
			continue;

		ScanIterator iterator =
			ts_scan_iterator_create(BGW_JOB, AccessShareLock, CurrentMemoryContext);
		ts_scanner_foreach(&iterator)
		{
			bool isnull;
			TupleInfo *ti = ts_scan_iterator_tuple_info(&iterator);
			Datum value = slot_getattr(ti->slot, Anum_bgw_job_owner, &isnull);
			if (!isnull && DatumGetObjectId(value) == roleid)
			{
				Datum value = slot_getattr(ti->slot, Anum_bgw_job_id, &isnull);
				Ensure(!isnull, "job id was null");
				ereport(ERROR,
						(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
						 errmsg("role \"%s\" cannot be dropped because some objects depend on it",
								rolespec->rolename),
						 errdetail("owner of job %d", DatumGetInt32(value))));
			}
		}
	}
	return DDL_CONTINUE;
}

static DDLResult
process_drop_start(ProcessUtilityArgs *args)
{
	DropStmt *stmt = (DropStmt *) args->parsetree;

	switch (stmt->removeType)
	{
		case OBJECT_TABLE:
			process_drop_hypertable(args, stmt);
			TS_FALLTHROUGH;
		case OBJECT_FOREIGN_TABLE:
			/* Chunks can be either normal tables, or foreign tables in the case of a distributed
			 * hypertable */
			process_drop_chunk(args, stmt);
			break;
		case OBJECT_INDEX:
			process_drop_hypertable_index(args, stmt);
			break;
		case OBJECT_MATVIEW:
			process_drop_continuous_aggregates(args, stmt);
			break;
		case OBJECT_VIEW:
			process_drop_view_start(args, stmt);
			break;
		case OBJECT_FOREIGN_SERVER:
			process_drop_foreign_server_start(stmt);
			break;
		case OBJECT_TRIGGER:
			process_drop_trigger_start(args, stmt);
			break;
		default:
			break;
	}

	return DDL_CONTINUE;
}

static void
reindex_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	ProcessUtilityArgs *args = arg;
	ReindexStmt *stmt = (ReindexStmt *) args->parsetree;
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);

	switch (stmt->kind)
	{
		case REINDEX_OBJECT_TABLE:
			stmt->relation->relname = NameStr(chunk->fd.table_name);
			stmt->relation->schemaname = NameStr(chunk->fd.schema_name);
#if PG14_LT
			ReindexTable(stmt->relation,
						 get_reindex_options(stmt),
						 stmt->concurrent /* should test for deadlocks */
			);
#elif PG14_GE
			ExecReindex(NULL, stmt, false);
#endif
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
static DDLResult
process_reindex(ProcessUtilityArgs *args)
{
	ReindexStmt *stmt = (ReindexStmt *) args->parsetree;
	Oid relid;
	Cache *hcache;
	Hypertable *ht;
	DDLResult result = DDL_CONTINUE;

	if (NULL == stmt->relation)
		/* Not a case we are interested in */
		return DDL_CONTINUE;

	relid = RangeVarGetRelid(stmt->relation, NoLock, true);

	if (!OidIsValid(relid))
		return DDL_CONTINUE;

	hcache = ts_hypertable_cache_pin();

	switch (stmt->kind)
	{
		case REINDEX_OBJECT_TABLE:
			ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);

			if (NULL != ht)
			{
				PreventCommandDuringRecovery("REINDEX");
				ts_hypertable_permissions_check_by_id(ht->fd.id);
#if PG14_LT
				if (stmt->concurrent)
#else
				if (get_reindex_options(stmt) & REINDEXOPT_CONCURRENTLY)
#endif
					ereport(ERROR,
							(errmsg("concurrent index creation on hypertables is not supported")));

				/* Do not process remote chunks in case of distributed hypertable */
				if (hypertable_is_distributed(ht))
				{
					result = DDL_DONE;
				}
				else
				{
					if (foreach_chunk(ht, reindex_chunk, args) >= 0)
						result = DDL_DONE;
				}

				add_hypertable_to_process_args(args, ht);
			}
			break;

		case REINDEX_OBJECT_INDEX:
			ht = ts_hypertable_cache_get_entry(hcache,
											   IndexGetRelation(relid, true),
											   CACHE_FLAG_MISSING_OK);

			if (NULL != ht)
			{
				add_hypertable_to_process_args(args, ht);
				ts_hypertable_permissions_check_by_id(ht->fd.id);

				/*
				 * Recursing to chunks is currently not supported. Need to
				 * look up all chunk indexes that corresponds to the
				 * hypertable's index.
				 */
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("reindexing of a specific index on a hypertable is unsupported"),
						 errhint(
							 "As a workaround, it is possible to run REINDEX TABLE to reindex all "
							 "indexes on a hypertable, including all indexes on chunks.")));
			}
			break;
		default:
			break;
	}

	ts_cache_release(hcache);

	return result;
}

/*
 * Rename a hypertable or a chunk.
 */
static void
process_rename_table(ProcessUtilityArgs *args, Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);

	if (NULL == ht)
	{
		Chunk *chunk = ts_chunk_get_by_relid(relid, false);

		if (NULL != chunk)
			ts_chunk_set_name(chunk, stmt->newname);
	}
	else
	{
		ts_hypertable_set_name(ht, stmt->newname);

		add_hypertable_to_process_args(args, ht);
	}
}

static void
process_rename_column(ProcessUtilityArgs *args, Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);
	Dimension *dim;

	if (NULL == ht)
	{
		Chunk *chunk = ts_chunk_get_by_relid(relid, false);

		if (NULL != chunk)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot rename column \"%s\" of hypertable chunk \"%s\"",
							stmt->subname,
							get_rel_name(relid)),
					 errhint("Rename the hypertable column instead.")));

		/* This was not a hypertable and not a chunk, but it could be a
		 * continuous aggregate.
		 *
		 * If this is a continuous aggregate, the rename should be done on the
		 * materialized table. Since the partial view and the direct view are
		 * not referencing the materialized table, we need to handle it here,
		 * and in addition, the dimension table contains the column name, we
		 * need to update the name there. */
		ContinuousAgg *cagg = ts_continuous_agg_find_by_relid(relid);
		if (cagg)
		{
			RenameStmt *direct_view_stmt = castNode(RenameStmt, copyObject(stmt));
			direct_view_stmt->relation = makeRangeVar(NameStr(cagg->data.direct_view_schema),
													  NameStr(cagg->data.direct_view_name),
													  -1);
			ExecRenameStmt(direct_view_stmt);

			RenameStmt *partial_view_stmt = castNode(RenameStmt, copyObject(stmt));
			partial_view_stmt->relation = makeRangeVar(NameStr(cagg->data.partial_view_schema),
													   NameStr(cagg->data.partial_view_name),
													   -1);
			ExecRenameStmt(partial_view_stmt);

			/* Fetch the main table and it's relid and use that for the
			 * processing below. This is necessary to rebuild the view based
			 * on the table with the renamed columns. */
			ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);
			relid = ht->main_table_relid;

			RenameStmt *mat_hypertable_stmt = castNode(RenameStmt, copyObject(stmt));
			mat_hypertable_stmt->relation =
				makeRangeVar(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name), -1);
			ExecRenameStmt(mat_hypertable_stmt);
		}
	}
	else
	{
		/* Block renaming columns on the materialization table of a continuous
		 * agg, but only if this was an explicit request for rename on a
		 * materialization table. */
		if ((ts_continuous_agg_hypertable_status(ht->fd.id) & HypertableIsMaterialization) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("renaming columns on materialization tables is not supported"),
					 errdetail("Column \"%s\" in materialization table \"%s\".",
							   stmt->subname,
							   get_rel_name(relid)),
					 errhint("Rename the column on the continuous aggregate instead.")));
	}

	/* If there were a hypertable or a continuous aggregate, we need to rename
	 * the dimension that we used as well as rebuilding the view. Otherwise,
	 * we don't do anything. */
	if (ht)
	{
		add_hypertable_to_process_args(args, ht);
		dim = ts_hyperspace_get_mutable_dimension_by_name(ht->space,
														  DIMENSION_TYPE_ANY,
														  stmt->subname);

		if (dim)
			ts_dimension_set_name(dim, stmt->newname);
		if (ts_cm_functions->process_rename_cmd)
			ts_cm_functions->process_rename_cmd(relid, hcache, stmt);
	}
}

static void
process_rename_index(ProcessUtilityArgs *args, Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Oid tablerelid = IndexGetRelation(relid, true);
	Hypertable *ht;

	if (!OidIsValid(tablerelid))
		return;

	ht = ts_hypertable_cache_get_entry(hcache, tablerelid, CACHE_FLAG_MISSING_OK);

	if (NULL != ht)
	{
		ts_chunk_index_rename_parent(ht, relid, stmt->newname);

		add_hypertable_to_process_args(args, ht);
	}
	else
	{
		Chunk *chunk = ts_chunk_get_by_relid(tablerelid, false);

		if (NULL != chunk)
			ts_chunk_index_rename(chunk, relid, stmt->newname);
	}
}

static void
process_rename_view(Oid relid, RenameStmt *stmt)
{
	char *schema = get_namespace_name(get_rel_namespace(relid));
	char *name = get_rel_name(relid);
	ts_continuous_agg_rename_view(schema, name, schema, stmt->newname, &stmt->renameType);
}

/* Visit all internal catalog tables with a schema column to check for applicable rename */
static void
process_rename_schema(RenameStmt *stmt)
{
	int i = 0;

	/* Block any renames of our internal schemas */
	for (i = 0; i < NUM_TIMESCALEDB_SCHEMAS; i++)
	{
		if (strncmp(stmt->subname, ts_extension_schema_names[i], NAMEDATALEN) == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_TS_OPERATION_NOT_SUPPORTED),
					 errmsg("cannot rename schemas used by the TimescaleDB extension")));
			return;
		}
	}

	ts_chunks_rename_schema_name(stmt->subname, stmt->newname);
	ts_dimensions_rename_schema_name(stmt->subname, stmt->newname);
	ts_hypertables_rename_schema_name(stmt->subname, stmt->newname);
	ts_continuous_agg_rename_schema_name(stmt->subname, stmt->newname);
}

static void
rename_hypertable_constraint(Hypertable *ht, Oid chunk_relid, void *arg)
{
	RenameStmt *stmt = (RenameStmt *) arg;
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);

	ts_chunk_constraint_rename_hypertable_constraint(chunk->fd.id, stmt->subname, stmt->newname);
}

static void
alter_hypertable_constraint(Hypertable *ht, Oid chunk_relid, void *arg)
{
	AlterTableCmd *cmd = (AlterTableCmd *) arg;
	Constraint *cmd_constraint;
	char *hypertable_constraint_name;

	Assert(IsA(cmd->def, Constraint));
	cmd_constraint = (Constraint *) cmd->def;
	hypertable_constraint_name = cmd_constraint->conname;

	cmd_constraint->conname =
		ts_chunk_constraint_get_name_from_hypertable_constraint(chunk_relid,
																hypertable_constraint_name);

	AlterTableInternal(chunk_relid, list_make1(cmd), false);

	/* Restore for next iteration */
	cmd_constraint->conname = hypertable_constraint_name;
}

static void
validate_hypertable_constraint(Hypertable *ht, Oid chunk_relid, void *arg)
{
	AlterTableCmd *cmd = (AlterTableCmd *) arg;
	AlterTableCmd *chunk_cmd = copyObject(cmd);

	chunk_cmd->name =
		ts_chunk_constraint_get_name_from_hypertable_constraint(chunk_relid, cmd->name);

	if (chunk_cmd->name == NULL)
		return;

	/* do not pass down the VALIDATE RECURSE subtype */
	chunk_cmd->subtype = AT_ValidateConstraint;
	AlterTableInternal(chunk_relid, list_make1(chunk_cmd), false);
}

static void
rename_hypertable_trigger(Hypertable *ht, Oid chunk_relid, void *arg)
{
	RenameStmt *stmt = copyObject(castNode(RenameStmt, arg));
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);

	stmt->relation = makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), 0);
	renametrig(stmt);
}

static void
process_rename_constraint_or_trigger(ProcessUtilityArgs *args, Cache *hcache, Oid relid,
									 RenameStmt *stmt)
{
	Hypertable *ht;

	ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);

	if (NULL != ht)
	{
		relation_not_only(stmt->relation);
		add_hypertable_to_process_args(args, ht);

		if (stmt->renameType == OBJECT_TABCONSTRAINT)
			foreach_chunk(ht, rename_hypertable_constraint, stmt);
		else if (stmt->renameType == OBJECT_TRIGGER && !hypertable_is_distributed(ht))
			foreach_chunk(ht, rename_hypertable_trigger, stmt);
	}
	else if (stmt->renameType == OBJECT_TABCONSTRAINT)
	{
		Chunk *chunk = ts_chunk_get_by_relid(relid, false);

		if (NULL != chunk)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("renaming constraints on chunks is not supported")));
	}
}

static DDLResult
process_rename(ProcessUtilityArgs *args)
{
	RenameStmt *stmt = (RenameStmt *) args->parsetree;
	Oid relid = InvalidOid;
	Cache *hcache;

	/* Only get the relid if it exists for this stmt */
	if (NULL != stmt->relation)
	{
		relid = RangeVarGetRelid(stmt->relation, NoLock, true);
		if (!OidIsValid(relid))
			return DDL_CONTINUE;
	}
	else
	{
		/*
		 * stmt->relation never be NULL unless we are renaming a schema or
		 * other objects, like foreign server
		 */
		if (stmt->renameType != OBJECT_SCHEMA)
			return DDL_CONTINUE;
	}

	hcache = ts_hypertable_cache_pin();

	switch (stmt->renameType)
	{
		case OBJECT_TABLE:
			process_rename_table(args, hcache, relid, stmt);
			break;
		case OBJECT_COLUMN:
			process_rename_column(args, hcache, relid, stmt);
			break;
		case OBJECT_INDEX:
			process_rename_index(args, hcache, relid, stmt);
			break;
		case OBJECT_TABCONSTRAINT:
		case OBJECT_TRIGGER:
			process_rename_constraint_or_trigger(args, hcache, relid, stmt);
			break;
		case OBJECT_MATVIEW:
		case OBJECT_VIEW:
			process_rename_view(relid, stmt);
			break;
		case OBJECT_SCHEMA:
			process_rename_schema(stmt);
			break;
		default:
			break;
	}

	ts_cache_release(hcache);
	return DDL_CONTINUE;
}

static void
process_altertable_change_owner_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	AlterTableCmd *cmd = arg;
	Oid roleid = get_rolespec_oid(cmd->newowner, false);

	ATExecChangeOwner(chunk_relid, roleid, false, AccessExclusiveLock);
}

static void
process_altertable_change_owner(Hypertable *ht, AlterTableCmd *cmd)
{
	Assert(IsA(cmd->newowner, RoleSpec));

	foreach_chunk(ht, process_altertable_change_owner_chunk, cmd);

	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		Hypertable *compressed_hypertable =
			ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
		AlterTableInternal(compressed_hypertable->main_table_relid, list_make1(cmd), false);
		process_altertable_change_owner(compressed_hypertable, cmd);
	}
}

static void
process_add_constraint_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	Oid hypertable_constraint_oid = *((Oid *) arg);
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);

	ts_chunk_constraint_create_on_chunk(ht, chunk, hypertable_constraint_oid);
}

static void
process_altertable_add_constraint(Hypertable *ht, const char *constraint_name)
{
	Oid hypertable_constraint_oid =
		get_relation_constraint_oid(ht->main_table_relid, constraint_name, false);

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
	int i;

	for (i = 0; i < ht->space->num_dimensions; i++)
	{
		Dimension *dim = &ht->space->dimensions[i];

		if (IS_OPEN_DIMENSION(dim) &&
			strncmp(NameStr(dim->fd.column_name), cmd->name, NAMEDATALEN) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_TS_OPERATION_NOT_SUPPORTED),
					 errmsg("cannot drop not-null constraint from a time-partitioned column")));
	}
}

static void
process_altertable_drop_column(Hypertable *ht, AlterTableCmd *cmd)
{
	int i;

	for (i = 0; i < ht->space->num_dimensions; i++)
	{
		Dimension *dim = &ht->space->dimensions[i];

		if (namestrcmp(&dim->fd.column_name, cmd->name) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("cannot drop column named in partition key"),
					 errdetail("Cannot drop column that is a hypertable partitioning (space or "
							   "time) dimension.")));
	}
}

/* process all regular-table alter commands to make sure they aren't adding
 * foreign-key constraints to hypertables */
static void
verify_constraint_plaintable(RangeVar *relation, Constraint *constr)
{
	Cache *hcache;
	Hypertable *ht;

	Assert(IsA(constr, Constraint));

	hcache = ts_hypertable_cache_pin();

	switch (constr->contype)
	{
		case CONSTR_FOREIGN:
			ht = ts_hypertable_cache_get_entry_rv(hcache, constr->pktable);

			if (NULL != ht)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("foreign keys to hypertables are not supported")));
			break;
		default:
			break;
	}

	ts_cache_release(hcache);
}

/*
 * Verify that a constraint is supported on a hypertable.
 */
static void
verify_constraint_hypertable(Hypertable *ht, Node *constr_node)
{
	ConstrType contype;
	const char *indexname;
	List *keys;

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
		IndexStmt *stmt = (IndexStmt *) constr_node;

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

			ts_indexing_verify_columns(ht->space, keys);
			break;
		case CONSTR_EXCLUSION:
			ts_indexing_verify_columns(ht->space, keys);
			break;
		default:
			break;
	}
}

static void
verify_constraint(RangeVar *relation, Constraint *constr)
{
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry_rv(hcache, relation);

	if (NULL == ht)
		verify_constraint_plaintable(relation, constr);
	else
		verify_constraint_hypertable(ht, (Node *) constr);

	ts_cache_release(hcache);
}

static void
verify_constraint_list(RangeVar *relation, List *constraint_list)
{
	ListCell *lc;

	foreach (lc, constraint_list)
	{
		Constraint *constraint = lfirst(lc);

		verify_constraint(relation, constraint);
	}
}

typedef struct HypertableIndexOptions
{
	/*
	 * true if we should run one transaction per chunk, otherwise use one
	 * transaction for all the chunks
	 */
	bool multitransaction;
	int n_ht_atts;

	/* Concurrency testing options. */
#ifdef DEBUG
	/*
	 * If barrier_table is a valid Oid we try to acquire a lock on it at the
	 * start of each chunks sub-transaction.
	 */
	Oid barrier_table;

	/*
	 * if max_chunks >= 0 we'll create indices on at most max_chunks, and
	 * leave the table marked as Invalid when the command ends.
	 */
	int32 max_chunks;
#endif
} HypertableIndexOptions;

typedef struct CreateIndexInfo
{
	IndexStmt *stmt;
	ObjectAddress obj;
	Oid main_table_relid;
	HypertableIndexOptions extended_options;
	MemoryContext mctx;
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
	Relation hypertable_index_rel;
	Relation chunk_rel;
	IndexInfo *indexinfo;
	Chunk *chunk;

	Assert(!hypertable_is_distributed(ht));

	chunk = ts_chunk_get_by_relid(chunk_relid, true);
	if (IS_OSM_CHUNK(chunk)) /*cannot create index on foreign OSM chunk */
	{
		ereport(NOTICE, (errmsg("skipping index creation for tiered data")));
		return;
	}
	chunk_rel = table_open(chunk_relid, ShareLock);
	hypertable_index_rel = index_open(info->obj.objectId, AccessShareLock);
	indexinfo = BuildIndexInfo(hypertable_index_rel);

	if (chunk_index_columns_changed(info->extended_options.n_ht_atts, RelationGetDescr(chunk_rel)))
		ts_adjust_indexinfo_attnos(indexinfo, info->main_table_relid, chunk_rel);

	ts_chunk_index_create_from_adjusted_index_info(ht->fd.id,
												   hypertable_index_rel,
												   chunk->fd.id,
												   chunk_rel,
												   indexinfo);

	index_close(hypertable_index_rel, NoLock);
	table_close(chunk_rel, NoLock);
}

static void
process_index_chunk_multitransaction(int32 hypertable_id, Oid chunk_relid, void *arg)
{
	CreateIndexInfo *info = (CreateIndexInfo *) arg;
	CatalogSecurityContext sec_ctx;
	Chunk *chunk;
	Relation hypertable_index_rel;
	Relation chunk_rel;
	IndexInfo *indexinfo;

	Assert(info->extended_options.multitransaction);

	/* Start a new transaction for each relation. */
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

#ifdef DEBUG
	if (info->extended_options.max_chunks == 0)
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		return;
	}

	/*
	 * if max_chunks is < 0 then we're indexing all the chunks, if it's >= 0
	 * then we're only indexing some of the chunks, and leaving the root index
	 * marked as invalid
	 */
	if (info->extended_options.max_chunks > 0)
		info->extended_options.max_chunks -= 1;

	if (OidIsValid(info->extended_options.barrier_table))
	{
		/*
		 * For isolation tests, and debugging, it's useful to be able to
		 * pause CREATE INDEX immediately before it starts working on chunks.
		 * We acquire and immediately release a lock on a barrier table to do
		 * this.
		 */
		Relation barrier = relation_open(info->extended_options.barrier_table, AccessExclusiveLock);

		relation_close(barrier, AccessExclusiveLock);
	}
#endif

	/*
	 * Change user since chunks are typically located in an internal schema
	 * and chunk indexes require metadata changes. In the single-transaction
	 * case, we do this once for the entire table.
	 */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

	/*
	 * Hold a lock on the hypertable index, and the chunk to prevent
	 * from being altered. Since we use the same relids across transactions,
	 * there is a potential issue if the id gets reassigned between one
	 * sub-transaction and the next. CLUSTER has a similar issue.
	 *
	 * We grab a ShareLock on the chunk, because that's what CREATE INDEX
	 * does. For the hypertable's index, we are ok using the weaker
	 * AccessShareLock, since we only need to prevent the index itself from
	 * being ALTERed or DROPed during this part of index creation.
	 */
	chunk_rel = table_open(chunk_relid, ShareLock);
	chunk = ts_chunk_get_by_relid(chunk_relid, true);

	/*
	 * Validation happens when creating the hypertable's index, which goes
	 * through the usual DefineIndex mechanism.
	 */
	if (!IS_OSM_CHUNK(chunk)) /*cannot create index on foreign OSM chunk */
	{
		hypertable_index_rel = index_open(info->obj.objectId, AccessShareLock);
		indexinfo = BuildIndexInfo(hypertable_index_rel);
		if (chunk_index_columns_changed(info->extended_options.n_ht_atts,
										RelationGetDescr(chunk_rel)))
			ts_adjust_indexinfo_attnos(indexinfo, info->main_table_relid, chunk_rel);

		ts_chunk_index_create_from_adjusted_index_info(hypertable_id,
													   hypertable_index_rel,
													   chunk->fd.id,
													   chunk_rel,
													   indexinfo);

		index_close(hypertable_index_rel, NoLock);
	}
	else
	{
		ereport(NOTICE, (errmsg("skipping index creation for tiered data")));
	}
	table_close(chunk_rel, NoLock);

	ts_catalog_restore_user(&sec_ctx);

	PopActiveSnapshot();
	CommitTransactionCommand();
}

typedef enum HypertableIndexFlags
{
	HypertableIndexFlagMultiTransaction = 0,
#ifdef DEBUG
	HypertableIndexFlagBarrierTable,
	HypertableIndexFlagMaxChunks,
#endif
} HypertableIndexFlags;

static const WithClauseDefinition index_with_clauses[] = {
	[HypertableIndexFlagMultiTransaction] = {.arg_name = "transaction_per_chunk", .type_id = BOOLOID,},
#ifdef DEBUG
	[HypertableIndexFlagBarrierTable] = {.arg_name = "barrier_table", .type_id = REGCLASSOID,},
	[HypertableIndexFlagMaxChunks] = {.arg_name = "max_chunks", .type_id = INT4OID, .default_val = (Datum)-1},
#endif
};

static bool
multitransaction_create_index_mark_valid(CreateIndexInfo info)
{
#ifdef DEBUG
	return info.extended_options.max_chunks < 0;
#else
	return true;
#endif
}

/*
 * Create an index on a hypertable
 *
 * We override CREATE INDEX on hypertables in order to ensure that the index is
 * created on all of the hypertable's chunks, and to ensure that locks on all
 * of said chunks are acquired at the correct time.
 */
static DDLResult
process_index_start(ProcessUtilityArgs *args)
{
	IndexStmt *stmt = (IndexStmt *) args->parsetree;
	Cache *hcache;
	Hypertable *ht;
	List *postgres_options = NIL;
	List *hypertable_options = NIL;
	WithClauseResult *parsed_with_clauses;
	CreateIndexInfo info = {
		.stmt = stmt,
#ifdef DEBUG
		.extended_options = {0, .max_chunks = -1,},
#endif
	};
	ObjectAddress root_table_index;
	Relation main_table_relation;
	TupleDesc main_table_desc;
	Relation main_table_index_relation;
	LockRelId main_table_index_lock_relid;
	int sec_ctx;
	Oid uid = InvalidOid, saved_uid = InvalidOid;
	ContinuousAgg *cagg = NULL;

	Assert(IsA(stmt, IndexStmt));

	/*
	 * PG11 adds some cases where the relation is not there, namely on
	 * declaratively partitioned tables, with partitioned indexes:
	 * https://github.com/postgres/postgres/commit/8b08f7d4820fd7a8ef6152a9dd8c6e3cb01e5f99
	 * we don't deal with them so we will just return immediately
	 */
	if (NULL == stmt->relation)
		return DDL_CONTINUE;

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry_rv(hcache, stmt->relation);

	if (!ht)
	{
		/* Check if the relation is a Continuous Aggregate */
		cagg = ts_continuous_agg_find_by_rv(stmt->relation);

		if (cagg)
		{
			/* If the relation is a CAgg and it is finalized */
			if (ContinuousAggIsFinalized(cagg))
			{
				ht = ts_hypertable_get_by_id(cagg->data.mat_hypertable_id);
			}
			else
			{
				ts_cache_release(hcache);
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("operation not supported on continuous aggreates that are not "
								"finalized"),
						 errhint("Recreate the continuous aggregate to allow index creation.")));
			}
		}

		if (!ht)
		{
			ts_cache_release(hcache);
			return DDL_CONTINUE;
		}

		if (stmt->unique)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("continuous aggregates do not support UNIQUE indexes")));

		/* Make the RangeVar for the underlying materialization hypertable */
		stmt->relation = makeRangeVar(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name), -1);
	}
	else if (TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
	{
		/* unique indexes are not allowed on compressed hypertables*/
		if (stmt->unique || stmt->primary || stmt->isconstraint)
		{
			ts_cache_release(hcache);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("operation not supported on hypertables that have compression "
							"enabled")));
		}
	}

	ts_hypertable_permissions_check_by_id(ht->fd.id);
	add_hypertable_to_process_args(args, ht);

	ts_with_clause_filter(stmt->options, &hypertable_options, &postgres_options);

	stmt->options = postgres_options;

	parsed_with_clauses = ts_with_clauses_parse(hypertable_options,
												index_with_clauses,
												TS_ARRAY_LEN(index_with_clauses));

	info.extended_options.multitransaction =
		DatumGetBool(parsed_with_clauses[HypertableIndexFlagMultiTransaction].parsed);
#ifdef DEBUG
	info.extended_options.max_chunks =
		DatumGetInt32(parsed_with_clauses[HypertableIndexFlagMaxChunks].parsed);
	info.extended_options.barrier_table =
		DatumGetObjectId(parsed_with_clauses[HypertableIndexFlagBarrierTable].parsed);
#endif

	/* Make sure this index is allowed */
	if (stmt->concurrent)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support concurrent "
						"index creation")));

	if (info.extended_options.multitransaction &&
		(stmt->unique || stmt->primary || stmt->isconstraint))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "cannot use timescaledb.transaction_per_chunk with UNIQUE or PRIMARY KEY")));

	if (info.extended_options.multitransaction && hypertable_is_distributed(ht))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg(
					 "cannot use timescaledb.transaction_per_chunk with distributed hypertable")));

	ts_indexing_verify_index(ht->space, stmt);

	if (info.extended_options.multitransaction)
		PreventInTransactionBlock(true,
								  "CREATE INDEX ... WITH (timescaledb.transaction_per_chunk)");

	if (cagg)
	{
		/*
		 * If this is an index creation for cagg, then we need to switch user as the current
		 * user might not have permissions on the internal schema where cagg index will be
		 * created.
		 * Need to restore user soon after this step.
		 */
		ts_cagg_permissions_check(ht->main_table_relid, GetUserId());
		SWITCH_TO_TS_USER(NameStr(cagg->data.direct_view_schema), uid, saved_uid, sec_ctx);
	}

	/* CREATE INDEX on the root table of the hypertable */
	root_table_index = ts_indexing_root_table_create_index(stmt,
														   args->query_string,
														   info.extended_options.multitransaction,
														   hypertable_is_distributed(ht));

	if (cagg)
		RESTORE_USER(uid, saved_uid, sec_ctx);

	/* root_table_index will have 0 objectId if the index already exists
	 * and if_not_exists is true. In that case there is nothing else
	 * to do here. */
	if (!OidIsValid(root_table_index.objectId) && stmt->if_not_exists)
	{
		ts_cache_release(hcache);
		return DDL_DONE;
	}
	Assert(OidIsValid(root_table_index.objectId));

	/* support ONLY ON clause, index on root table already created */
	if (!stmt->relation->inh)
	{
		ts_cache_release(hcache);
		return DDL_DONE;
	}

	/* CREATE INDEX on the chunks, unless this is a distributed hypertable */
	if (hypertable_is_distributed(ht))
	{
		ts_cache_release(hcache);
		return DDL_DONE;
	}

	info.obj.objectId = root_table_index.objectId;
	/* collect information required for per chunk index creation */
	main_table_relation = table_open(ht->main_table_relid, AccessShareLock);
	main_table_desc = RelationGetDescr(main_table_relation);

	main_table_index_relation = index_open(info.obj.objectId, AccessShareLock);
	main_table_index_lock_relid = main_table_index_relation->rd_lockInfo.lockRelId;

	info.extended_options.n_ht_atts = main_table_desc->natts;
	info.main_table_relid = ht->main_table_relid;

	index_close(main_table_index_relation, NoLock);
	table_close(main_table_relation, NoLock);

	/* create chunk indexes using the same transaction for all the chunks */
	if (!info.extended_options.multitransaction)
	{
		CatalogSecurityContext sec_ctx;
		/*
		 * Change user since chunk's are typically located in an internal
		 * schema and chunk indexes require metadata changes. In the
		 * multi-transaction case, we do this once per chunk.
		 */
		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		/* Recurse to each chunk and create a corresponding index. */
		foreach_chunk(ht, process_index_chunk, &info);

		ts_catalog_restore_user(&sec_ctx);
		ts_cache_release(hcache);

		return DDL_DONE;
	}

	/* create chunk indexes using a separate transaction for each chunk */

	/*
	 * Lock the index for the remainder of the command. Since we're using
	 * multiple transactions for index creation, a regular
	 * transaction-level lock won't prevent the index from being
	 * concurrently ALTERed or DELETEed. Instead, we grab a session level
	 * lock on the index, which we'll release when the command is
	 * finished. (This is the same strategy postgres uses in CREATE INDEX
	 * CONCURRENTLY)
	 */
	LockRelationIdForSession(&main_table_index_lock_relid, AccessShareLock);

	/*
	 * mark the hypertable's index as invalid until all the chunk indexes
	 * are created. This allows us to determine if the CREATE INDEX
	 * completed successfully or  not
	 */
	ts_indexing_mark_as_invalid(info.obj.objectId);
	CacheInvalidateRelcacheByRelid(info.main_table_relid);
	CacheInvalidateRelcacheByRelid(info.obj.objectId);

	ts_cache_release(hcache);

	/* we need a long-lived context in which to store the list of chunks since the per-transaction
	 * context will get freed at the end of each transaction. Fortunately we're within just such a
	 * context now; the PortalContext. */
	info.mctx = CurrentMemoryContext;
	PopActiveSnapshot();
	CommitTransactionCommand();

	foreach_chunk_multitransaction(info.main_table_relid,
								   info.mctx,
								   process_index_chunk_multitransaction,
								   &info);

	StartTransactionCommand();
	MemoryContextSwitchTo(info.mctx);

	if (multitransaction_create_index_mark_valid(info))
	{
		/* we're done, the index is now valid */
		ts_indexing_mark_as_valid(info.obj.objectId);

		CacheInvalidateRelcacheByRelid(info.main_table_relid);
		CacheInvalidateRelcacheByRelid(info.obj.objectId);
	}

	CommitTransactionCommand();
	StartTransactionCommand();

	UnlockRelationIdForSession(&main_table_index_lock_relid, AccessShareLock);

	DEBUG_WAITPOINT("process_index_start_indexing_done");

	return DDL_DONE;
}

static int
chunk_index_mappings_cmp(const void *p1, const void *p2)
{
	const ChunkIndexMapping *mapping[] = { *((ChunkIndexMapping *const *) p1),
										   *((ChunkIndexMapping *const *) p2) };

	return mapping[0]->chunkoid - mapping[1]->chunkoid;
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
static DDLResult
process_cluster_start(ProcessUtilityArgs *args)
{
	ClusterStmt *stmt = (ClusterStmt *) args->parsetree;
	Cache *hcache;
	Hypertable *ht;
	DDLResult result = DDL_CONTINUE;

	Assert(IsA(stmt, ClusterStmt));

	/* If this is a re-cluster on all tables, there is nothing we need to do */
	if (NULL == stmt->relation)
		return DDL_CONTINUE;

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry_rv(hcache, stmt->relation);

	if (NULL != ht)
	{
		bool is_top_level = (args->context == PROCESS_UTILITY_TOPLEVEL);
		Oid index_relid;
		Relation index_rel;
		List *chunk_indexes;
		ListCell *lc;
		MemoryContext old, mcxt;
		LockRelId cluster_index_lockid;
		ChunkIndexMapping **mappings = NULL;
		int i;

		ts_hypertable_permissions_check_by_id(ht->fd.id);

		/*
		 * If CLUSTER is run inside a user transaction block; we bail out or
		 * otherwise we'd be holding locks way too long.
		 */
		PreventInTransactionBlock(is_top_level, "CLUSTER");

		add_hypertable_to_process_args(args, ht);

		if (NULL == stmt->indexname)
		{
			index_relid = ts_indexing_find_clustered_index(ht->main_table_relid);
			if (!OidIsValid(index_relid))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_OBJECT),
						 errmsg("there is no previously clustered index for table \"%s\"",
								get_rel_name(ht->main_table_relid))));
		}
		else
			index_relid =
				get_relname_relid(stmt->indexname, get_rel_namespace(ht->main_table_relid));

		if (!OidIsValid(index_relid))
		{
			/* Let regular process utility handle */
			ts_cache_release(hcache);
			return DDL_CONTINUE;
		}

		/*
		 * DROP INDEX locks the table then the index, to prevent deadlocks we
		 * lock them in the same order. The main table lock will be released
		 * when the current transaction commits, and never taken again. We
		 * will use the index relation to grab a session lock on the index,
		 * which we will hold throughout CLUSTER
		 */
		LockRelationOid(ht->main_table_relid, AccessShareLock);
		index_rel = index_open(index_relid, AccessShareLock);
		cluster_index_lockid = index_rel->rd_lockInfo.lockRelId;

		index_close(index_rel, NoLock);

		/*
		 * mark the main table as clustered, even though it has no data, so
		 * future calls to CLUSTER don't need to pass in the index
		 */
		ts_chunk_index_mark_clustered(ht->main_table_relid, index_relid);

		/* we will keep holding this lock throughout CLUSTER */
		LockRelationIdForSession(&cluster_index_lockid, AccessShareLock);

		/*
		 * The list of chunks and their indexes need to be on a memory context
		 * that will survive moving to a new transaction for each chunk
		 */
		mcxt = AllocSetContextCreate(PortalContext, "Hypertable cluster", ALLOCSET_DEFAULT_SIZES);

		/*
		 * Get a list of chunks and indexes that correspond to the
		 * hypertable's index
		 */
		old = MemoryContextSwitchTo(mcxt);
		chunk_indexes = ts_chunk_index_get_mappings(ht, index_relid);

		if (list_length(chunk_indexes) > 0)
		{
			/* Sort the mappings on chunk OID. This makes the verbose output more
			 * predictable in tests, but isn't strictly necessary. We could also do
			 * it only for "verbose" output, but this doesn't seem worth it as the
			 * cost of sorting is quickly amortized over the actual work to cluster
			 * the chunks. */
			mappings = palloc(sizeof(ChunkIndexMapping *) * list_length(chunk_indexes));

			i = 0;
			foreach (lc, chunk_indexes)
				mappings[i++] = lfirst(lc);

			qsort(mappings,
				  list_length(chunk_indexes),
				  sizeof(ChunkIndexMapping *),
				  chunk_index_mappings_cmp);
		}

		MemoryContextSwitchTo(old);

		hcache->release_on_commit = false;

		/* Commit to get out of starting transaction */
		PopActiveSnapshot();
		CommitTransactionCommand();

		for (i = 0; i < list_length(chunk_indexes); i++)
		{
			ChunkIndexMapping *cim = mappings[i];

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
			ts_chunk_index_mark_clustered(cim->chunkoid, cim->indexoid);

			/* Do the job. */

			/*
			 * Since we keep OIDs between transactions, there is a potential
			 * issue if an OID gets reassigned between two subtransactions
			 */
			cluster_rel(cim->chunkoid, cim->indexoid, get_cluster_options(stmt));
			PopActiveSnapshot();
			CommitTransactionCommand();
		}

		hcache->release_on_commit = true;
		/* Start a new transaction for the cleanup work. */
		StartTransactionCommand();

		/* Clean up working storage */
		MemoryContextDelete(mcxt);

		UnlockRelationIdForSession(&cluster_index_lockid, AccessShareLock);
		result = DDL_DONE;
	}

	ts_cache_release(hcache);
	return result;
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
	ListCell *lc;

	verify_constraint_list(stmt->relation, stmt->constraints);

	/*
	 * Only after parse analysis does tableElts contain only ColumnDefs. So,
	 * if we capture this in processUtility, we should be prepared to have
	 * constraint nodes and TableLikeClauses intermixed
	 */
	foreach (lc, stmt->tableElts)
	{
		ColumnDef *coldef;

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
	return strVal(llast(tn->names));
}

static void
process_alter_column_type_start(Hypertable *ht, AlterTableCmd *cmd)
{
	int i;

	for (i = 0; i < ht->space->num_dimensions; i++)
	{
		Dimension *dim = &ht->space->dimensions[i];

		if (IS_CLOSED_DIMENSION(dim) &&
			strncmp(NameStr(dim->fd.column_name), cmd->name, NAMEDATALEN) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_TS_OPERATION_NOT_SUPPORTED),
					 errmsg("cannot change the type of a hash-partitioned column")));

		if (dim->partitioning != NULL &&
			strncmp(NameStr(dim->fd.column_name), cmd->name, NAMEDATALEN) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_TS_OPERATION_NOT_SUPPORTED),
					 errmsg("cannot change the type of a column with a custom partitioning "
							"function")));
	}
}

static void
process_alter_column_type_end(Hypertable *ht, AlterTableCmd *cmd)
{
	ColumnDef *coldef = (ColumnDef *) cmd->def;
	Oid new_type = TypenameGetTypid(typename_get_unqual_name(coldef->typeName));
	Dimension *dim =
		ts_hyperspace_get_mutable_dimension_by_name(ht->space, DIMENSION_TYPE_ANY, cmd->name);

	if (NULL == dim)
		return;

	ts_dimension_set_type(dim, new_type);
	ts_process_utility_set_expect_chunk_modification(true);
	ts_chunk_recreate_all_constraints_for_dimension(ht, dim->fd.id);
	ts_process_utility_set_expect_chunk_modification(false);
}

static void
process_altertable_clusteron_end(Hypertable *ht, AlterTableCmd *cmd)
{
	Oid index_relid = ts_get_relation_relid(NameStr(ht->fd.schema_name), cmd->name, true);

	/* If this is part of changing the type of a column that is used in a clustered index
	 * the above lookup might fail. But in this case we don't need to mark the index clustered
	 * as postgres takes care of that already */
	if (!OidIsValid(index_relid))
		return;

	List *chunk_indexes = ts_chunk_index_get_mappings(ht, index_relid);
	ListCell *lc;

	foreach (lc, chunk_indexes)
	{
		ChunkIndexMapping *cim = lfirst(lc);

		ts_chunk_index_mark_clustered(cim->chunkoid, cim->indexoid);
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
process_altertable_chunk_replica_identity(Hypertable *ht, Oid chunk_relid, void *arg)
{
	AlterTableCmd *cmd = castNode(AlterTableCmd, copyObject(arg));
	ReplicaIdentityStmt *stmt = castNode(ReplicaIdentityStmt, cmd->def);
	char relkind = get_rel_relkind(chunk_relid);

	/* If this is not a local chunk (e.g., it is foreign table representing a
	 * data node or OSM chunk), then we don't set replica identity locally */
	if (relkind != RELKIND_RELATION)
		return;

	if (stmt->identity_type == REPLICA_IDENTITY_INDEX)
	{
		Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
		Oid hyper_schema_oid = get_rel_namespace(ht->main_table_relid);
		Oid hyper_index_oid = get_relname_relid(stmt->name, hyper_schema_oid);
		ChunkIndexMapping cim;

		Assert(OidIsValid(hyper_index_oid));

		if (!ts_chunk_index_get_by_hypertable_indexrelid(chunk, hyper_index_oid, &cim))
			elog(ERROR,
				 "chunk \"%s.%s\" has no index corresponding to hypertable index \"%s\"",
				 NameStr(chunk->fd.schema_name),
				 NameStr(chunk->fd.table_name),
				 stmt->name);

		stmt->name = get_rel_name(cim.indexoid);
	}

	AlterTableInternal(chunk_relid, list_make1(cmd), false);
}

static void
process_altertable_replica_identity(Hypertable *ht, AlterTableCmd *cmd)
{
	ReplicaIdentityStmt *stmt = castNode(ReplicaIdentityStmt, cmd->def);

	if (stmt->identity_type == REPLICA_IDENTITY_INDEX)
	{
		Oid hyper_schema_oid = get_rel_namespace(ht->main_table_relid);
		Oid hyper_index_oid;

		hyper_index_oid = get_relname_relid(stmt->name, hyper_schema_oid);

		if (!OidIsValid(hyper_index_oid))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					 errmsg("index \"%s\" for table \"%s.%s\" does not exist",
							stmt->name,
							NameStr(ht->fd.schema_name),
							NameStr(ht->fd.table_name))));
	}

	foreach_chunk(ht, process_altertable_chunk_replica_identity, cmd);
}

static void
process_altertable_set_tablespace_end(Hypertable *ht, AlterTableCmd *cmd)
{
	NameData tspc_name;
	Tablespaces *tspcs;

	namestrcpy(&tspc_name, cmd->name);

	tspcs = ts_tablespace_scan(ht->fd.id);

	if (tspcs->num_tablespaces > 1)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot set new tablespace when multiple tablespaces are attached to "
						"hypertable \"%s\"",
						get_rel_name(ht->main_table_relid)),
				 errhint("Detach tablespaces before altering the hypertable.")));

	if (tspcs->num_tablespaces == 1)
	{
		Assert(ts_hypertable_has_tablespace(ht, tspcs->tablespaces[0].tablespace_oid));
		ts_tablespace_delete(ht->fd.id,
							 NameStr(tspcs->tablespaces[0].fd.tablespace_name),
							 tspcs->tablespaces[0].tablespace_oid);
	}

	ts_tablespace_attach_internal(&tspc_name, ht->main_table_relid, true);
	foreach_chunk(ht, process_altertable_chunk, cmd);
	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		Hypertable *compressed_hypertable =
			ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
		AlterTableInternal(compressed_hypertable->main_table_relid, list_make1(cmd), false);
		process_altertable_set_tablespace_end(compressed_hypertable, cmd);
	}
}

static void
process_altertable_end_index(Node *parsetree, CollectedCommand *cmd)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	Oid indexrelid = AlterTableLookupRelation(stmt, NoLock);
	Oid tablerelid = IndexGetRelation(indexrelid, false);
	Cache *hcache;
	Hypertable *ht;

	if (!OidIsValid(tablerelid))
		return;

	ht = ts_hypertable_cache_get_cache_and_entry(tablerelid, CACHE_FLAG_MISSING_OK, &hcache);

	if (NULL != ht)
	{
		ListCell *lc;

		foreach (lc, stmt->cmds)
		{
			AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

			switch (cmd->subtype)
			{
				case AT_SetTableSpace:
					ts_chunk_index_set_tablespace(ht, indexrelid, cmd->name);
					break;
				default:
					break;
			}
		}
	}

	ts_cache_release(hcache);
}

static inline void
process_altertable_chunk_set_tablespace(AlterTableCmd *cmd, Oid relid)
{
	Chunk *chunk = ts_chunk_get_by_relid(relid, false);

	if (chunk == NULL)
		return;

	if (ts_chunk_contains_compressed_data(chunk))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("changing tablespace of compressed chunk is not supported"),
				 errhint("Please use the corresponding chunk on the uncompressed hypertable "
						 "instead.")));

	/* set tablespace for compressed chunk */
	if (chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
	{
		Chunk *compressed_chunk = ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, true);

		AlterTableInternal(compressed_chunk->table_id, list_make1(cmd), false);
	}
}

static DDLResult
process_altertable_start_table(ProcessUtilityArgs *args)
{
	AlterTableStmt *stmt = (AlterTableStmt *) args->parsetree;
	Oid relid = AlterTableLookupRelation(stmt, NoLock);
	Cache *hcache;
	Hypertable *ht;
	ListCell *lc;
	DDLResult result = DDL_CONTINUE;
	int num_cmds;

	if (!OidIsValid(relid))
		return DDL_CONTINUE;

	check_chunk_alter_table_operation_allowed(relid, stmt);

	ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_MISSING_OK, &hcache);
	if (ht != NULL)
	{
		ts_hypertable_permissions_check_by_id(ht->fd.id);
		check_continuous_agg_alter_table_allowed(ht, stmt);
		check_alter_table_allowed_on_ht_with_compression(ht, stmt);
		relation_not_only(stmt->relation);
		add_hypertable_to_process_args(args, ht);
	}
	num_cmds = list_length(stmt->cmds);
	foreach (lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_AddIndex:
			{
				IndexStmt *istmt = (IndexStmt *) cmd->def;

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
#if PG16_LT
			case AT_AddColumnRecurse:
#endif
			{
				ColumnDef *col;
				ListCell *constraint_lc;

				Assert(IsA(cmd->def, ColumnDef));
				col = (ColumnDef *) cmd->def;
				if (ht && TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
					check_altertable_add_column_for_compressed(ht, col);
				if (NULL == ht)
					foreach (constraint_lc, col->constraints)
						verify_constraint_plaintable(stmt->relation, lfirst(constraint_lc));
				else
					foreach (constraint_lc, col->constraints)
						verify_constraint_hypertable(ht, lfirst(constraint_lc));
				break;
			}
			case AT_DropColumn:
#if PG16_LT
			case AT_DropColumnRecurse:
#endif
				if (NULL != ht)
					process_altertable_drop_column(ht, cmd);
				break;
			case AT_AddConstraint:
#if PG16_LT
			case AT_AddConstraintRecurse:
#endif
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
			case AT_AttachPartition:
			{
				RangeVar *relation;
				PartitionCmd *partstmt;

				partstmt = (PartitionCmd *) cmd->def;
				relation = partstmt->name;
				Assert(NULL != relation);

				if (OidIsValid(ts_hypertable_relid(relation)))
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("hypertables do not support native "
									"postgres partitioning")));
				}
				break;
			}
			case AT_SetRelOptions:
			{
				if (ht != NULL)
				{
					if (num_cmds != 1)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("ALTER TABLE <hypertable> SET does not support multiple "
										"clauses")));
					}
					EventTriggerAlterTableStart(args->parsetree);
					result = process_altertable_set_options(cmd, ht);
				}
				break;
			}
			case AT_ResetRelOptions:
			case AT_ReplaceRelOptions:
				process_altertable_reset_options(cmd, ht);
				break;
			case AT_SetTableSpace:
				if (NULL == ht)
					process_altertable_chunk_set_tablespace(cmd, relid);
				break;
			default:
				break;
		}
	}

	ts_cache_release(hcache);
	return result;
}

static void
continuous_agg_with_clause_perm_check(ContinuousAgg *cagg, Oid view_relid)
{
	Oid ownerid = ts_rel_get_owner(view_relid);

	if (!has_privs_of_role(GetUserId(), ownerid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be owner of continuous aggregate \"%s\"", get_rel_name(view_relid))));
}

static void
process_altercontinuousagg_set_with(ContinuousAgg *cagg, Oid view_relid, const List *defelems)
{
	WithClauseResult *parse_results;
	List *pg_options = NIL, *cagg_options = NIL;

	continuous_agg_with_clause_perm_check(cagg, view_relid);

	ts_with_clause_filter(defelems, &cagg_options, &pg_options);
	if (list_length(pg_options) > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only timescaledb parameters allowed in WITH clause for continuous "
						"aggregate")));

	if (list_length(cagg_options) > 0)
	{
		parse_results = ts_continuous_agg_with_clause_parse(cagg_options);
		ts_cm_functions->continuous_agg_update_options(cagg, parse_results);
	}
}

/* Run an alter table command on a relation */
static void
alter_table_by_relation(RangeVar *relation, AlterTableCmd *cmd)
{
	const Oid relid = RangeVarGetRelid(relation, NoLock, true);
	AlterTableInternal(relid, list_make1(cmd), false);
}

/* Run an alter table command on a relation given by name */
static void
alter_table_by_name(Name schema_name, Name table_name, AlterTableCmd *cmd)
{
	alter_table_by_relation(makeRangeVar(NameStr(*schema_name), NameStr(*table_name), -1), cmd);
}

/* Alter a hypertable and do some extra processing */
static void
alter_hypertable_by_id(int32 hypertable_id, AlterTableStmt *stmt, AlterTableCmd *cmd,
					   void (*extra)(Hypertable *, AlterTableCmd *))
{
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry_by_id(hcache, hypertable_id);
	Assert(ht); /* Broken continuous aggregate */
	ts_hypertable_permissions_check_by_id(ht->fd.id);
	check_alter_table_allowed_on_ht_with_compression(ht, stmt);
	relation_not_only(stmt->relation);
	AlterTableInternal(ht->main_table_relid, list_make1(cmd), false);
	(*extra)(ht, cmd);
	ts_cache_release(hcache);
}

static DDLResult
process_altertable_start_matview(ProcessUtilityArgs *args)
{
	AlterTableStmt *stmt = (AlterTableStmt *) args->parsetree;
	const Oid view_relid = RangeVarGetRelid(stmt->relation, NoLock, true);
	ContinuousAgg *cagg;
	ListCell *lc;

	if (!OidIsValid(view_relid))
		return DDL_CONTINUE;

	cagg = ts_continuous_agg_find_by_relid(view_relid);

	if (cagg == NULL)
		return DDL_CONTINUE;

	continuous_agg_with_clause_perm_check(cagg, view_relid);

	foreach (lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_SetRelOptions:
				if (!IsA(cmd->def, List))
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("expected set options to contain a list")));
				process_altercontinuousagg_set_with(cagg, view_relid, (List *) cmd->def);
				break;

			case AT_ChangeOwner:
				alter_table_by_relation(stmt->relation, cmd);
				alter_table_by_name(&cagg->data.partial_view_schema,
									&cagg->data.partial_view_name,
									cmd);
				alter_table_by_name(&cagg->data.direct_view_schema,
									&cagg->data.direct_view_name,
									cmd);
				alter_hypertable_by_id(cagg->data.mat_hypertable_id,
									   stmt,
									   cmd,
									   process_altertable_change_owner);
				break;

			case AT_SetTableSpace:
				alter_hypertable_by_id(cagg->data.mat_hypertable_id,
									   stmt,
									   cmd,
									   process_altertable_set_tablespace_end);
				break;

			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter only SET options of a continuous "
								"aggregate")));
		}
	}
	/* All commands processed by us, nothing for postgres to do.*/
	return DDL_DONE;
}

static DDLResult
process_altertable_start_view(ProcessUtilityArgs *args)
{
	AlterTableStmt *stmt = (AlterTableStmt *) args->parsetree;
	Oid relid = AlterTableLookupRelation(stmt, NoLock);
	ContinuousAgg *cagg;
	ContinuousAggViewType vtyp;
	const char *view_name;
	const char *view_schema;

	/* Check if this is a materialized view and give error if it is. */
	cagg = ts_continuous_agg_find_by_relid(relid);

	if (cagg)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter continuous aggregate using ALTER VIEW"),
				 errhint("Use ALTER MATERIALIZED VIEW to alter a continuous aggregate.")));

	/* Check if this is an internal view of a continuous aggregate and give
	 * error if attempts are made to alter them. */
	view_name = get_rel_name(relid);
	view_schema = get_namespace_name(get_rel_namespace(relid));
	cagg = ts_continuous_agg_find_by_view_name(view_schema, view_name, ContinuousAggAnyView);

	if (cagg == NULL)
		return DDL_CONTINUE;

	vtyp = ts_continuous_agg_view_type(&cagg->data, view_schema, view_name);

	if (vtyp == ContinuousAggPartialView || vtyp == ContinuousAggDirectView)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter the internal view of a continuous aggregate")));

	return DDL_DONE;
}

static DDLResult
process_altertable_start(ProcessUtilityArgs *args)
{
	AlterTableStmt *stmt = (AlterTableStmt *) args->parsetree;
	switch (get_altertable_objecttype(stmt))
	{
		case OBJECT_TABLE:
			return process_altertable_start_table(args);
		case OBJECT_MATVIEW:
			return process_altertable_start_matview(args);
		case OBJECT_VIEW:
			return process_altertable_start_view(args);
		default:
			return DDL_CONTINUE;
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
			IndexStmt *stmt = (IndexStmt *) cmd->def;
			const char *idxname = stmt->idxname;

			Assert(IsA(cmd->def, IndexStmt));

			Assert(stmt->isconstraint);

			if (idxname == NULL)
				idxname = get_rel_name(obj->objectId);

			process_altertable_add_constraint(ht, idxname);
		}
		break;
		case AT_AddConstraint:
#if PG16_LT
		case AT_AddConstraintRecurse:
#endif
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
			/* Break here to silence compiler */
			break;
		case AT_ClusterOn:
			process_altertable_clusteron_end(ht, cmd);
			break;
		case AT_SetUnLogged:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("logging cannot be turned off for hypertables")));
			/* Break here to silence compiler */
			break;
		case AT_ReplicaIdentity:
			process_altertable_replica_identity(ht, cmd);
			break;
		case AT_EnableRule:
		case AT_EnableAlwaysRule:
		case AT_EnableReplicaRule:
		case AT_DisableRule:
			/* should never actually get here but just in case */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertables do not support rules")));
			/* Break here to silence compiler */
			break;
		case AT_AlterConstraint:
			process_altertable_alter_constraint_end(ht, cmd);
			break;
		case AT_ValidateConstraint:
#if PG16_LT
		case AT_ValidateConstraintRecurse:
#endif
			process_altertable_validate_constraint_end(ht, cmd);
			break;
		case AT_DropCluster:
			foreach_chunk(ht, process_altertable_chunk, cmd);
			break;
		case AT_SetRelOptions:
		case AT_ResetRelOptions:
		case AT_ReplaceRelOptions:
		case AT_DropOids:
		case AT_SetOptions:
		case AT_ResetOptions:
#if PG14_GE
		case AT_ReAddStatistics:
		case AT_SetCompression:
#endif
			/* Avoid running this command for distributed hypertable chunks
			 * since PostgreSQL currently does not allow to alter
			 * storage options for a foreign table. */
			if (!hypertable_is_distributed(ht))
				foreach_chunk(ht, process_altertable_chunk, cmd);
			break;
		case AT_SetTableSpace:
			process_altertable_set_tablespace_end(ht, cmd);
			break;
		case AT_AddInherit:
		case AT_DropInherit:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("hypertables do not support inheritance")));
		case AT_SetStatistics:
		case AT_SetLogged:
		case AT_SetStorage:
		case AT_ColumnDefault:
#if PG_VERSION_NUM >= 120005
		case AT_CookedColumnDefault:
#endif
		case AT_SetNotNull:
		case AT_CheckNotNull:
		case AT_DropNotNull:
		case AT_AddOf:
		case AT_DropOf:
		case AT_AddIdentity:
		case AT_SetIdentity:
		case AT_DropIdentity:
			/* all of the above are handled by default recursion */
			break;
		case AT_EnableRowSecurity:
		case AT_DisableRowSecurity:
		case AT_ForceRowSecurity:
		case AT_NoForceRowSecurity:
			/* RLS commands should not recurse to chunks */
			break;
		case AT_ReAddConstraint:
		case AT_ReAddIndex:

			/*
			 * all of the above are internal commands that are hit in tests
			 * and correctly handled
			 */
			break;
		case AT_AddColumn:
#if PG16_LT
		case AT_AddColumnRecurse:
#endif
			/* this is handled for compressed hypertables by tsl code */
			break;
		case AT_DropColumn:
#if PG16_LT
		case AT_DropColumnRecurse:
#endif
		case AT_DropExpression:

			/*
			 * adding and dropping columns handled in
			 * process_altertable_start_table
			 */
			break;
		case AT_DropConstraint:
#if PG16_LT
		case AT_DropConstraintRecurse:
#endif
			/* drop constraints handled by process_ddl_sql_drop */
			break;
		case AT_ReAddComment:			   /* internal command never hit in our test
											* code, so don't know how to handle */
		case AT_AddColumnToView:		   /* only used with views */
		case AT_AlterColumnGenericOptions: /* only used with foreign tables */
		case AT_GenericOptions:			   /* only used with foreign tables */
		case AT_ReAddDomainConstraint:	   /* We should handle this in future,
											* new subset of constraints in PG11
											* currently not hit in test code */
		case AT_AttachPartition:		   /* handled in
											* process_altertable_start_table but also
											* here as failsafe */
		case AT_DetachPartition:
#if PG14_GE
		case AT_DetachPartitionFinalize:
#endif
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("operation not supported on hypertables %d", cmd->subtype)));
			break;
		default:
			break;
	}
	if (ts_cm_functions->process_altertable_cmd)
		ts_cm_functions->process_altertable_cmd(ht, cmd);
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
	ListCell *lc;

	foreach (lc, cmds)
	{
		CollectedATSubcmd *cmd = lfirst(lc);

		process_altertable_end_subcmd(ht, cmd->parsetree, &cmd->address);
	}
}

static void
process_altertable_end_table(Node *parsetree, CollectedCommand *cmd)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;
	Oid relid;
	Cache *hcache;
	Hypertable *ht;

	Assert(IsA(stmt, AlterTableStmt));

	relid = AlterTableLookupRelation(stmt, NoLock);

	if (!OidIsValid(relid))
		return;

	ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_MISSING_OK, &hcache);

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
	ts_cache_release(hcache);
}

static void
process_altertable_end(Node *parsetree, CollectedCommand *cmd)
{
	AlterTableStmt *stmt = (AlterTableStmt *) parsetree;

	switch (get_altertable_objecttype(stmt))
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

static DDLResult
process_create_trigger_start(ProcessUtilityArgs *args)
{
	CreateTrigStmt *stmt = (CreateTrigStmt *) args->parsetree;
	Cache *hcache;
	Hypertable *ht;
	ObjectAddress PG_USED_FOR_ASSERTS_ONLY address;

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry_rv(hcache, stmt->relation);
	if (ht == NULL)
	{
		ts_cache_release(hcache);
		return DDL_CONTINUE;
	}

	if (stmt->transitionRels)
	{
		ts_cache_release(hcache);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("trigger with transition tables not supported on hypertables")));
	}

	add_hypertable_to_process_args(args, ht);

	if (!stmt->row)
	{
		ts_cache_release(hcache);
		return DDL_CONTINUE;
	}

	address = ts_hypertable_create_trigger(ht, stmt, args->query_string);
	Assert(OidIsValid(address.objectId));

	ts_cache_release(hcache);
	return DDL_DONE;
}

static DDLResult
process_create_rule_start(ProcessUtilityArgs *args)
{
	RuleStmt *stmt = (RuleStmt *) args->parsetree;

	if (!OidIsValid(ts_hypertable_relid(stmt->relation)))
		return DDL_CONTINUE;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("hypertables do not support rules")));

	return DDL_CONTINUE;
}

/* ALTER TABLE <name> SET ( timescaledb.compress, ...) */
static DDLResult
process_altertable_set_options(AlterTableCmd *cmd, Hypertable *ht)
{
	List *pg_options = NIL, *compress_options = NIL;
	WithClauseResult *parse_results = NULL;
	List *inpdef = NIL;
	/* is this a compress table stmt */
	Assert(IsA(cmd->def, List));
	inpdef = (List *) cmd->def;
	ts_with_clause_filter(inpdef, &compress_options, &pg_options);
	if (compress_options)
	{
		parse_results = ts_compress_hypertable_set_clause_parse(compress_options);
		/* We allow updating compress chunk time interval independently of other compression
		 * options. */
		if (parse_results[CompressEnabled].is_default &&
			parse_results[CompressChunkTimeInterval].is_default)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("the option timescaledb.compress must be set to true to enable "
							"compression")));
	}
	else
		return DDL_CONTINUE;

	if (pg_options != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only timescaledb.compress parameters allowed when specifying compression "
						"parameters for hypertable")));
	ts_cm_functions->process_compress_table(cmd, ht, parse_results);
	return DDL_DONE;
}

static DDLResult
process_altertable_reset_options(AlterTableCmd *cmd, Hypertable *ht)
{
	List *pg_options = NIL, *compress_options = NIL;
	List *inpdef = NIL;
	/* is this a compress table stmt */
	Assert(IsA(cmd->def, List));
	inpdef = (List *) cmd->def;
	ts_with_clause_filter(inpdef, &compress_options, &pg_options);

	if (compress_options)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("compression options cannot be reset")));

	return DDL_CONTINUE;
}

static DDLResult
process_viewstmt(ProcessUtilityArgs *args)
{
	ViewStmt *stmt = castNode(ViewStmt, args->parsetree);
	List *pg_options = NIL;
	List *cagg_options = NIL;

	/* Check if user is passing continuous aggregate parameters and print a
	 * useful error message if that is the case. */
	ts_with_clause_filter(stmt->options, &cagg_options, &pg_options);
	if (cagg_options)
		ereport(ERROR,
				(errmsg("cannot create continuous aggregate with CREATE VIEW"),
				 errhint("Use CREATE MATERIALIZED VIEW to create a continuous aggregate.")));
	return DDL_CONTINUE;
}

static DDLResult
process_create_table_as(ProcessUtilityArgs *args)
{
	CreateTableAsStmt *stmt = castNode(CreateTableAsStmt, args->parsetree);
	WithClauseResult *parse_results = NULL;
	bool is_cagg = false;
	List *pg_options = NIL, *cagg_options = NIL;

	if (get_createtableas_objecttype(stmt) == OBJECT_MATVIEW)
	{
		/* Check for creation of continuous aggregate */
		ts_with_clause_filter(stmt->into->options, &cagg_options, &pg_options);

		if (cagg_options)
		{
			parse_results = ts_continuous_agg_with_clause_parse(cagg_options);
			is_cagg = DatumGetBool(parse_results[ContinuousEnabled].parsed);
		}

		if (!is_cagg)
			return DDL_CONTINUE;

		if (pg_options != NIL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("unsupported combination of storage parameters"),
					 errdetail("A continuous aggregate does not support standard storage "
							   "parameters."),
					 errhint("Use only parameters with the \"timescaledb.\" prefix when "
							 "creating a continuous aggregate.")));

		if (!stmt->into->skipData)
			PreventInTransactionBlock(args->context == PROCESS_UTILITY_TOPLEVEL,
									  "CREATE MATERIALIZED VIEW ... WITH DATA");

		return ts_cm_functions->process_cagg_viewstmt(args->parsetree,
													  args->query_string,
													  args->pstmt,
													  parse_results);
	}

	return DDL_CONTINUE;
}

static DDLResult
process_refresh_mat_view_start(ProcessUtilityArgs *args)
{
	RefreshMatViewStmt *stmt = castNode(RefreshMatViewStmt, args->parsetree);
	Oid view_relid = RangeVarGetRelid(stmt->relation, NoLock, true);
	const ContinuousAgg *cagg;

	if (!OidIsValid(view_relid))
		return DDL_CONTINUE;

	cagg = ts_continuous_agg_find_by_relid(view_relid);

	if (cagg)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("operation not supported on continuous aggregate"),
				 errdetail("A continuous aggregate does not support REFRESH MATERIALIZED VIEW."),
				 errhint("Use \"refresh_continuous_aggregate\" or set up a policy to refresh the "
						 "continuous aggregate.")));

	return DDL_CONTINUE;
}

static DDLResult
preprocess_execute(ProcessUtilityArgs *args)
{
#ifdef USE_TELEMETRY
	ListCell *lc;
	ExecuteStmt *stmt = (ExecuteStmt *) args->parsetree;
	PreparedStatement *entry = FetchPreparedStatement(stmt->name, false);
	if (!entry)
		return DDL_CONTINUE;

	foreach (lc, entry->plansource->query_list)
	{
		Query *query = lfirst_node(Query, lc);
		ts_telemetry_function_info_gather(query);
	}
#endif
	return DDL_CONTINUE;
}

/*
 * Handle DDL commands before they have been processed by PostgreSQL.
 */
static DDLResult
process_ddl_command_start(ProcessUtilityArgs *args)
{
	bool check_read_only = true;
	ts_process_utility_handler_t handler;

	switch (nodeTag(args->parsetree))
	{
		case T_CreateForeignTableStmt:
			handler = process_create_foreign_table_start;
			break;
		case T_AlterForeignServerStmt:
			handler = process_alter_foreign_server;
			break;
		case T_CreateForeignServerStmt:
			handler = process_create_foreign_server_start;
			break;
		case T_AlterObjectSchemaStmt:
			handler = process_alterobjectschema;
			break;
		case T_TruncateStmt:
			handler = process_truncate;
			break;
		case T_AlterTableStmt:
			handler = process_altertable_start;
			break;
		case T_RenameStmt:
			handler = process_rename;
			break;
		case T_IndexStmt:
			handler = process_index_start;
			break;
		case T_CreateTrigStmt:
			handler = process_create_trigger_start;
			break;
		case T_RuleStmt:
			handler = process_create_rule_start;
			break;
		case T_DropStmt:
			/*
			 * Drop associated metadata/chunks but also continue on to drop
			 * the main table. Because chunks are deleted before the main
			 * table is dropped, the drop respects CASCADE in the expected
			 * way.
			 */
			handler = process_drop_start;
			break;
		case T_DropRoleStmt:
			handler = process_drop_role;
			break;
		case T_DropTableSpaceStmt:
			handler = process_drop_tablespace;
			break;
		case T_GrantStmt:
			handler = process_grant_and_revoke;
			break;
		case T_GrantRoleStmt:
			handler = process_grant_and_revoke_role;
			break;
		case T_CopyStmt:
			check_read_only = false;
			handler = process_copy;
			break;
		case T_VacuumStmt:
			handler = process_vacuum;
			break;
		case T_ReindexStmt:
			handler = process_reindex;
			break;
		case T_ClusterStmt:
			handler = process_cluster_start;
			break;
		case T_ViewStmt:
			handler = process_viewstmt;
			break;
		case T_RefreshMatViewStmt:
			handler = process_refresh_mat_view_start;
			break;
		case T_CreateTableAsStmt:
			handler = process_create_table_as;
			break;
		case T_ExecuteStmt:
			check_read_only = false;
			handler = preprocess_execute;
			break;
		default:
			handler = NULL;
			break;
	}

	if (handler == NULL)
		return DDL_CONTINUE;

	if (check_read_only)
		PreventCommandIfReadOnly(CreateCommandName(args->parsetree));

	return handler(args);
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
		default:
			break;
	}
}

static void
process_drop_constraint_on_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	const char *hypertable_constraint_name = arg;
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);

	/* drop both metadata and table; sql_drop won't be called recursively */
	ts_chunk_constraint_delete_by_hypertable_constraint_name(chunk->fd.id,
															 hypertable_constraint_name,
															 true,
															 true);
}

static void
process_drop_table_constraint(EventTriggerDropObject *obj)
{
	EventTriggerDropTableConstraint *constraint = (EventTriggerDropTableConstraint *) obj;
	Hypertable *ht;

	Assert(obj->type == EVENT_TRIGGER_DROP_TABLE_CONSTRAINT);

	/* do not use relids because underlying table could be gone */
	ht = ts_hypertable_get_by_name(constraint->schema, constraint->table);

	if (ht != NULL)
	{
		CatalogSecurityContext sec_ctx;

		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

		/* Recurse to each chunk and drop the corresponding constraint */
		foreach_chunk(ht, process_drop_constraint_on_chunk, (void *) constraint->constraint_name);

		ts_catalog_restore_user(&sec_ctx);
	}
	else
	{
		/* Cannot get the full chunk here because it's table might be dropped */
		int32 chunk_id;
		bool found = ts_chunk_get_id(constraint->schema, constraint->table, &chunk_id, true);

		if (found)
			ts_chunk_constraint_delete_by_constraint_name(chunk_id,
														  constraint->constraint_name,
														  true,
														  false);
	}
}

static void
process_drop_index(EventTriggerDropObject *obj)
{
	EventTriggerDropRelation *index = (EventTriggerDropRelation *) obj;

	Assert(obj->type == EVENT_TRIGGER_DROP_INDEX);
	ts_chunk_index_delete_by_name(index->schema, index->name, true);
}

static void
process_drop_table(EventTriggerDropObject *obj)
{
	EventTriggerDropRelation *table = (EventTriggerDropRelation *) obj;

	Assert(obj->type == EVENT_TRIGGER_DROP_TABLE || obj->type == EVENT_TRIGGER_DROP_FOREIGN_TABLE);
	ts_hypertable_delete_by_name(table->schema, table->name);
	ts_chunk_delete_by_name(table->schema, table->name, DROP_RESTRICT);
}

static void
process_drop_schema(EventTriggerDropObject *obj)
{
	EventTriggerDropSchema *schema = (EventTriggerDropSchema *) obj;
	int count;

	Assert(obj->type == EVENT_TRIGGER_DROP_SCHEMA);

	if (strcmp(schema->schema, INTERNAL_SCHEMA_NAME) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop the internal schema for extension \"%s\"", EXTENSION_NAME),
				 errhint("Use DROP EXTENSION to remove the extension and the schema.")));

	/*
	 * Check for any remaining hypertables that use the schema as its
	 * associated schema. For matches, we reset their associated schema to the
	 * INTERNAL schema
	 */
	count = ts_hypertable_reset_associated_schema_name(schema->schema);

	if (count > 0)
		ereport(NOTICE,
				(errmsg("the chunk storage schema changed to \"%s\" for %d hypertable%c",
						INTERNAL_SCHEMA_NAME,
						count,
						(count > 1) ? 's' : '\0')));
}

static void
process_drop_trigger(EventTriggerDropObject *obj)
{
	EventTriggerDropTrigger *trigger_event = (EventTriggerDropTrigger *) obj;
	Hypertable *ht;

	Assert(obj->type == EVENT_TRIGGER_DROP_TRIGGER);

	/* do not use relids because underlying table could be gone */
	ht = ts_hypertable_get_by_name(trigger_event->schema, trigger_event->table);

	if (ht != NULL)
	{
		/* Recurse to each chunk and drop the corresponding trigger */
		ts_hypertable_drop_trigger(ht->main_table_relid, trigger_event->trigger_name);
	}
}

static void
process_drop_view(EventTriggerDropView *dropped_view)
{
	ts_continuous_agg_drop(dropped_view->schema, dropped_view->view_name);
}

static void
process_drop_foreign_server(EventTriggerDropObject *obj)
{
	EventTriggerDropForeignServer *server = (EventTriggerDropForeignServer *) obj;

	Assert(obj->type == EVENT_TRIGGER_DROP_FOREIGN_SERVER);
	ts_hypertable_data_node_delete_by_node_name(server->servername);
	ts_chunk_data_node_delete_by_node_name(server->servername);
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
		case EVENT_TRIGGER_DROP_FOREIGN_TABLE:
			process_drop_table(obj);
			break;
		case EVENT_TRIGGER_DROP_SCHEMA:
			process_drop_schema(obj);
			break;
		case EVENT_TRIGGER_DROP_TRIGGER:
			process_drop_trigger(obj);
			break;
		case EVENT_TRIGGER_DROP_VIEW:
			process_drop_view((EventTriggerDropView *) obj);
			break;
		case EVENT_TRIGGER_DROP_FOREIGN_SERVER:
			process_drop_foreign_server(obj);
			break;
	}
}

/*
 * ProcessUtility hook for DDL commands that have not yet been processed by
 * PostgreSQL.
 */
static void
timescaledb_ddl_command_start(PlannedStmt *pstmt, const char *query_string,
#if PG14_GE
							  bool readonly_tree,
#endif
							  ProcessUtilityContext context, ParamListInfo params,
							  QueryEnvironment *queryEnv, DestReceiver *dest,
							  QueryCompletion *completion_tag)
{
	ProcessUtilityArgs args = {
		.query_string = query_string,
		.context = context,
		.params = params,
#if PG14_GE
		.readonly_tree = readonly_tree,
#endif
		.dest = dest,
		.completion_tag = completion_tag,
		.pstmt = pstmt,
		.parsetree = pstmt->utilityStmt,
		.queryEnv = queryEnv,
		.parse_state = make_parsestate(NULL),
		.hypertable_list = NIL
	};

	bool altering_timescaledb = false;
	DDLResult result;

	args.parse_state->p_sourcetext = query_string;

	if (IsA(args.parsetree, AlterExtensionStmt))
	{
		AlterExtensionStmt *stmt = (AlterExtensionStmt *) args.parsetree;

		altering_timescaledb = (strcmp(stmt->extname, EXTENSION_NAME) == 0);
	}

	/*
	 * We don't want to load the extension if we just got the command to alter
	 * it.
	 */
	if (altering_timescaledb || !ts_extension_is_loaded())
	{
		prev_ProcessUtility(&args);
		return;
	}

	/*
	 * Process Utility/DDL operation locally then pass it on for
	 * execution in TSL.
	 */
	result = process_ddl_command_start(&args);

	/*
	 * We need to run tsl-side ddl_command_start hook before
	 * standard process utility hook to maintain proper invocation
	 * order of sql_drop and ddl_command_end triggers.
	 */
	if (ts_cm_functions->ddl_command_start)
		ts_cm_functions->ddl_command_start(&args);

	if (result == DDL_CONTINUE)
		prev_ProcessUtility(&args);
}

static void
process_ddl_event_command_end(EventTriggerData *trigdata)
{
	ListCell *lc;

	/* Inhibit collecting new commands while in the trigger */
	EventTriggerInhibitCommandCollection();

	if (ts_cm_functions->ddl_command_end)
		ts_cm_functions->ddl_command_end(trigdata);

	switch (nodeTag(trigdata->parsetree))
	{
		case T_AlterTableStmt:
		case T_CreateTrigStmt:
		case T_CreateStmt:
		case T_IndexStmt:
			foreach (lc, ts_event_trigger_ddl_commands())
				process_ddl_command_end(lfirst(lc));
			break;
		default:
			break;
	}

	EventTriggerUndoInhibitCommandCollection();
}

static void
process_ddl_event_sql_drop(EventTriggerData *trigdata)
{
	ListCell *lc;
	List *dropped_objects = ts_event_trigger_dropped_objects();

	if (ts_cm_functions->sql_drop)
		ts_cm_functions->sql_drop(dropped_objects);

	foreach (lc, dropped_objects)
		process_ddl_sql_drop(lfirst(lc));
}

TS_FUNCTION_INFO_V1(ts_timescaledb_process_ddl_event);

/*
 * Event trigger hook for DDL commands that have alread been handled by
 * PostgreSQL (i.e., "ddl_command_end" and "sql_drop" events).
 */
Datum
ts_timescaledb_process_ddl_event(PG_FUNCTION_ARGS)
{
	EventTriggerData *trigdata = (EventTriggerData *) fcinfo->context;

	if (!CALLED_AS_EVENT_TRIGGER(fcinfo))
		elog(ERROR, "not fired by event trigger manager");

	if (!ts_extension_is_loaded())
		PG_RETURN_NULL();

	if (strcmp("ddl_command_end", trigdata->event) == 0)
		process_ddl_event_command_end(trigdata);
	else if (strcmp("sql_drop", trigdata->event) == 0)
		process_ddl_event_sql_drop(trigdata);

	PG_RETURN_NULL();
}

extern void
ts_process_utility_set_expect_chunk_modification(bool expect)
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
			break;
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
			break;
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
