/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/nodes.h>
#include <nodes/makefuncs.h>
#include <tcop/utility.h>
#include <catalog/namespace.h>
#include <catalog/index.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_trigger.h>
#include <commands/copy.h>
#include <commands/vacuum.h>
#include <commands/defrem.h>
#include <commands/trigger.h>
#include <commands/tablecmds.h>
#include <commands/cluster.h>
#include <commands/event_trigger.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <storage/lmgr.h>
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
#include "compat.h"
#if PG11_LT /* PG11 consolidates pg_foo_fn.h -> pg_foo.h */
#include <catalog/pg_inherits_fn.h>
#include <catalog/pg_constraint_fn.h>
#endif

#include <miscadmin.h>

#include "export.h"
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
#include "hypertable.h"
#include "hypertable_cache.h"
#include "dimension_vector.h"
#include "indexing.h"
#include "scan_iterator.h"
#include "trigger.h"
#include "utils.h"
#include "with_clause_parser.h"
#include "cross_module_fn.h"
#include "continuous_agg.h"
#include "compression_with_clause.h"
#include "partitioning.h"

#include "cross_module_fn.h"

void _process_utility_init(void);
void _process_utility_fini(void);

static ProcessUtility_hook_type prev_ProcessUtility_hook;

static bool expect_chunk_modification = false;
static bool process_altertable_set_options(AlterTableCmd *cmd, Hypertable *ht);
static bool process_altertable_reset_options(AlterTableCmd *cmd, Hypertable *ht);

/* Call the default ProcessUtility and handle PostgreSQL version differences */
static void
prev_ProcessUtility(ProcessUtilityArgs *args)
{
	if (prev_ProcessUtility_hook != NULL)
	{
#if !PG96
		/* Call any earlier hooks */
		(prev_ProcessUtility_hook)(args->pstmt,
								   args->query_string,
								   args->context,
								   args->params,
								   args->queryEnv,
								   args->dest,
								   args->completion_tag);
#else
		(prev_ProcessUtility_hook)(args->parsetree,
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
#if !PG96
		standard_ProcessUtility(args->pstmt,
								args->query_string,
								args->context,
								args->params,
								args->queryEnv,
								args->dest,
								args->completion_tag);
#else
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

/* on continuous aggregate materialization tables we block all altercommands except for ADD INDEX */
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
	if (!TS_HYPERTABLE_HAS_COMPRESSION(ht))
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
				continue;
				/*
				 * BLOCKED:
				 *
				 * List things that we want to explicitly block for documentation purposes
				 * But also block everything else as well.
				 */
#if PG12_LT
			case AT_AddOids:
			case AT_DropOids:
			case AT_AddOidsRecurse:
#endif
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
relation_not_only(RangeVar *rv)
{
#if !PG96
	bool only = !rv->inh;
#else
	bool only = (rv->inhOpt == INH_NO);
#endif
	if (only)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ONLY option not supported on hypertable operations")));
}

static void
process_add_hypertable(ProcessUtilityArgs *args, Hypertable *ht)
{
	args->hypertable_list = lappend_oid(args->hypertable_list, ht->main_table_relid);
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
		Chunk *chunk = ts_chunk_get_by_relid(relid, 0, false);

		if (NULL != chunk)
			ts_chunk_set_schema(chunk, alterstmt->newschema);
	}
	else
	{
		ts_hypertable_set_schema(ht, alterstmt->newschema);

		process_add_hypertable(args, ht);
	}

	ts_cache_release(hcache);
}

static void
process_alterviewschema(ProcessUtilityArgs *args)
{
	AlterObjectSchemaStmt *alterstmt = (AlterObjectSchemaStmt *) args->parsetree;
	Oid relid;
	char *schema;
	char *name;

	Assert(alterstmt->objectType == OBJECT_VIEW);

	if (NULL == alterstmt->relation)
		return;

	relid = RangeVarGetRelid(alterstmt->relation, NoLock, true);
	if (!OidIsValid(relid))
		return;

	schema = get_namespace_name(get_rel_namespace(relid));
	name = get_rel_name(relid);

	ts_continuous_agg_rename_view(schema, name, alterstmt->newschema, name);
}

/* Change the schema of a hypertable or a chunk */
static void
process_alterobjectschema(ProcessUtilityArgs *args)
{
	AlterObjectSchemaStmt *alterstmt = (AlterObjectSchemaStmt *) args->parsetree;

	switch (alterstmt->objectType)
	{
		case OBJECT_TABLE:
			process_altertableschema(args);
			break;
		case OBJECT_VIEW:
			process_alterviewschema(args);
			break;
		default:
			return;
	};
}

static bool
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
			return false;

		ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_MISSING_OK, &hcache);

		if (ht == NULL)
		{
			ts_cache_release(hcache);
			return false;
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
		return false;
	}

	/* Performs acl check in here inside `copy_security_check` */
	timescaledb_DoCopy(stmt, args->query_string, &processed, ht);

	if (args->completion_tag)
		snprintf(args->completion_tag, COMPLETION_TAG_BUFSIZE, "COPY " UINT64_FORMAT, processed);

	process_add_hypertable(args, ht);

	ts_cache_release(hcache);

	return true;
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

/*
 * PG11 modified  how vacuum works (see:
 * https://github.com/postgres/postgres/commit/11d8d72c27a64ea4e30adce11cf6c4f3dd3e60db)
 * so that a) one can run vacuum on multiple tables at once with `VACUUM table1,
 * table2;` and b) because of this modified the way that the VacuumStmt node in
 * the planner works due to this change. It now has a list of VacuumRelations.
 * Given this change it seemed easier to take rewrite this completely for 11 to
 * take advantage of the new changes.
 */
#if PG11_LT
typedef struct VacuumCtx
{
	VacuumStmt *stmt;
	bool is_toplevel;
} VacuumCtx;

/* Vacuums a single chunk */
static void
vacuum_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	VacuumCtx *ctx = (VacuumCtx *) arg;
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	ctx->stmt->relation->relname = NameStr(chunk->fd.table_name);
	ctx->stmt->relation->schemaname = NameStr(chunk->fd.schema_name);
	ExecVacuum(ctx->stmt, ctx->is_toplevel);
}

/* Vacuums each chunk of a hypertable */
static bool
process_vacuum(ProcessUtilityArgs *args)
{
	VacuumStmt *stmt = (VacuumStmt *) args->parsetree;
	bool is_toplevel = (args->context == PROCESS_UTILITY_TOPLEVEL);
	VacuumCtx ctx = {
		.stmt = stmt,
		.is_toplevel = is_toplevel,
	};
	Oid hypertable_oid;
	Cache *hcache;
	Hypertable *ht;

	if (stmt->relation == NULL)
		/* Vacuum is for all tables */
		return false;

	hypertable_oid = ts_hypertable_relid(stmt->relation);
	if (!OidIsValid(hypertable_oid))
		return false;

	PreventCommandDuringRecovery((stmt->options & VACOPT_VACUUM) ? "VACUUM" : "ANALYZE");

	ht = ts_hypertable_cache_get_cache_and_entry(hypertable_oid, CACHE_FLAG_MISSING_OK, &hcache);

	if (ht)
		process_add_hypertable(args, ht);

	/* allow vacuum to be cross-commit */
	hcache->release_on_commit = false;
	foreach_chunk(ht, vacuum_chunk, &ctx);
	hcache->release_on_commit = true;

	ts_cache_release(hcache);

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
#else
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
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);
	VacuumRelation *chunk_vacuum_rel;
	RangeVar *chunk_range_var = copyObject(ctx->ht_vacuum_rel->relation);

	chunk_range_var->relname = NameStr(chunk->fd.table_name);
	chunk_range_var->schemaname = NameStr(chunk->fd.schema_name);
	chunk_vacuum_rel =
		makeVacuumRelation(chunk_range_var, chunk_relid, ctx->ht_vacuum_rel->va_cols);

	ctx->chunk_rels = lappend(ctx->chunk_rels, chunk_vacuum_rel);
}

/* Vacuums a hypertable and all of it's chunks */
static bool
process_vacuum(ProcessUtilityArgs *args)
{
	VacuumStmt *stmt = (VacuumStmt *) args->parsetree;
	bool is_toplevel = (args->context == PROCESS_UTILITY_TOPLEVEL);
	VacuumCtx ctx = {
		.ht_vacuum_rel = NULL,
		.chunk_rels = NIL,
	};
	ListCell *lc;
	Cache *hcache;
	Hypertable *ht;
	bool affects_hypertable = false;

	if (stmt->rels == NIL)
		/* Vacuum is for all tables */
		return false;

	hcache = ts_hypertable_cache_pin();
	foreach (lc, stmt->rels)
	{
		VacuumRelation *vacuum_rel = lfirst_node(VacuumRelation, lc);
		Oid table_relid = vacuum_rel->oid;

		if (!OidIsValid(table_relid) && vacuum_rel->relation != NULL)
			table_relid = RangeVarGetRelid(vacuum_rel->relation, NoLock, true);

		if (!OidIsValid(table_relid))
			continue;

		ht = ts_hypertable_cache_get_entry(hcache, table_relid, CACHE_FLAG_MISSING_OK);

		if (!ht)
			continue;

		affects_hypertable = true;
		process_add_hypertable(args, ht);
		ctx.ht_vacuum_rel = vacuum_rel;
		foreach_chunk(ht, add_chunk_to_vacuum, &ctx);
	}
	ts_cache_release(hcache);
	if (!affects_hypertable)
		return false;

	stmt->rels = list_concat(ctx.chunk_rels, stmt->rels);
	PreventCommandDuringRecovery((stmt->options && VACOPT_VACUUM) ? "VACUUM" : "ANALYZE");
	/* ACL permission checks inside vacuum_rel and analyze_rel called by this ExecVacuum */
	ExecVacuum(
#if PG12_GE
		args->parse_state,
#endif
		stmt,
		is_toplevel);
	return true;
}
#endif

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
#if !PG96
	return rv->inh;
#else
	if (rv->inhOpt == INH_DEFAULT)
	{
		char *inherit_guc = GetConfigOptionByName("SQL_inheritance", NULL, false);

		return strncmp(inherit_guc, "on", 2) == 0;
	}
	return rv->inhOpt == INH_YES;
#endif
}

/* handle forwading TRUNCATEs to the chunks of a hypertable */
static void
handle_truncate_hypertable(ProcessUtilityArgs *args, TruncateStmt *stmt, Hypertable *ht)
{
	process_add_hypertable(args, ht);

	/* Delete the metadata */
	ts_chunk_delete_by_hypertable_id(ht->fd.id);

	/* Drop the chunk tables */
	foreach_chunk(ht, process_truncate_chunk, stmt);
}

/*
 * Truncate a hypertable.
 */
static bool
process_truncate(ProcessUtilityArgs *args)
{
	TruncateStmt *stmt = (TruncateStmt *) args->parsetree;
	Cache *hcache = ts_hypertable_cache_pin();
	ListCell *cell;

	/* Call standard process utility first to truncate all tables */
	prev_ProcessUtility(args);

	/* For all hypertables, we drop the now empty chunks. We also propogate the
	 * TRUNCATE call to the compressed version of the hypertable, if it exists.
	 */
	foreach (cell, stmt->relations)
	{
		RangeVar *rv = lfirst(cell);
		Oid relid;

		if (NULL == rv)
			continue;

		relid = RangeVarGetRelid(rv, NoLock, true);

		if (OidIsValid(relid))
		{
			Hypertable *ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);

			if (ht != NULL)
			{
				ContinuousAggHypertableStatus agg_status =
					ts_continuous_agg_hypertable_status(ht->fd.id);

				ts_hypertable_permissions_check_by_id(ht->fd.id);

				if ((agg_status & HypertableIsMaterialization) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg(
								 "cannot TRUNCATE a hypertable underlying a continuous aggregate"),
							 errhint(
								 "DELETE from the table this continuous aggregate is based on.")));

				if (agg_status == HypertableIsRawTable)
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot TRUNCATE a hypertable that has a continuous aggregate"),
							 errhint(
								 "either DROP the continuous aggregate, or DELETE or drop_chunks "
								 "from the table this continuous aggregate is based on.")));

				if (!relation_should_recurse(rv))
					ereport(ERROR,
							(errcode(ERRCODE_WRONG_OBJECT_TYPE),
							 errmsg("cannot truncate only a hypertable"),
							 errhint("Do not specify the ONLY keyword, or use truncate"
									 " only on the chunks directly.")));

				handle_truncate_hypertable(args, stmt, ht);
				process_add_hypertable(args, ht);

				/* Delete the metadata */
				ts_chunk_delete_by_hypertable_id(ht->fd.id);

				/* Drop the chunk tables */
				foreach_chunk(ht, process_truncate_chunk, stmt);

				/* propogate to the compressed hypertable */
				if (TS_HYPERTABLE_HAS_COMPRESSION(ht))
				{
					Hypertable *compressed_ht =
						ts_hypertable_cache_get_entry_by_id(hcache,
															ht->fd.compressed_hypertable_id);
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
		}
	}

	ts_cache_release(hcache);

	return true;
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

	foreach (lc, stmt->objects)
	{
		List *object = lfirst(lc);
		RangeVar *relation = makeRangeVarFromNameList(object);
		Oid relid;
		Chunk *chunk;

		if (NULL == relation)
			continue;

		relid = RangeVarGetRelid(relation, NoLock, true);
		chunk = ts_chunk_get_by_relid(relid, 0, false);
		if (chunk != NULL)
		{
			if (ts_chunk_contains_compressed_data(chunk))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("dropping compressed chunks not supported"),
						 errhint(
							 "Please drop the corresponding chunk on the uncompressed hypertable "
							 "instead.")));

			/* if cascade is enabled, delete the compressed chunk with cascade too. Otherwise
			 *  it would be blocked if there are depenent objects */
			if (stmt->behavior == DROP_CASCADE && chunk->fd.compressed_chunk_id != INVALID_CHUNK_ID)
			{
				Chunk *compressed_chunk =
					ts_chunk_get_by_id(chunk->fd.compressed_chunk_id, 0, false);
				/* The chunk may have been delete by a CASCADE */
				if (compressed_chunk != NULL)
					ts_chunk_drop(compressed_chunk, stmt->behavior, DEBUG1);
			}
		}
	}
}

/*
 * We need to drop hypertable chunks and associated compressed hypertables
 * when dropping hypertables to maintain correct semantics wrt CASCADE modifiers.
 * Also block dropping compressed hypertables directly.
 */
static bool
process_drop_hypertable(ProcessUtilityArgs *args, DropStmt *stmt)
{
	Cache *hcache = ts_hypertable_cache_pin();
	ListCell *lc;
	bool handled = false;

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

				if (ht->fd.compressed)
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
				if (stmt->behavior == DROP_CASCADE && TS_HYPERTABLE_HAS_COMPRESSION(ht))
				{
					Hypertable *compressed_hypertable =
						ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
					ts_hypertable_drop(compressed_hypertable, DROP_CASCADE);
				}
			}

			handled = true;
		}
	}

	ts_cache_release(hcache);

	return handled;
}

/*
 *  We need to ensure that DROP INDEX uses only one hypertable per query,
 *  otherwise query string might not be reusable for execution on a
 *  remote server.
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
		}
	}

	ts_cache_release(hcache);
}

/* Note that DROP TABLESPACE does not have a hook in event triggers so cannot go
 * through process_ddl_sql_drop */
static void
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
}

/*
 * Handle GRANT / REVOKE.
 *
 * A revoke is a GrantStmt with 'is_grant' set to false.
 */
static bool
process_grant_and_revoke(ProcessUtilityArgs *args)
{
	GrantStmt *stmt = (GrantStmt *) args->parsetree;

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
/*
 * PG11 consolidated several ACL_OBJECT_FOO or similar to the already extant
 * OBJECT_FOO see:
 * https://github.com/postgres/postgres/commit/2c6f37ed62114bd5a092c20fe721bd11b3bcb91e
 * so we can't simply #define OBJECT_TABLESPACE ACL_OBJECT_TABLESPACE and have
 * things work correctly for previous versions.
 */
#if PG11_LT
		case ACL_OBJECT_TABLESPACE:
#else
		case OBJECT_TABLESPACE:
#endif
			ts_tablespace_validate_revoke(stmt);
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

	ts_tablespace_validate_revoke_role(stmt);

	return true;
}

/* Force the use of CASCADE to drop continuous aggregates */
static void
block_dropping_continuous_aggregates_without_cascade(ProcessUtilityArgs *args, DropStmt *stmt)
{
	ListCell *lc;

	if (stmt->behavior == DROP_CASCADE)
		return;

	foreach (lc, stmt->objects)
	{
		List *object = lfirst(lc);
		RangeVar *relation = makeRangeVarFromNameList(object);
		Oid relid;
		char *schema;
		char *name;
		ContinuousAgg *cagg;

		if (NULL == relation)
			continue;

		relid = RangeVarGetRelid(relation, NoLock, true);
		if (!OidIsValid(relid))
			continue;

		schema = get_namespace_name(get_rel_namespace(relid));
		name = get_rel_name(relid);

		cagg = ts_continuous_agg_find_by_view_name(schema, name);
		if (cagg == NULL)
			continue;

		if (ts_continuous_agg_view_type(&cagg->data, schema, name) == ContinuousAggUserView)
			ereport(ERROR,
					(errcode(ERRCODE_DEPENDENT_OBJECTS_STILL_EXIST),
					 errmsg("dropping a continuous aggregate requires using CASCADE")));
	}
}

static void
process_drop(ProcessUtilityArgs *args)
{
	DropStmt *stmt = (DropStmt *) args->parsetree;

	switch (stmt->removeType)
	{
		case OBJECT_TABLE:
			process_drop_hypertable(args, stmt);
			process_drop_chunk(args, stmt);
			break;
		case OBJECT_INDEX:
			process_drop_hypertable_index(args, stmt);
			break;
		case OBJECT_VIEW:
			block_dropping_continuous_aggregates_without_cascade(args, stmt);
			break;
		default:
			break;
	}
}

static void
reindex_chunk(Hypertable *ht, Oid chunk_relid, void *arg)
{
	ProcessUtilityArgs *args = arg;
	ReindexStmt *stmt = (ReindexStmt *) args->parsetree;
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	switch (stmt->kind)
	{
		case REINDEX_OBJECT_TABLE:
			stmt->relation->relname = NameStr(chunk->fd.table_name);
			stmt->relation->schemaname = NameStr(chunk->fd.schema_name);
			ReindexTable(stmt->relation,
						 stmt->options
#if PG12_GE
						 ,
						 stmt->concurrent /* TODO test */
#endif
			);
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
process_reindex(ProcessUtilityArgs *args)
{
	ReindexStmt *stmt = (ReindexStmt *) args->parsetree;
	Oid relid;
	Cache *hcache;
	Hypertable *ht;
	bool ret = false;

	if (NULL == stmt->relation)
		/* Not a case we are interested in */
		return false;

	relid = RangeVarGetRelid(stmt->relation, NoLock, true);

	if (!OidIsValid(relid))
		return false;

	hcache = ts_hypertable_cache_pin();

	switch (stmt->kind)
	{
		case REINDEX_OBJECT_TABLE:
			ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);

			if (NULL != ht)
			{
				PreventCommandDuringRecovery("REINDEX");
				ts_hypertable_permissions_check_by_id(ht->fd.id);

				if (foreach_chunk(ht, reindex_chunk, args) >= 0)
					ret = true;

				process_add_hypertable(args, ht);
			}
			break;
		case REINDEX_OBJECT_INDEX:
			ht = ts_hypertable_cache_get_entry(hcache,
											   IndexGetRelation(relid, true),
											   CACHE_FLAG_MISSING_OK);

			if (NULL != ht)
			{
				process_add_hypertable(args, ht);
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

	return ret;
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
		Chunk *chunk = ts_chunk_get_by_relid(relid, 0, false);

		if (NULL != chunk)
			ts_chunk_set_name(chunk, stmt->newname);
	}
	else
	{
		ts_hypertable_set_name(ht, stmt->newname);

		process_add_hypertable(args, ht);
	}
}

static void
process_rename_column(ProcessUtilityArgs *args, Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);
	Dimension *dim;

	if (NULL == ht)
	{
		Chunk *chunk = ts_chunk_get_by_relid(relid, 0, false);

		if (NULL != chunk)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot rename column \"%s\" of hypertable chunk \"%s\"",
							stmt->subname,
							get_rel_name(relid)),
					 errhint("Rename the hypertable column instead.")));
		return;
	}

	/* block renaming columns on the materialization table of a continuous agg*/
	if ((ts_continuous_agg_hypertable_status(ht->fd.id) & HypertableIsMaterialization) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot rename column \"%s\" of materialization table \"%s\"",
						stmt->subname,
						get_rel_name(relid))));

	process_add_hypertable(args, ht);

	dim = ts_hyperspace_get_dimension_by_name(ht->space, DIMENSION_TYPE_ANY, stmt->subname);

	if (NULL == dim)
		return;

	ts_dimension_set_name(dim, stmt->newname);
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

		process_add_hypertable(args, ht);
	}
	else
	{
		Chunk *chunk = ts_chunk_get_by_relid(tablerelid, 0, false);

		if (NULL != chunk)
			ts_chunk_index_rename(chunk, relid, stmt->newname);
	}
}

static void
process_rename_view(Oid relid, RenameStmt *stmt)
{
	char *schema = get_namespace_name(get_rel_namespace(relid));
	char *name = get_rel_name(relid);
	ts_continuous_agg_rename_view(schema, name, schema, stmt->newname);
}

/* Visit all internal catalog tables with a schema column to check for applicable rename */
static void
process_rename_schema(RenameStmt *stmt)
{
	int i = 0;

	/* Block any renames of our internal schemas */
	for (i = 0; i < NUM_TIMESCALEDB_SCHEMAS; i++)
	{
		if (strncmp(stmt->subname, timescaledb_schema_names[i], NAMEDATALEN) == 0)
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
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

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
process_rename_constraint(ProcessUtilityArgs *args, Cache *hcache, Oid relid, RenameStmt *stmt)
{
	Hypertable *ht;

	ht = ts_hypertable_cache_get_entry(hcache, relid, CACHE_FLAG_MISSING_OK);

	if (NULL != ht)
	{
		relation_not_only(stmt->relation);
		process_add_hypertable(args, ht);
		foreach_chunk(ht, rename_hypertable_constraint, stmt);
	}
	else
	{
		Chunk *chunk = ts_chunk_get_by_relid(relid, 0, false);

		if (NULL != chunk)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("renaming constraints on chunks is not supported")));
	}
}

static void
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
			return;
	}
	else
	{
		/*
		 * stmt->relation never be NULL unless we are renaming a schema
		 */
		if (stmt->renameType != OBJECT_SCHEMA)
			return;
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
			process_rename_constraint(args, hcache, relid, stmt);
			break;
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

	if (TS_HYPERTABLE_HAS_COMPRESSION(ht))
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
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	ts_chunk_constraint_create_on_chunk(chunk, hypertable_constraint_oid);
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
					 errdetail("cannot drop column that is a hypertable partitioning (space or "
							   "time) dimension")));
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
	IndexInfo *indexinfo;
	int n_ht_atts;
	bool ht_hasoid;

	/* Concurrency testing options. */
#ifdef DEBUG
	/*
	 * If barrier_table is a valid Oid we try to acquire a lock on it at the
	 * start of each chunks sub-transaction.
	 */
	Oid barrier_table;

	/*
	 * if max_chunks >= 0 we'll create indicies on at most max_chunks, and
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
	IndexStmt *stmt = transformIndexStmt(chunk_relid, info->stmt, NULL);
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	ts_chunk_index_create_from_stmt(stmt, chunk->fd.id, chunk_relid, ht->fd.id, info->obj.objectId);
}

static void
process_index_chunk_multitransaction(int32 hypertable_id, Oid chunk_relid, void *arg)
{
	CreateIndexInfo *info = (CreateIndexInfo *) arg;
	CatalogSecurityContext sec_ctx;
	Chunk *chunk;
	Relation hypertable_index_rel;
	Relation chunk_rel;

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
	hypertable_index_rel = index_open(info->obj.objectId, AccessShareLock);

	chunk = ts_chunk_get_by_relid(chunk_relid, 0, true);

	/*
	 * use ts_chunk_index_create instead of ts_chunk_index_create_from_stmt to
	 * handle cases where the index is altered. Validation happens when
	 * creating the hypertable's index, which goes through the usual
	 * DefineIndex mechanism.
	 */
	if (chunk_index_columns_changed(info->extended_options.n_ht_atts,
									info->extended_options.ht_hasoid,
									RelationGetDescr(chunk_rel)))
		ts_adjust_indexinfo_attnos(info->extended_options.indexinfo,
								   info->main_table_relid,
								   hypertable_index_rel,
								   chunk_rel);

	ts_chunk_index_create_from_adjusted_index_info(hypertable_id,
												   hypertable_index_rel,
												   chunk->fd.id,
												   chunk_rel,
												   info->extended_options.indexinfo);

	index_close(hypertable_index_rel, NoLock);
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
	[HypertableIndexFlagMaxChunks] = {.arg_name = "max_chunks", .type_id = INT4OID, .default_val = Int32GetDatum(-1)},
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
static bool
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

	Assert(IsA(stmt, IndexStmt));

	/*
	 * PG11 adds some cases where the relation is not there, namely on
	 * declaratively partitioned tables, with partitioned indexes:
	 * https://github.com/postgres/postgres/commit/8b08f7d4820fd7a8ef6152a9dd8c6e3cb01e5f99
	 * we don't deal with them so we will just return immediately
	 */
	if (NULL == stmt->relation)
		return false;

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry_rv(hcache, stmt->relation);

	if (NULL == ht)
	{
		ts_cache_release(hcache);
		return false;
	}

	ts_hypertable_permissions_check_by_id(ht->fd.id);
	process_add_hypertable(args, ht);

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

	ts_indexing_verify_index(ht->space, stmt);

	if (info.extended_options.multitransaction)
		PreventInTransactionBlock(true,
								  "CREATE INDEX ... WITH (timescaledb.transaction_per_chunk)");

	/* CREATE INDEX on the root table of the hypertable */
	root_table_index = ts_indexing_root_table_create_index(stmt,
														   args->query_string,
														   info.extended_options.multitransaction);
	info.obj.objectId = root_table_index.objectId;

	/* CREATE INDEX on the chunks */

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
		/* Recurse to each chunk and create a corresponding index */
		foreach_chunk(ht, process_index_chunk, &info);

		ts_catalog_restore_user(&sec_ctx);
		ts_cache_release(hcache);
		return true;
	}

	/* create chunk indexes using a separate transaction for each chunk */

	/* we're about to release the hcache so store the main_table_relid for later */
	info.main_table_relid = ht->main_table_relid;
	main_table_relation = table_open(ht->main_table_relid, AccessShareLock);
	main_table_desc = RelationGetDescr(main_table_relation);

	main_table_index_relation = index_open(info.obj.objectId, AccessShareLock);
	main_table_index_lock_relid = main_table_index_relation->rd_lockInfo.lockRelId;

	info.extended_options.indexinfo = BuildIndexInfo(main_table_index_relation);
	info.extended_options.n_ht_atts = main_table_desc->natts;
	info.extended_options.ht_hasoid = TUPLE_DESC_HAS_OIDS(main_table_desc);

	index_close(main_table_index_relation, NoLock);

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

	table_close(main_table_relation, NoLock);

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

	UnlockRelationIdForSession(&main_table_index_lock_relid, AccessShareLock);

	return true;
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
process_cluster_start(ProcessUtilityArgs *args)
{
	ClusterStmt *stmt = (ClusterStmt *) args->parsetree;
	Cache *hcache;
	Hypertable *ht;
	bool handled = false;

	Assert(IsA(stmt, ClusterStmt));

	/* If this is a re-cluster on all tables, there is nothing we need to do */
	if (NULL == stmt->relation)
		return false;

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

		ts_hypertable_permissions_check_by_id(ht->fd.id);

		/*
		 * If CLUSTER is run inside a user transaction block; we bail out or
		 * otherwise we'd be holding locks way too long.
		 */
		PreventInTransactionBlock(is_top_level, "CLUSTER");

		process_add_hypertable(args, ht);

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
			return false;
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
		MemoryContextSwitchTo(old);

		hcache->release_on_commit = false;

		/* Commit to get out of starting transaction */
		PopActiveSnapshot();
		CommitTransactionCommand();

		foreach (lc, chunk_indexes)
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
			ts_chunk_index_mark_clustered(cim->chunkoid, cim->indexoid);

			/* Do the job. */

			/*
			 * Since we keep OIDs between transactions, there is a potential
			 * issue if an OID gets reassigned between two subtransactions
			 */
			cluster_rel(cim->chunkoid,
						cim->indexoid,
#if PG12_LT
						true,
						stmt->verbose
#else
						stmt->options
#endif
			);
			PopActiveSnapshot();
			CommitTransactionCommand();
		}

		hcache->release_on_commit = true;
		/* Start a new transaction for the cleanup work. */
		StartTransactionCommand();

		/* Clean up working storage */
		MemoryContextDelete(mcxt);

		UnlockRelationIdForSession(&cluster_index_lockid, AccessShareLock);
		handled = true;
	}

	ts_cache_release(hcache);
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
	Value *name = llast(tn->names);

	return name->val.str;
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
	Dimension *dim = ts_hyperspace_get_dimension_by_name(ht->space, DIMENSION_TYPE_ANY, cmd->name);

	if (NULL == dim)
		return;

	ts_dimension_set_type(dim, new_type);
	ts_process_utility_set_expect_chunk_modification(true);
	ts_chunk_recreate_all_constraints_for_dimension(ht->space, dim->fd.id);
	ts_process_utility_set_expect_chunk_modification(false);
}

static void
process_altertable_clusteron_end(Hypertable *ht, AlterTableCmd *cmd)
{
	Oid index_relid =
		get_relname_relid(cmd->name, get_namespace_oid(NameStr(ht->fd.schema_name), false));
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
		ts_tablespace_delete(ht->fd.id, NameStr(tspcs->tablespaces[0].fd.tablespace_name));
	}

	ts_tablespace_attach_internal(&tspc_name, ht->main_table_relid, true);
	foreach_chunk(ht, process_altertable_chunk, cmd);
	if (TS_HYPERTABLE_HAS_COMPRESSION(ht))
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

static bool
process_altertable_start_table(ProcessUtilityArgs *args)
{
	AlterTableStmt *stmt = (AlterTableStmt *) args->parsetree;
	Oid relid = AlterTableLookupRelation(stmt, NoLock);
	Cache *hcache;
	Hypertable *ht;
	ListCell *lc;
	bool handled = false;
	int num_cmds;

	if (!OidIsValid(relid))
		return false;

	check_chunk_alter_table_operation_allowed(relid, stmt);

	ht = ts_hypertable_cache_get_cache_and_entry(relid, CACHE_FLAG_MISSING_OK, &hcache);
	if (ht != NULL)
	{
		ts_hypertable_permissions_check_by_id(ht->fd.id);
		check_continuous_agg_alter_table_allowed(ht, stmt);
		check_alter_table_allowed_on_ht_with_compression(ht, stmt);
		relation_not_only(stmt->relation);
		process_add_hypertable(args, ht);
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
			case AT_AddColumnRecurse:
			{
				ColumnDef *col;
				ListCell *constraint_lc;

				Assert(IsA(cmd->def, ColumnDef));
				col = (ColumnDef *) cmd->def;

				if (NULL == ht)
					foreach (constraint_lc, col->constraints)
						verify_constraint_plaintable(stmt->relation, lfirst(constraint_lc));
				else
					foreach (constraint_lc, col->constraints)
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
#if !PG96
			case AT_AttachPartition:
			{
				RangeVar *relation;
				PartitionCmd *partstmt;

				partstmt = (PartitionCmd *) cmd->def;
				relation = partstmt->name;
				Assert(NULL != relation);

				if (InvalidOid != ts_hypertable_relid(relation))
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("hypertables do not support native "
									"postgres partitioning")));
				}
				break;
			}
#endif
			case AT_SetRelOptions:
			{
				if (num_cmds != 1)
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("ALTER TABLE <hypertable> SET does not support multiple "
									"clauses")));
				}
				if (ht != NULL)
				{
					handled = process_altertable_set_options(cmd, ht);
				}
				break;
			}
			case AT_ResetRelOptions:
			case AT_ReplaceRelOptions:
				process_altertable_reset_options(cmd, ht);
				break;
			default:
				break;
		}
	}

	ts_cache_release(hcache);
	return handled;
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

static bool
process_altertable_start_view(ProcessUtilityArgs *args)
{
	AlterTableStmt *stmt = (AlterTableStmt *) args->parsetree;
	Oid view_relid = AlterTableLookupRelation(stmt, NoLock);
	NameData view_name;
	NameData view_schema;
	ContinuousAgg *cagg;
	ListCell *lc;
	ContinuousAggViewType vtyp;

	if (!OidIsValid(view_relid))
		return false;

	namestrcpy(&view_name, get_rel_name(view_relid));
	namestrcpy(&view_schema, get_namespace_name(get_rel_namespace(view_relid)));
	cagg = ts_continuous_agg_find_by_view_name(NameStr(view_schema), NameStr(view_name));

	if (cagg == NULL)
		return false;

	continuous_agg_with_clause_perm_check(cagg, view_relid);

	vtyp = ts_continuous_agg_view_type(&cagg->data, NameStr(view_schema), NameStr(view_name));
	if (vtyp == ContinuousAggPartialView || vtyp == ContinuousAggDirectView)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter the internal view of a continuous aggregate")));

	foreach (lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		switch (cmd->subtype)
		{
			case AT_SetRelOptions:
				if (!IsA(cmd->def, List))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("expected set options to contain a list")));
				process_altercontinuousagg_set_with(cagg, view_relid, (List *) cmd->def);
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot alter only SET options of a continuous "
								"aggregate")));
		}
	}
	/* All commands processed by us, nothing for postgres to do.*/
	return true;
}

static bool
process_altertable_start(ProcessUtilityArgs *args)
{
	AlterTableStmt *stmt = (AlterTableStmt *) args->parsetree;
	switch (stmt->relkind)
	{
		case OBJECT_TABLE:
			return process_altertable_start_table(args);
		case OBJECT_VIEW:
			return process_altertable_start_view(args);
		default:
			return false;
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
#if PG12_LT
		case AT_AddOids:
#endif
		case AT_DropOids:
		case AT_SetOptions:
		case AT_ResetOptions:
		case AT_DropCluster:
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
		case AT_SetNotNull:
#if PG12_GE
		case AT_CheckNotNull: /*TODO test*/
#endif
		case AT_DropNotNull:
		case AT_AddOf:
		case AT_DropOf:
#if PG10_GE
		case AT_AddIdentity:
		case AT_SetIdentity:
		case AT_DropIdentity:
#endif
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
#if PG12_LT
		case AT_AddOidsRecurse:
#endif

			/*
			 * all of the above are internal commands that are hit in tests
			 * and correctly handled
			 */
			break;
		case AT_AddColumn:
		case AT_AddColumnRecurse:
		case AT_DropColumn:
		case AT_DropColumnRecurse:

			/*
			 * adding and dropping columns handled in
			 * process_altertable_start_table
			 */
			break;
		case AT_DropConstraint:
		case AT_DropConstraintRecurse:
			/* drop constraints handled by process_ddl_sql_drop */
			break;
		case AT_ProcessedConstraint:	   /* internal command never hit in our
											* test code, so don't know how to
											* handle */
		case AT_ReAddComment:			   /* internal command never hit in our test
											* code, so don't know how to handle */
		case AT_AddColumnToView:		   /* only used with views */
		case AT_AlterColumnGenericOptions: /* only used with foreign tables */
		case AT_GenericOptions:			   /* only used with foreign tables */
#if PG11_GE
		case AT_ReAddDomainConstraint: /* We should handle this in future,
										* new subset of constraints in PG11
										* currently not hit in test code */
#endif
#if PG10_GE
		case AT_AttachPartition: /* handled in
								  * process_altertable_start_table but also
								  * here as failsafe */
		case AT_DetachPartition:
#endif
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("operation not supported on hypertables %d", cmd->subtype)));
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

static bool
process_create_trigger_start(ProcessUtilityArgs *args)
{
	CreateTrigStmt *stmt = (CreateTrigStmt *) args->parsetree;
	Cache *hcache;
	Hypertable *ht;
	ObjectAddress PG_USED_FOR_ASSERTS_ONLY address;

	if (!stmt->row)
		return false;

	hcache = ts_hypertable_cache_pin();
	ht = ts_hypertable_cache_get_entry_rv(hcache, stmt->relation);
	if (ht == NULL)
	{
		ts_cache_release(hcache);
		return false;
	}

	process_add_hypertable(args, ht);
	address = ts_hypertable_create_trigger(ht, stmt, args->query_string);
	Assert(OidIsValid(address.objectId));

	ts_cache_release(hcache);
	return true;
}

static void
process_create_rule_start(ProcessUtilityArgs *args)
{
	RuleStmt *stmt = (RuleStmt *) args->parsetree;

	if (ts_hypertable_relid(stmt->relation) == InvalidOid)
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("hypertables do not support rules")));
}

static void
check_supported_pg_version_for_compression()
{
#if PG10_LT
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("compression is not supported with postgres version older than 10")));
#endif
}

/* ALTER TABLE <name> SET ( timescaledb.compress, ...) */
static bool
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
		if (parse_results[CompressEnabled].is_default)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("the option timescaledb.compress must be set to true to enable "
							"compression")));
	}
	else
		return false;

	check_supported_pg_version_for_compression();
	if (pg_options != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only timescaledb.compress parameters allowed when specifying compression "
						"parameters for hypertable")));
	ts_cm_functions->process_compress_table(cmd, ht, parse_results);
	return true;
}

static bool
process_altertable_reset_options(AlterTableCmd *cmd, Hypertable *ht)
{
	List *pg_options = NIL, *compress_options = NIL;
	List *inpdef = NIL;
	/* is this a compress table stmt */
	Assert(IsA(cmd->def, List));
	inpdef = (List *) cmd->def;
	ts_with_clause_filter(inpdef, &compress_options, &pg_options);
	if (compress_options)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("compression options cannot be reset")));
	}
	else
		return false;
}

static bool
process_viewstmt(ProcessUtilityArgs *args)
{
	WithClauseResult *parse_results = NULL;
	bool is_cagg = false;
	Node *parsetree = args->parsetree;
	ViewStmt *stmt = (ViewStmt *) parsetree;
	List *pg_options = NIL, *cagg_options = NIL;
	Assert(IsA(parsetree, ViewStmt));
	/* is this a continuous agg */
	ts_with_clause_filter(stmt->options, &cagg_options, &pg_options);
	if (cagg_options)
	{
		parse_results = ts_continuous_agg_with_clause_parse(cagg_options);
		is_cagg = DatumGetBool(parse_results[ContinuousEnabled].parsed);
	}

	if (!is_cagg)
		return false;

	if (pg_options != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only timescaledb parameters allowed in WITH clause for continuous "
						"aggregate")));

#if PG10_GE
	return ts_cm_functions->process_cagg_viewstmt(stmt,
												  args->query_string,
												  args->pstmt,
												  parse_results);
#else
	return ts_cm_functions->process_cagg_viewstmt(stmt, args->query_string, NULL, parse_results);
#endif
}

static bool
process_refresh_mat_view_start(ProcessUtilityArgs *args, Node *parsetree)
{
	RefreshMatViewStmt *stmt = castNode(RefreshMatViewStmt, parsetree);
	Oid view_relid = RangeVarGetRelid(stmt->relation, NoLock, true);
	int32 materialization_id = -1;
	ScanIterator continuous_aggregate_iter;
	NameData view_name;
	NameData view_schema;
	bool cagg_fullrange;
	ContinuousAggMatOptions mat_options;

	if (!OidIsValid(view_relid))
		return false;

	namestrcpy(&view_name, get_rel_name(view_relid));
	namestrcpy(&view_schema, get_namespace_name(get_rel_namespace(view_relid)));

	continuous_aggregate_iter =
		ts_scan_iterator_create(CONTINUOUS_AGG, AccessShareLock, CurrentMemoryContext);

	ts_scan_iterator_scan_key_init(&continuous_aggregate_iter,
								   Anum_continuous_agg_user_view_name,
								   BTEqualStrategyNumber,
								   F_NAMEEQ,
								   NameGetDatum(&view_name));
	ts_scan_iterator_scan_key_init(&continuous_aggregate_iter,
								   Anum_continuous_agg_user_view_schema,
								   BTEqualStrategyNumber,
								   F_NAMEEQ,
								   NameGetDatum(&view_schema));

	ts_scanner_foreach(&continuous_aggregate_iter)
	{
		HeapTuple tuple = ts_scan_iterator_tuple(&continuous_aggregate_iter);
		Form_continuous_agg form = (Form_continuous_agg) GETSTRUCT(tuple);
		Assert(materialization_id == -1);
		materialization_id = form->mat_hypertable_id;
	}

	if (materialization_id == -1)
		return false;

	PreventInTransactionBlock(args->context == PROCESS_UTILITY_TOPLEVEL, "REFRESH");

	PopActiveSnapshot();
	CommitTransactionCommand();

	mat_options = (ContinuousAggMatOptions){
		.verbose = true,
		.within_single_transaction = false,
		.process_only_invalidation = false,
		.invalidate_prior_to_time = PG_INT64_MAX,
	};
	cagg_fullrange = ts_cm_functions->continuous_agg_materialize(materialization_id, &mat_options);
	if (!cagg_fullrange)
	{
		elog(WARNING,
			 "REFRESH did not materialize the entire range since it was limited by the "
			 "max_interval_per_job setting");
	}

	StartTransactionCommand();
	return true;
}

/*
 * Handle DDL commands before they have been processed by PostgreSQL.
 */
static bool
process_ddl_command_start(ProcessUtilityArgs *args)
{
	bool handled = false;

	switch (nodeTag(args->parsetree))
	{
		case T_AlterObjectSchemaStmt:
			process_alterobjectschema(args);
			break;
		case T_TruncateStmt:
			handled = process_truncate(args);
			break;
		case T_AlterTableStmt:
			handled = process_altertable_start(args);
			break;
		case T_RenameStmt:
			process_rename(args);
			break;
		case T_IndexStmt:
			handled = process_index_start(args);
			break;
		case T_CreateTrigStmt:
			handled = process_create_trigger_start(args);
			break;
		case T_RuleStmt:
			process_create_rule_start(args);
			break;
		case T_DropStmt:

			/*
			 * Drop associated metadata/chunks but also continue on to drop
			 * the main table. Because chunks are deleted before the main
			 * table is dropped, the drop respects CASCADE in the expected
			 * way.
			 */
			process_drop(args);
			break;
		case T_DropTableSpaceStmt:
			process_drop_tablespace(args);
			break;
		case T_GrantStmt:
			handled = process_grant_and_revoke(args);
			break;
		case T_GrantRoleStmt:
			handled = process_grant_and_revoke_role(args);
			break;
		case T_CopyStmt:
			handled = process_copy(args);
			break;
		case T_VacuumStmt:
			handled = process_vacuum(args);
			break;
		case T_ReindexStmt:
			handled = process_reindex(args);
			break;
		case T_ClusterStmt:
			handled = process_cluster_start(args);
			break;
		case T_ViewStmt:
			handled = process_viewstmt(args);
			break;
		case T_RefreshMatViewStmt:
			handled = process_refresh_mat_view_start(args, args->parsetree);
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
	char *hypertable_constraint_name = arg;
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, ht->space->num_dimensions, true);

	/* drop both metadata and table; sql_drop won't be called recursively */
	ts_chunk_constraint_delete_by_hypertable_constraint_name(chunk->fd.id,
															 hypertable_constraint_name,
															 true,
															 true);
}

static void
process_drop_table_constraint(EventTriggerDropObject *obj)
{
	EventTriggerDropTableConstraint *constraint;
	Hypertable *ht;

	Assert(obj->type == EVENT_TRIGGER_DROP_TABLE_CONSTRAINT);
	constraint = (EventTriggerDropTableConstraint *) obj;

	/* do not use relids because underlying table could be gone */
	ht = ts_hypertable_get_by_name(constraint->schema, constraint->table);

	if (ht != NULL)
	{
		CatalogSecurityContext sec_ctx;

		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);

		/* Recurse to each chunk and drop the corresponding constraint */
		foreach_chunk(ht, process_drop_constraint_on_chunk, constraint->constraint_name);

		ts_catalog_restore_user(&sec_ctx);
	}
	else
	{
		Chunk *chunk = chunk_get_by_name(constraint->schema, constraint->table, 0, false);

		if (NULL != chunk)
		{
			ts_chunk_constraint_delete_by_constraint_name(chunk->fd.id,
														  constraint->constraint_name,
														  true,
														  false);
		}
	}
}

static void
process_drop_index(EventTriggerDropObject *obj)
{
	EventTriggerDropIndex *index;

	Assert(obj->type == EVENT_TRIGGER_DROP_INDEX);
	index = (EventTriggerDropIndex *) obj;

	ts_chunk_index_delete_by_name(index->schema, index->index_name, true);
}

static void
process_drop_table(EventTriggerDropObject *obj)
{
	EventTriggerDropTable *table;

	Assert(obj->type == EVENT_TRIGGER_DROP_TABLE);
	table = (EventTriggerDropTable *) obj;

	ts_hypertable_delete_by_name(table->schema, table->table_name);
	ts_chunk_delete_by_name(table->schema, table->table_name, DROP_RESTRICT);
}

static void
process_drop_schema(EventTriggerDropObject *obj)
{
	EventTriggerDropSchema *schema;
	int count;

	Assert(obj->type == EVENT_TRIGGER_DROP_SCHEMA);
	schema = (EventTriggerDropSchema *) obj;

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
	EventTriggerDropTrigger *trigger_event;
	Hypertable *ht;

	Assert(obj->type == EVENT_TRIGGER_DROP_TRIGGER);
	trigger_event = (EventTriggerDropTrigger *) obj;

	/* do not use relids because underlying table could be gone */
	ht = ts_hypertable_get_by_name(trigger_event->schema, trigger_event->table);

	if (ht != NULL)
	{
		/* Recurse to each chunk and drop the corresponding trigger */
		ts_hypertable_drop_trigger(ht, trigger_event->trigger_name);
	}
}

static void
process_drop_view(EventTriggerDropView *dropped_view)
{
	ContinuousAgg *ca;

	ca = ts_continuous_agg_find_by_view_name(dropped_view->schema, dropped_view->view_name);
	if (ca != NULL)
		ts_continuous_agg_drop_view_callback(ca, dropped_view->schema, dropped_view->view_name);
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
		case EVENT_TRIGGER_DROP_VIEW:
			process_drop_view((EventTriggerDropView *) obj);
			break;
	}
}

/*
 * ProcessUtility hook for DDL commands that have not yet been processed by
 * PostgreSQL.
 */
static void
timescaledb_ddl_command_start(
#if PG10_GE
	PlannedStmt *pstmt,
#else
	Node *parsetree,
#endif
	const char *query_string, ProcessUtilityContext context, ParamListInfo params,
#if PG10_GE
	QueryEnvironment *queryEnv,
#endif
	DestReceiver *dest, char *completion_tag)
{
	ProcessUtilityArgs args = {
		.query_string = query_string,
		.context = context,
		.params = params,
		.dest = dest,
		.completion_tag = completion_tag,
#if PG10_GE
		.pstmt = pstmt,
		.parsetree = pstmt->utilityStmt,
		.queryEnv = queryEnv,
		.parse_state = make_parsestate(NULL),
#else
		.parsetree = parsetree,
#endif
		.hypertable_list = NIL
	};

	bool altering_timescaledb = false;
	bool handled;

#if PG10_GE
	args.parse_state->p_sourcetext = query_string;
#endif

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
	handled = process_ddl_command_start(&args);

	/*
	 * We need to run tsl-side ddl_command_start hook before
	 * standard process utility hook to maintain proper invocation
	 * order of sql_drop and ddl_command_end triggers.
	 */
	if (ts_cm_functions->ddl_command_start)
		ts_cm_functions->ddl_command_start(&args);

	if (!handled)
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
