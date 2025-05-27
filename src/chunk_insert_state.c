/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/attnum.h>
#include <access/xact.h>
#include <catalog/pg_type.h>
#include <executor/tuptable.h>
#include <foreign/fdwapi.h>
#include <miscadmin.h>
#include <nodes/execnodes.h>
#include <nodes/makefuncs.h>
#include <nodes/nodes.h>
#include <nodes/plannodes.h>
#include <optimizer/optimizer.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <utils/rls.h>

#include "compat/compat.h"
#include "chunk_index.h"
#include "chunk_insert_state.h"
#include "chunk_tuple_routing.h"
#include "debug_point.h"
#include "errors.h"
#include "indexing.h"
#include "nodes/modify_hypertable.h"
#include "ts_catalog/continuous_agg.h"

/* Just like ExecPrepareExpr except that it doesn't switch to the query memory context */
static inline ExprState *
prepare_constr_expr(Expr *node)
{
	ExprState *result;

	node = expression_planner(node);
	result = ExecInitExpr(node, NULL);

	return result;
}

/*
 * Create the constraint exprs inside the current memory context. If this
 * is not done here, then ExecRelCheck will do it for you but put it into
 * the query memory context, which will cause a memory leak.
 *
 * See the comment in `ts_chunk_insert_state_destroy` for more information
 * on the implications of this.
 */
static inline void
create_chunk_rri_constraint_expr(ResultRelInfo *rri, Relation rel)
{
	int ncheck, i;
	ConstrCheck *check;

	Assert(rel->rd_att->constr != NULL && rri->ri_CheckConstraintExprs == NULL);

	ncheck = rel->rd_att->constr->num_check;
	check = rel->rd_att->constr->check;
	rri->ri_CheckConstraintExprs = (ExprState **) palloc(ncheck * sizeof(ExprState *));

	for (i = 0; i < ncheck; i++)
	{
		Expr *checkconstr = stringToNode(check[i].ccbin);

		rri->ri_CheckConstraintExprs[i] = prepare_constr_expr(checkconstr);
	}
}

/*
 * Create a new ResultRelInfo for a chunk.
 *
 * The ResultRelInfo holds the executor state (e.g., open relation, indexes, and
 * options) for the result relation where tuples will be stored.
 *
 * The Hypertable ResultRelInfo is used as a template for the chunk's new ResultRelInfo.
 */
ResultRelInfo *
create_chunk_result_relation_info(ResultRelInfo *ht_rri, Relation rel, EState *estate)
{
	ResultRelInfo *rri;
	rri = makeNode(ResultRelInfo);

	InitResultRelInfo(rri, rel, ht_rri->ri_RangeTableIndex, NULL, estate->es_instrument);

	/* Copy options from the main table's (hypertable's) result relation info */
	rri->ri_WithCheckOptions = ht_rri->ri_WithCheckOptions;
	rri->ri_WithCheckOptionExprs = ht_rri->ri_WithCheckOptionExprs;
	rri->ri_projectReturning = ht_rri->ri_projectReturning;

	rri->ri_FdwState = NULL;
	rri->ri_usesFdwDirectModify = ht_rri->ri_usesFdwDirectModify;

	if (RelationGetForm(rel)->relkind == RELKIND_FOREIGN_TABLE)
		rri->ri_FdwRoutine = GetFdwRoutineForRelation(rel, true);

	create_chunk_rri_constraint_expr(rri, rel);

	return rri;
}

static ProjectionInfo *
get_adjusted_projection_info_returning(ProjectionInfo *orig, List *returning_clauses,
									   TupleConversionMap *map, Index varno, Oid rowtype,
									   TupleDesc chunk_desc)
{
	bool found_whole_row;

	Assert(returning_clauses != NIL);

	/* map hypertable attnos -> chunk attnos */
	if (map != NULL)
		returning_clauses = castNode(List,
									 map_variable_attnos((Node *) returning_clauses,
														 varno,
														 0,
														 map->attrMap,
														 rowtype,
														 &found_whole_row));

	return ExecBuildProjectionInfo(returning_clauses,
								   orig->pi_exprContext,
								   orig->pi_state.resultslot,
								   orig->pi_state.parent,
								   chunk_desc);
}

static List *
translate_clause(List *inclause, TupleConversionMap *chunk_map, Index varno, Relation hyper_rel,
				 Relation chunk_rel)
{
	List *clause = copyObject(inclause);
	bool found_whole_row;

	/* nothing to do here if the chunk_map is NULL */
	if (!chunk_map)
		return list_copy(clause);

	/* map hypertable attnos -> chunk attnos for the "excluded" table */
	clause = castNode(List,
					  map_variable_attnos((Node *) clause,
										  INNER_VAR,
										  0,
										  chunk_map->attrMap,
										  RelationGetForm(chunk_rel)->reltype,
										  &found_whole_row));

	/* map hypertable attnos -> chunk attnos for the hypertable */
	clause = castNode(List,
					  map_variable_attnos((Node *) clause,
										  varno,
										  0,
										  chunk_map->attrMap,
										  RelationGetForm(chunk_rel)->reltype,
										  &found_whole_row));

	return clause;
}

/*
 * adjust_chunk_colnos
 *		Adjust the list of UPDATE target column numbers to account for
 *		attribute differences between the parent and the partition.
 *
 * adapted from postgres adjust_partition_colnos
 */
static List *
adjust_chunk_colnos(List *colnos, ResultRelInfo *chunk_rri)
{
	List *new_colnos = NIL;
	TupleConversionMap *map = ExecGetChildToRootMap(chunk_rri);
	AttrMap *attrMap;
	ListCell *lc;

	Assert(map != NULL); /* else we shouldn't be here */
	attrMap = map->attrMap;

	foreach (lc, colnos)
	{
		AttrNumber parentattrno = lfirst_int(lc);

		if (parentattrno <= 0 || parentattrno > attrMap->maplen ||
			attrMap->attnums[parentattrno - 1] == 0)
			elog(ERROR, "unexpected attno %d in target column list", parentattrno);
		new_colnos = lappend_int(new_colnos, attrMap->attnums[parentattrno - 1]);
	}

	return new_colnos;
}

/*
 * Setup ON CONFLICT state for a chunk.
 *
 * Mostly, this is about mapping attribute numbers from the hypertable root to
 * a chunk, accounting for differences in the tuple descriptors due to dropped
 * columns, etc.
 */
static void
setup_on_conflict_state(ResultRelInfo *ht_rri, ModifyTableState *mtstate, ChunkInsertState *state,
						TupleConversionMap *chunk_map)
{
	TupleConversionMap *map = state->hyper_to_chunk_map;
	ResultRelInfo *chunk_rri = state->result_relation_info;
	Relation chunk_rel = state->result_relation_info->ri_RelationDesc;
	Relation hyper_rel = ht_rri->ri_RelationDesc;
	ModifyTable *mt = castNode(ModifyTable, mtstate->ps.plan);

	OnConflictSetState *onconfl = makeNode(OnConflictSetState);
	memcpy(onconfl, ht_rri->ri_onConflict, sizeof(OnConflictSetState));
	chunk_rri->ri_onConflict = onconfl;

#if PG16_LT
	chunk_rri->ri_RootToPartitionMap = map;
#else
	chunk_rri->ri_RootToChildMap = map;
	chunk_rri->ri_RootToChildMapValid = true;
#endif

	Assert(mt->onConflictSet);
	Assert(ht_rri->ri_onConflict != NULL);

	/*
	 * Need a separate existing slot for each partition, as the
	 * partition could be of a different AM, even if the tuple
	 * descriptors match.
	 */
	onconfl->oc_Existing = table_slot_create(chunk_rri->ri_RelationDesc, NULL);
	state->existing_slot = onconfl->oc_Existing;

	/*
	 * If the chunk's tuple descriptor matches exactly the hypertable
	 * (the common case), we can reuse most of the parent's ON
	 * CONFLICT SET state, skipping a bunch of work.  Otherwise, we
	 * need to create state specific to this partition.
	 */
	if (!map)
	{
		/*
		 * It's safe to reuse these from the hypertable, as we
		 * only process one tuple at a time (therefore we won't
		 * overwrite needed data in slots), and the results of
		 * projections are independent of the underlying storage.
		 * Projections and where clauses themselves don't store state
		 * / are independent of the underlying storage.
		 */
		onconfl->oc_ProjSlot = ht_rri->ri_onConflict->oc_ProjSlot;
		onconfl->oc_ProjInfo = ht_rri->ri_onConflict->oc_ProjInfo;
		onconfl->oc_WhereClause = ht_rri->ri_onConflict->oc_WhereClause;
		state->conflproj_slot = onconfl->oc_ProjSlot;
	}
	else
	{
		List *onconflset;
		List *onconflcols;

		/*
		 * Translate expressions in onConflictSet to account for
		 * different attribute numbers.  For that, map partition
		 * varattnos twice: first to catch the EXCLUDED
		 * pseudo-relation (INNER_VAR), and second to handle the main
		 * target relation (firstVarno).
		 */
		onconflset = copyObject(mt->onConflictSet);

		Assert(map->outdesc == RelationGetDescr(chunk_rel));

		if (!chunk_map)
			chunk_map =
				convert_tuples_by_name(RelationGetDescr(chunk_rel), RelationGetDescr(hyper_rel));

		onconflset = translate_clause(onconflset,
									  chunk_map,
									  ht_rri->ri_RangeTableIndex,
									  hyper_rel,
									  chunk_rel);

		chunk_rri->ri_ChildToRootMap = chunk_map;
		chunk_rri->ri_ChildToRootMapValid = true;

		/* Finally, adjust the target colnos to match the chunk. */
		if (chunk_map)
			onconflcols = adjust_chunk_colnos(mt->onConflictCols, chunk_rri);
		else
			onconflcols = mt->onConflictCols;

		/* create the tuple slot for the UPDATE SET projection */
		onconfl->oc_ProjSlot = table_slot_create(chunk_rel, NULL);
		state->conflproj_slot = onconfl->oc_ProjSlot;

		/* build UPDATE SET projection state */
		onconfl->oc_ProjInfo = ExecBuildUpdateProjection(onconflset,
														 true,
														 onconflcols,
														 RelationGetDescr(chunk_rel),
														 mtstate->ps.ps_ExprContext,
														 onconfl->oc_ProjSlot,
														 &mtstate->ps);

		Node *onconflict_where = mt->onConflictWhere;

		/*
		 * Map attribute numbers in the WHERE clause, if it exists.
		 */
		if (onconflict_where && chunk_map)
		{
			List *clause = translate_clause(castNode(List, onconflict_where),
											chunk_map,
											ht_rri->ri_RangeTableIndex,
											hyper_rel,
											chunk_rel);

			chunk_rri->ri_onConflict->oc_WhereClause = ExecInitQual(clause, NULL);
		}
	}
}

static void
destroy_on_conflict_state(ChunkInsertState *state)
{
	/*
	 * Clean up per-chunk tuple table slots created for ON CONFLICT handling.
	 */
	if (NULL != state->existing_slot)
		ExecDropSingleTupleTableSlot(state->existing_slot);

	/* The ON CONFLICT projection slot is only chunk specific in case the
	 * tuple descriptor didn't match the hypertable */
	if (NULL != state->hyper_to_chunk_map && NULL != state->conflproj_slot)
		ExecDropSingleTupleTableSlot(state->conflproj_slot);
}

/* Translate hypertable indexes to chunk indexes in the arbiter clause */
static void
set_arbiter_indexes(ChunkInsertState *state, List *ht_arbiter_indexes)
{
	List *chunk_arbiter_indexes = NIL;
	ListCell *lc;

	foreach (lc, ht_arbiter_indexes)
	{
		Oid hypertable_index = lfirst_oid(lc);
		Oid chunk_index_oid =
			ts_chunk_index_get_by_hypertable_indexrelid(state->rel, hypertable_index);
		if (!OidIsValid(chunk_index_oid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("could not find arbiter index for hypertable index \"%s\" on chunk "
							"\"%s\"",
							get_rel_name(hypertable_index),
							get_rel_name(RelationGetRelid(state->rel)))));
		}

		chunk_arbiter_indexes = lappend_oid(chunk_arbiter_indexes, chunk_index_oid);
	}
	state->result_relation_info->ri_onConflictArbiterIndexes = chunk_arbiter_indexes;
}

/* Change the projections to work with chunks instead of hypertables */
static void
adjust_projections(ResultRelInfo *ht_rri, ModifyTableState *mtstate, ChunkInsertState *cis,
				   Oid rowtype)
{
	ResultRelInfo *chunk_rri = cis->result_relation_info;
	Relation hyper_rel = ht_rri->ri_RelationDesc;
	Relation chunk_rel = cis->rel;
	TupleConversionMap *chunk_map = NULL;
	OnConflictAction onConflictAction = ONCONFLICT_NONE;
	List *returningLists = NIL;

	if (mtstate)
	{
		ModifyTable *mt = castNode(ModifyTable, mtstate->ps.plan);
		onConflictAction = mt->onConflictAction;
		returningLists = mt->returningLists;
	}

	if (returningLists)
	{
		/*
		 * We need the opposite map from cis->hyper_to_chunk_map. The map needs
		 * to have the hypertable_desc in the out spot for map_variable_attnos
		 * to work correctly in mapping hypertable attnos->chunk attnos.
		 */
		chunk_map =
			convert_tuples_by_name(RelationGetDescr(chunk_rel), RelationGetDescr(hyper_rel));

		chunk_rri->ri_projectReturning =
			get_adjusted_projection_info_returning(chunk_rri->ri_projectReturning,
												   linitial(returningLists),
												   chunk_map,
												   ht_rri->ri_RangeTableIndex,
												   rowtype,
												   RelationGetDescr(chunk_rel));
	}

	/* Set the chunk's arbiter indexes for ON CONFLICT statements */
	if (onConflictAction != ONCONFLICT_NONE)
	{
		set_arbiter_indexes(cis, ht_rri->ri_onConflictArbiterIndexes);

		if (onConflictAction == ONCONFLICT_UPDATE)
			setup_on_conflict_state(ht_rri, mtstate, cis, chunk_map);
	}
}

/*
 * Create new insert chunk state.
 *
 * This is essentially a ResultRelInfo for a chunk. Initialization of the
 * ResultRelInfo should be similar to ExecInitModifyTable().
 */
extern ChunkInsertState *
ts_chunk_insert_state_create(Oid chunk_relid, const ChunkTupleRouting *ctr)
{
	ChunkInsertState *state;
	Relation rel, parent_rel;
	MemoryContext cis_context = AllocSetContextCreate(ctr->estate->es_query_cxt,
													  "chunk insert state memory context",
													  ALLOCSET_DEFAULT_SIZES);
	ResultRelInfo *relinfo;
	const Chunk *chunk;

	/* permissions NOT checked here; were checked at hypertable level */
	if (check_enable_rls(chunk_relid, InvalidOid, false) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support row-level security")));

	MemoryContext old_mcxt =
		MemoryContextSwitchTo(ctr->estate->es_per_tuple_exprcontext->ecxt_per_tuple_memory);
	/*
	 * Since we insert data and won't modify metadata, a RowExclusiveLock
	 * should be sufficient. This should conflict with any metadata-modifying
	 * operations as they should take higher-level locks (ShareLock and
	 * above).
	 */
	rel = table_open(chunk_relid, RowExclusiveLock);

	/*
	 * A concurrent chunk operation (e.g., compression) might have changed the
	 * chunk metadata before we got a lock, so re-read it.
	 *
	 * This works even in higher levels of isolation since catalog data is
	 * always read from latest snapshot.
	 */
	chunk = ts_chunk_get_by_relid(chunk_relid, true);
	Assert(chunk->relkind == RELKIND_RELATION);
	ts_chunk_validate_chunk_status_for_operation(chunk, CHUNK_INSERT, true);

	MemoryContextSwitchTo(cis_context);
	relinfo = create_chunk_result_relation_info(ctr->hypertable_rri, rel, ctr->estate);
	if (ctr->mht_state)
		CheckValidResultRelCompat(relinfo, ctr->mht_state->mt->operation, NIL);

	state = palloc0(sizeof(ChunkInsertState));
	state->counters = ctr->counters;
	if (ctr->mht_state)
		state->onConflictAction = ctr->mht_state->mt->onConflictAction;
	state->mctx = cis_context;
	state->rel = rel;
	state->result_relation_info = relinfo;
	state->estate = ctr->estate;
	ts_set_compression_status(state, chunk);

	if (relinfo->ri_RelationDesc->rd_rel->relhasindex && relinfo->ri_IndexRelationDescs == NULL)
		ExecOpenIndices(relinfo, state->onConflictAction != ONCONFLICT_NONE);

	if (relinfo->ri_TrigDesc != NULL)
	{
		TriggerDesc *tg = relinfo->ri_TrigDesc;

		/* instead of triggers can only be created on VIEWs */
		Assert(!tg->trig_insert_instead_row);

		/*
		 * A statement that targets a parent table in an inheritance or
		 * partitioning hierarchy does not cause the statement-level triggers
		 * of affected child tables to be fired; only the parent table's
		 * statement-level triggers are fired. However, row-level triggers
		 * of any affected child tables will be fired.
		 * During chunk creation we only copy ROW trigger to chunks so
		 * statement triggers should not exist on chunks.
		 */
		if (tg->trig_insert_after_statement || tg->trig_insert_before_statement)
			elog(ERROR, "statement trigger on chunk table not supported");
	}

	parent_rel = table_open(ctr->hypertable->main_table_relid, AccessShareLock);

	/* Set tuple conversion map, if tuple needs conversion. We don't want to
	 * convert tuples going into foreign tables since these are actually sent to
	 * data nodes for insert on that node's local hypertable. */
	if (chunk->relkind != RELKIND_FOREIGN_TABLE)
		state->hyper_to_chunk_map =
			convert_tuples_by_name(RelationGetDescr(parent_rel), RelationGetDescr(rel));

	if (ctr->mht_state)
		adjust_projections(ctr->hypertable_rri,
						   linitial_node(ModifyTableState, ctr->mht_state->cscan_state.custom_ps),
						   state,
						   RelationGetForm(rel)->reltype);

	/* Need a tuple table slot to store tuples going into this chunk. We don't
	 * want this slot tied to the executor's tuple table, since that would tie
	 * the slot's lifetime to the entire length of the execution and we want
	 * to be able to dynamically create and destroy chunk insert
	 * state. Otherwise, memory might blow up when there are many chunks being
	 * inserted into. This also means that the slot needs to be destroyed with
	 * the chunk insert state. */
	state->slot = MakeSingleTupleTableSlot(RelationGetDescr(relinfo->ri_RelationDesc),
										   table_slot_callbacks(relinfo->ri_RelationDesc));
	table_close(parent_rel, AccessShareLock);

	state->hypertable_relid = chunk->hypertable_relid;
	state->chunk_id = chunk->fd.id;

	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
#if PG16_LT
		RangeTblEntry *rte = rt_fetch(relinfo->ri_RangeTableIndex, ctr->estate->es_range_table);

		Assert(rte != NULL);

		state->user_id = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();
#else
		state->user_id = ExecGetResultRelCheckAsUser(relinfo, ctr->estate);
#endif
	}

	MemoryContextSwitchTo(old_mcxt);

	return state;
}

void
ts_set_compression_status(ChunkInsertState *state, const Chunk *chunk)
{
	state->chunk_compressed = ts_chunk_is_compressed(chunk);
	if (state->chunk_compressed)
	{
		state->chunk_partial = ts_chunk_is_partial(chunk);
	}
}

extern void
ts_chunk_insert_state_destroy(ChunkInsertState *state)
{
	ResultRelInfo *rri = state->result_relation_info;

	/*
	 * Check if we need to mark the chunk as partial.
	 * We need to change chunk status to partial in the following cases:
	 * - rowstore insert into compressed chunk
	 * - columnstore insert into uncompressed chunk that is not a new chunk (flagged as
	 * needs_partial in chunk_tuple_routing.c)
	 */
	if (state->chunk_compressed && !state->chunk_partial &&
		(!state->columnstore_insert || state->needs_partial))
	{
		Oid chunk_relid = RelationGetRelid(state->result_relation_info->ri_RelationDesc);
		Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
		ts_chunk_set_partial(chunk);
	}

	if (rri->ri_FdwRoutine && !rri->ri_usesFdwDirectModify && rri->ri_FdwRoutine->EndForeignModify)
		rri->ri_FdwRoutine->EndForeignModify(state->estate, rri);

	destroy_on_conflict_state(state);
	ExecCloseIndices(state->result_relation_info);

	table_close(state->rel, NoLock);
	if (state->slot)
		ExecDropSingleTupleTableSlot(state->slot);

	MemoryContextDelete(state->mctx);
}
