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
#include "errors.h"
#include "chunk_dispatch.h"
#include "chunk_insert_state.h"
#include "ts_catalog/chunk_data_node.h"
#include "ts_catalog/continuous_agg.h"
#include "chunk_index.h"
#include "indexing.h"
#include <utils/inval.h>

/* Just like ExecPrepareExpr except that it doesn't switch to the query memory context */
static inline ExprState *
prepare_constr_expr(Expr *node)
{
	ExprState *result;

	node = expression_planner(node);
	result = ExecInitExpr(node, NULL);

	return result;
}

static inline ModifyTableState *
get_modifytable_state(const ChunkDispatch *dispatch)
{
	return dispatch->dispatch_state->mtstate;
}

static inline ModifyTable *
get_modifytable(const ChunkDispatch *dispatch)
{
	return castNode(ModifyTable, get_modifytable_state(dispatch)->ps.plan);
}

static List *
chunk_dispatch_get_arbiter_indexes(const ChunkDispatch *dispatch)
{
	return dispatch->dispatch_state->arbiter_indexes;
}

static bool
chunk_dispatch_has_returning(const ChunkDispatch *dispatch)
{
	if (!dispatch->dispatch_state)
		return false;
	return get_modifytable(dispatch)->returningLists != NIL;
}

static List *
chunk_dispatch_get_returning_clauses(const ChunkDispatch *dispatch)
{
#if PG14_LT
	ModifyTableState *mtstate = get_modifytable_state(dispatch);
	return list_nth(get_modifytable(dispatch)->returningLists, mtstate->mt_whichplan);
#else
	Assert(list_length(get_modifytable(dispatch)->returningLists) == 1);
	return linitial(get_modifytable(dispatch)->returningLists);
#endif
}

OnConflictAction
chunk_dispatch_get_on_conflict_action(const ChunkDispatch *dispatch)
{
	if (!dispatch->dispatch_state)
		return ONCONFLICT_NONE;
	return get_modifytable(dispatch)->onConflictAction;
}

static CmdType
chunk_dispatch_get_cmd_type(const ChunkDispatch *dispatch)
{
	return dispatch->dispatch_state == NULL ? CMD_INSERT :
											  dispatch->dispatch_state->mtstate->operation;
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

	Assert(rel->rd_att->constr != NULL && rri->ri_ConstraintExprs == NULL);

	ncheck = rel->rd_att->constr->num_check;
	check = rel->rd_att->constr->check;
	rri->ri_ConstraintExprs = (ExprState **) palloc(ncheck * sizeof(ExprState *));

	for (i = 0; i < ncheck; i++)
	{
		Expr *checkconstr = stringToNode(check[i].ccbin);

		rri->ri_ConstraintExprs[i] = prepare_constr_expr(checkconstr);
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
static inline ResultRelInfo *
create_chunk_result_relation_info(ChunkDispatch *dispatch, Relation rel)
{
	ResultRelInfo *rri;
	ResultRelInfo *rri_orig = dispatch->hypertable_result_rel_info;
	Index hyper_rti = rri_orig->ri_RangeTableIndex;
	rri = makeNode(ResultRelInfo);

	InitResultRelInfo(rri, rel, hyper_rti, NULL, dispatch->estate->es_instrument);

	/* Copy options from the main table's (hypertable's) result relation info */
	rri->ri_WithCheckOptions = rri_orig->ri_WithCheckOptions;
	rri->ri_WithCheckOptionExprs = rri_orig->ri_WithCheckOptionExprs;
#if PG14_LT
	rri->ri_junkFilter = rri_orig->ri_junkFilter;
#endif
	rri->ri_projectReturning = rri_orig->ri_projectReturning;

	rri->ri_FdwState = NULL;
	rri->ri_usesFdwDirectModify = rri_orig->ri_usesFdwDirectModify;

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

#if PG14_LT
/*
 * adjust_hypertable_tlist - from Postgres source code `adjust_partition_tlist`
 *		Adjust the targetlist entries for a given chunk to account for
 *		attribute differences between hypertable and the chunk
 *
 * The expressions have already been fixed, but here we fix the list to make
 * target resnos match the chunk's attribute numbers.  This results in a
 * copy of the original target list in which the entries appear in resno
 * order, including both the existing entries (that may have their resno
 * changed in-place) and the newly added entries for columns that don't exist
 * in the parent.
 *
 * Scribbles on the input tlist's entries resno so be aware.
 */
static List *
adjust_hypertable_tlist(List *tlist, TupleConversionMap *map)
{
	List *new_tlist = NIL;
	TupleDesc chunk_tupdesc = map->outdesc;
	AttrNumber *attrMap = map->attrMap->attnums;
	AttrNumber chunk_attrno;

	for (chunk_attrno = 1; chunk_attrno <= chunk_tupdesc->natts; chunk_attrno++)
	{
		Form_pg_attribute att_tup = TupleDescAttr(chunk_tupdesc, chunk_attrno - 1);
		TargetEntry *tle;

		if (attrMap[chunk_attrno - 1] != InvalidAttrNumber)
		{
			Assert(!att_tup->attisdropped);

			/*
			 * Use the corresponding entry from the parent's tlist, adjusting
			 * the resno the match the partition's attno.
			 */
			tle = (TargetEntry *) list_nth(tlist, attrMap[chunk_attrno - 1] - 1);
			if (namestrcmp(&att_tup->attname, tle->resname) != 0)
				elog(ERROR, "invalid translation of ON CONFLICT update statements");
			tle->resno = chunk_attrno;
		}
		else
		{
			Const *expr;

			/*
			 * For a dropped attribute in the partition, generate a dummy
			 * entry with resno matching the partition's attno.
			 */
			Assert(att_tup->attisdropped);
			expr = makeConst(INT4OID,
							 -1,
							 InvalidOid,
							 sizeof(int32),
							 (Datum) 0,
							 true, /* isnull */
							 true /* byval */);
			tle = makeTargetEntry((Expr *) expr,
								  chunk_attrno,
								  pstrdup(NameStr(att_tup->attname)),
								  false);
		}
		new_tlist = lappend(new_tlist, tle);
	}
	return new_tlist;
}
#endif

#if PG14_GE
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
#endif

/*
 * Setup ON CONFLICT state for a chunk.
 *
 * Mostly, this is about mapping attribute numbers from the hypertable root to
 * a chunk, accounting for differences in the tuple descriptors due to dropped
 * columns, etc.
 */
static void
setup_on_conflict_state(ChunkInsertState *state, ChunkDispatch *dispatch,
						TupleConversionMap *chunk_map)
{
	TupleConversionMap *map = state->hyper_to_chunk_map;
	ResultRelInfo *chunk_rri = state->result_relation_info;
	ResultRelInfo *hyper_rri = dispatch->hypertable_result_rel_info;
	Relation chunk_rel = state->result_relation_info->ri_RelationDesc;
	Relation hyper_rel = hyper_rri->ri_RelationDesc;
	ModifyTableState *mtstate = castNode(ModifyTableState, dispatch->dispatch_state->mtstate);
	ModifyTable *mt = castNode(ModifyTable, mtstate->ps.plan);

	Assert(chunk_dispatch_get_on_conflict_action(dispatch) == ONCONFLICT_UPDATE);

	OnConflictSetState *onconfl = makeNode(OnConflictSetState);
	memcpy(onconfl, hyper_rri->ri_onConflict, sizeof(OnConflictSetState));
	chunk_rri->ri_onConflict = onconfl;

#if PG14_GE && PG16_LT
	chunk_rri->ri_RootToPartitionMap = map;
#elif PG16_GE
	chunk_rri->ri_RootToChildMap = map;
	chunk_rri->ri_RootToChildMapValid = true;
#endif

	Assert(mt->onConflictSet);
	Assert(hyper_rri->ri_onConflict != NULL);

	/*
	 * Need a separate existing slot for each partition, as the
	 * partition could be of a different AM, even if the tuple
	 * descriptors match.
	 */
	onconfl->oc_Existing = table_slot_create(chunk_rri->ri_RelationDesc, NULL);
	state->existing_slot = onconfl->oc_Existing;

	/*
	 * If the chunk's tuple descriptor matches exactly the hypertable
	 * (the common case), we can re-use most of the parent's ON
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
		onconfl->oc_ProjSlot = hyper_rri->ri_onConflict->oc_ProjSlot;
		onconfl->oc_ProjInfo = hyper_rri->ri_onConflict->oc_ProjInfo;
		onconfl->oc_WhereClause = hyper_rri->ri_onConflict->oc_WhereClause;
		state->conflproj_slot = onconfl->oc_ProjSlot;
	}
	else
	{
		List *onconflset;
#if PG14_GE
		List *onconflcols;
#endif

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
									  hyper_rri->ri_RangeTableIndex,
									  hyper_rel,
									  chunk_rel);

#if PG14_LT
		onconflset = adjust_hypertable_tlist(onconflset, state->hyper_to_chunk_map);
#else
		chunk_rri->ri_ChildToRootMap = chunk_map;
		chunk_rri->ri_ChildToRootMapValid = true;

		/* Finally, adjust the target colnos to match the chunk. */
		if (chunk_map)
			onconflcols = adjust_chunk_colnos(mt->onConflictCols, chunk_rri);
		else
			onconflcols = mt->onConflictCols;
#endif

		/* create the tuple slot for the UPDATE SET projection */
		onconfl->oc_ProjSlot = table_slot_create(chunk_rel, NULL);
		state->conflproj_slot = onconfl->oc_ProjSlot;

		/* build UPDATE SET projection state */
#if PG14_LT
		ExprContext *econtext = hyper_rri->ri_onConflict->oc_ProjInfo->pi_exprContext;
		onconfl->oc_ProjInfo = ExecBuildProjectionInfo(onconflset,
													   econtext,
													   state->conflproj_slot,
													   NULL,
													   RelationGetDescr(chunk_rel));
#else
		onconfl->oc_ProjInfo = ExecBuildUpdateProjection(onconflset,
														 true,
														 onconflcols,
														 RelationGetDescr(chunk_rel),
														 mtstate->ps.ps_ExprContext,
														 onconfl->oc_ProjSlot,
														 &mtstate->ps);
#endif

		Node *onconflict_where = mt->onConflictWhere;

		/*
		 * Map attribute numbers in the WHERE clause, if it exists.
		 */
		if (onconflict_where && chunk_map)
		{
			List *clause = translate_clause(castNode(List, onconflict_where),
											chunk_map,
											hyper_rri->ri_RangeTableIndex,
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
set_arbiter_indexes(ChunkInsertState *state, ChunkDispatch *dispatch)
{
	List *arbiter_indexes = chunk_dispatch_get_arbiter_indexes(dispatch);
	ListCell *lc;

	state->arbiter_indexes = NIL;

	foreach (lc, arbiter_indexes)
	{
		Oid hypertable_index = lfirst_oid(lc);
		Chunk *chunk = ts_chunk_get_by_relid(RelationGetRelid(state->rel), true);
		ChunkIndexMapping cim;

		if (ts_chunk_index_get_by_hypertable_indexrelid(chunk, hypertable_index, &cim) < 1)
		{
			/*
			 * In case of distributed hypertables, we don't have information about the
			 * arbiter index on the remote side, so error out with a helpful hint
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("could not find arbiter index for hypertable index \"%s\" on chunk "
							"\"%s\"",
							get_rel_name(hypertable_index),
							get_rel_name(RelationGetRelid(state->rel))),
					 hypertable_is_distributed(dispatch->hypertable) ?
						 errhint(
							 "Omit the index inference specification for the distributed hypertable"
							 " in the ON CONFLICT clause.") :
						 0));
		}

		state->arbiter_indexes = lappend_oid(state->arbiter_indexes, cim.indexoid);
	}
	state->result_relation_info->ri_onConflictArbiterIndexes = state->arbiter_indexes;
}

/* Change the projections to work with chunks instead of hypertables */
static void
adjust_projections(ChunkInsertState *cis, ChunkDispatch *dispatch, Oid rowtype)
{
	ResultRelInfo *chunk_rri = cis->result_relation_info;
	Relation hyper_rel = dispatch->hypertable_result_rel_info->ri_RelationDesc;
	Relation chunk_rel = cis->rel;
	TupleConversionMap *chunk_map = NULL;
	OnConflictAction onconflict_action = chunk_dispatch_get_on_conflict_action(dispatch);

	if (chunk_dispatch_has_returning(dispatch))
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
												   chunk_dispatch_get_returning_clauses(dispatch),
												   chunk_map,
												   dispatch->hypertable_result_rel_info
													   ->ri_RangeTableIndex,
												   rowtype,
												   RelationGetDescr(chunk_rel));
	}

	/* Set the chunk's arbiter indexes for ON CONFLICT statements */
	if (onconflict_action != ONCONFLICT_NONE)
	{
		set_arbiter_indexes(cis, dispatch);

		if (onconflict_action == ONCONFLICT_UPDATE)
			setup_on_conflict_state(cis, dispatch, chunk_map);
	}
}

/*
 * Create new insert chunk state.
 *
 * This is essentially a ResultRelInfo for a chunk. Initialization of the
 * ResultRelInfo should be similar to ExecInitModifyTable().
 */
extern ChunkInsertState *
ts_chunk_insert_state_create(const Chunk *chunk, ChunkDispatch *dispatch)
{
	ChunkInsertState *state;
	Relation rel, parent_rel;
	MemoryContext cis_context = AllocSetContextCreate(dispatch->estate->es_query_cxt,
													  "chunk insert state memory context",
													  ALLOCSET_DEFAULT_SIZES);
	OnConflictAction onconflict_action = chunk_dispatch_get_on_conflict_action(dispatch);
	ResultRelInfo *relinfo;

	/* permissions NOT checked here; were checked at hypertable level */
	if (check_enable_rls(chunk->table_id, InvalidOid, false) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support row-level security")));
	Assert(chunk->relkind == RELKIND_RELATION || chunk->relkind == RELKIND_FOREIGN_TABLE);

	ts_chunk_validate_chunk_status_for_operation(chunk, CHUNK_INSERT, true);

	rel = table_open(chunk->table_id, RowExclusiveLock);

	MemoryContext old_mcxt = MemoryContextSwitchTo(cis_context);
	relinfo = create_chunk_result_relation_info(dispatch, rel);
	CheckValidResultRel(relinfo, chunk_dispatch_get_cmd_type(dispatch));

	state = palloc0(sizeof(ChunkInsertState));
	state->mctx = cis_context;
	state->rel = rel;
	state->result_relation_info = relinfo;
	state->estate = dispatch->estate;
	ts_set_compression_status(state, chunk);

	if (relinfo->ri_RelationDesc->rd_rel->relhasindex && relinfo->ri_IndexRelationDescs == NULL)
		ExecOpenIndices(relinfo, onconflict_action != ONCONFLICT_NONE);

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

	parent_rel = table_open(dispatch->hypertable->main_table_relid, AccessShareLock);

	/* Set tuple conversion map, if tuple needs conversion. We don't want to
	 * convert tuples going into foreign tables since these are actually sent to
	 * data nodes for insert on that node's local hypertable. */
	if (chunk->relkind != RELKIND_FOREIGN_TABLE)
		state->hyper_to_chunk_map =
			convert_tuples_by_name(RelationGetDescr(parent_rel), RelationGetDescr(rel));

	adjust_projections(state, dispatch, RelationGetForm(rel)->reltype);

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

	state->chunk_id = chunk->fd.id;

	if (chunk->relkind == RELKIND_FOREIGN_TABLE)
	{
#if PG16_LT
		RangeTblEntry *rte =
			rt_fetch(relinfo->ri_RangeTableIndex, dispatch->estate->es_range_table);

		Assert(rte != NULL);

		state->user_id = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();
#else
		state->user_id = ExecGetResultRelCheckAsUser(relinfo, state->estate);
#endif
		state->chunk_data_nodes = ts_chunk_data_nodes_copy(chunk);
	}

	if (dispatch->hypertable_result_rel_info->ri_usesFdwDirectModify)
	{
		/* If the hypertable is setup for direct modify, we do not really use
		 * the FDW. Instead exploit the FdwPrivate pointer to pass on the
		 * chunk insert state to DataNodeDispatch so that it knows which data nodes
		 * to insert into. */
		relinfo->ri_FdwState = state;
	}
	else if (relinfo->ri_FdwRoutine && !relinfo->ri_usesFdwDirectModify &&
			 relinfo->ri_FdwRoutine->BeginForeignModify)
	{
		/*
		 * If this is a chunk located one or more data nodes, setup the
		 * foreign data wrapper state for the chunk. The private fdw data was
		 * created at the planning stage and contains, among other things, a
		 * deparsed insert statement for the hypertable.
		 */
		ModifyTableState *mtstate = dispatch->dispatch_state->mtstate;
		ModifyTable *mt = castNode(ModifyTable, mtstate->ps.plan);
		List *fdwprivate = linitial_node(List, mt->fdwPrivLists);

		Assert(NIL != fdwprivate);
		/*
		 * Since the fdwprivate data is part of the plan it must only
		 * consist of Node objects that can be copied. Therefore, we
		 * cannot directly append the non-Node ChunkInsertState to the
		 * private data. Instead, we make a copy of the private data
		 * before passing it on to the FDW handler function. In the
		 * FDW, the ChunkInsertState will be at the offset defined by
		 * the FdwModifyPrivateChunkInsertState (see
		 * tsl/src/fdw/timescaledb_fdw.c).
		 */
		fdwprivate = lappend(list_copy(fdwprivate), state);
		relinfo->ri_FdwRoutine->BeginForeignModify(mtstate,
												   relinfo,
												   fdwprivate,
												   0,
												   dispatch->eflags);
	}

	MemoryContextSwitchTo(old_mcxt);

	return state;
}

void
ts_set_compression_status(ChunkInsertState *state, const Chunk *chunk)
{
	state->chunk_compressed = ts_chunk_is_compressed(chunk);
	if (state->chunk_compressed)
		state->chunk_partial = ts_chunk_is_partial(chunk);
}

extern void
ts_chunk_insert_state_destroy(ChunkInsertState *state)
{
	ResultRelInfo *rri = state->result_relation_info;

	if (state->chunk_compressed && !state->chunk_partial)
	{
		Oid chunk_relid = RelationGetRelid(state->result_relation_info->ri_RelationDesc);
		Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
		ts_chunk_set_partial(chunk);
		/* changed chunk status, so invalidate any plans involving this chunk */
		CacheInvalidateRelcacheByRelid(chunk_relid);
	}

	if (rri->ri_FdwRoutine && !rri->ri_usesFdwDirectModify && rri->ri_FdwRoutine->EndForeignModify)
		rri->ri_FdwRoutine->EndForeignModify(state->estate, rri);

	destroy_on_conflict_state(state);
	ExecCloseIndices(state->result_relation_info);

	table_close(state->rel, NoLock);
	if (state->slot)
		ExecDropSingleTupleTableSlot(state->slot);

	/*
	 * Postgres stores cached row types from `get_cached_rowtype` in the
	 * constraint expression and tries to free this type via a callback from the
	 * `per_tuple_exprcontext`. Since we create constraint expressions within
	 * the chunk insert state memory context, this leads to a series of pointers
	 * structured like: `per_tuple_exprcontext -> constraint expr (in chunk
	 * insert state) -> cached row type` if we try to free the the chunk insert
	 * state MemoryContext while the `es_per_tuple_exprcontext` is live,
	 * postgres tries to dereference a dangling pointer in one of
	 * `es_per_tuple_exprcontext`'s callbacks. Normally postgres allocates the
	 * constraint expressions in a parent context of per_tuple_exprcontext so
	 * there is no issue, however we've run into excessive memory usage due to
	 * too many constraints, and want to allocate them for a shorter lifetime so
	 * we free them when SubspaceStore gets to full. This leaves us with a
	 * memory context relationship like the following:
	 *
	 *     query_ctx
	 *       / \
	 *      /   \
	 *   CIS    per_tuple
	 *
	 *
	 * To ensure this doesn't create dangling pointers from the per-tuple
	 * context to the chunk insert state (CIS) when we destroy the CIS, we avoid
	 * freeing the CIS memory context immediately. Instead we change its parent
	 * to be the per-tuple context (if it is still alive) so that it is only
	 * freed once that context is freed:
	 *
	 *     query_ctx
	 *        \
	 *         \
	 *         per_tuple
	 *           \
	 *            \
	 *            CIS
	 *
	 * Note that a previous approach registered the chunk insert state (CIS) to
	 * be freed by a reset callback on the per-tuple context. That caused a
	 * subtle bug because both the per-tuple context and the CIS share the same
	 * parent. Thus, the callback on a child would trigger the deletion of a
	 * sibling, leading to a cyclic relationship:
	 *
	 *     query_ctx
	 *       / \
	 *      /   \
	 *   CIS <-- per_tuple
	 *
	 *
	 * With this cycle, a delete of the query_ctx could first trigger a delete
	 * of the CIS (if not already deleted), then the per_tuple context, followed
	 * by the CIS again (via the callback), and thus a crash.
	 */
	if (state->estate->es_per_tuple_exprcontext)
		MemoryContextSetParent(state->mctx,
							   state->estate->es_per_tuple_exprcontext->ecxt_per_tuple_memory);
	else
		MemoryContextDelete(state->mctx);
}
