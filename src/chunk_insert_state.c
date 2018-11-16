/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>

#include <utils/rel.h>
#include <utils/rls.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <nodes/plannodes.h>
#include <nodes/relation.h>
#include <access/xact.h>
#include <optimizer/plancat.h>
#include <optimizer/clauses.h>
#include <optimizer/planner.h>
#include <miscadmin.h>
#include <parser/parsetree.h>
#include <rewrite/rewriteManip.h>
#include <nodes/makefuncs.h>
#include <catalog/pg_type.h>

#include "errors.h"
#include "chunk_insert_state.h"
#include "chunk_dispatch.h"
#include "chunk_dispatch_state.h"
#include "compat.h"
#include "chunk_index.h"

/*
 * Create a new RangeTblEntry for the chunk in the executor's range table and
 * return the index.
 */
static inline Index
create_chunk_range_table_entry(ChunkDispatch *dispatch, Relation rel)
{
	RangeTblEntry *rte;
	ListCell   *lc;
	Index		rti = 1;
	EState	   *estate = dispatch->estate;


	/*
	 * Check if we previously created an entry for this relation. This can
	 * happen if we close a chunk insert state and then reopen it in the same
	 * transaction. Reusing an entry ensures the range table never grows
	 * larger than the number of chunks in a table, although it imposes a
	 * penalty for looking up old entries.
	 */
	foreach(lc, estate->es_range_table)
	{
		rte = lfirst(lc);

		if (rte->relid == RelationGetRelid(rel))
			return rti;
		rti++;
	}


	rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = RelationGetRelid(rel);
	rte->relkind = rel->rd_rel->relkind;
	rte->requiredPerms = ACL_INSERT;

	/*
	 * If the hypertable has a rangetable entry, copy some information from
	 * its eref to the chunk's eref so that explain analyze works correctly.
	 */
	if (0 != dispatch->hypertable_result_rel_info->ri_RangeTableIndex)
	{
		RangeTblEntry *hypertable_rte;

		hypertable_rte = rt_fetch(dispatch->hypertable_result_rel_info->ri_RangeTableIndex, estate->es_range_table);
		rte->eref = hypertable_rte->eref;
	}

	/*
	 * If this is the first tuple, we make a copy of the range table to avoid
	 * modifying the old list.
	 */
	if (estate->es_processed == 0)
		estate->es_range_table = list_copy(estate->es_range_table);

	estate->es_range_table = lappend(estate->es_range_table, rte);

	return list_length(estate->es_range_table);
}


/*
 * Convert a tuple to match the chunk's rowtype. This might be necessary if the
 * main table has been modified, e.g., a column was dropped. The dropped column
 * will remain on existing tables (marked as dropped) but won't be created on
 * new tables (chunks). This leads to a situation where the root table and
 * chunks can have different attnums for columns.
 */
HeapTuple
chunk_insert_state_convert_tuple(ChunkInsertState *state,
								 HeapTuple tuple,
								 TupleTableSlot **existing_slot)
{
	Relation	chunkrel = state->result_relation_info->ri_RelationDesc;

	if (NULL == state->tup_conv_map)
		/* No conversion needed */
		return tuple;

	tuple = do_convert_tuple(tuple, state->tup_conv_map);

	ExecSetSlotDescriptor(state->slot, RelationGetDescr(chunkrel));
	ExecStoreTuple(tuple, state->slot, InvalidBuffer, true);

	if (NULL != existing_slot)
		*existing_slot = state->slot;

	return tuple;
}

/* Just like ExecPrepareExpr except that it doesn't switch to the query memory context */
static inline ExprState *
prepare_constr_expr(Expr *node)
{
	ExprState  *result;

	node = expression_planner(node);
	result = ExecInitExpr(node, NULL);

	return result;
}

/*
 * Create the constraint exprs inside the current memory context. If this
 * is not done here, then ExecRelCheck will do it for you but put it into
 * the query memory context, which will cause a memory leak.
 *
 * See the comment in `chunk_insert_state_destroy` for more information
 * on the implications of this.
 */
static inline void
create_chunk_rri_constraint_expr(ResultRelInfo *rri, Relation rel)
{
	int			ncheck,
				i;
	ConstrCheck *check;

	Assert(rel->rd_att->constr != NULL &&
		   rri->ri_ConstraintExprs == NULL);

	ncheck = rel->rd_att->constr->num_check;
	check = rel->rd_att->constr->check;

#if PG10
	rri->ri_ConstraintExprs =
		(ExprState **) palloc(ncheck * sizeof(ExprState *));

	for (i = 0; i < ncheck; i++)
	{
		Expr	   *checkconstr = stringToNode(check[i].ccbin);

		rri->ri_ConstraintExprs[i] =
			prepare_constr_expr(checkconstr);
	}
#elif PG96
	rri->ri_ConstraintExprs =
		(List **) palloc(ncheck * sizeof(List *));

	for (i = 0; i < ncheck; i++)
	{
		/* ExecQual wants implicit-AND form */
		List	   *qual = make_ands_implicit(stringToNode(check[i].ccbin));

		rri->ri_ConstraintExprs[i] = (List *)
			prepare_constr_expr((Expr *) qual);
	}
#endif
}

/*
 * Create a new ResultRelInfo for a chunk.
 *
 * The ResultRelInfo holds the executor state (e.g., open relation, indexes, and
 * options) for the result relation where tuples will be stored.
 *
 * The first ResultRelInfo in the executor's array (corresponding to the main
 * table's) is used as a template for the chunk's new ResultRelInfo.
 */
static inline ResultRelInfo *
create_chunk_result_relation_info(ChunkDispatch *dispatch, Relation rel, Index rti)
{
	ResultRelInfo *rri,
			   *rri_orig;


	rri = palloc0(sizeof(ResultRelInfo));
	NodeSetTag(rri, T_ResultRelInfo);

	InitResultRelInfoCompat(rri,
							rel,
							rti,
							dispatch->estate->es_instrument);

	/* Copy options from the main table's (hypertable's) result relation info */
	rri_orig = dispatch->hypertable_result_rel_info;
	rri->ri_WithCheckOptions = rri_orig->ri_WithCheckOptions;
	rri->ri_WithCheckOptionExprs = rri_orig->ri_WithCheckOptionExprs;
	rri->ri_junkFilter = rri_orig->ri_junkFilter;
	rri->ri_projectReturning = rri_orig->ri_projectReturning;
	rri->ri_onConflictSetProj = rri_orig->ri_onConflictSetProj;
	rri->ri_onConflictSetWhere = rri_orig->ri_onConflictSetWhere;

	create_chunk_rri_constraint_expr(rri, rel);

	return rri;
}

static ProjectionInfo *
get_adjusted_projection_info_returning(ProjectionInfo *orig, List *returning_clauses, AttrNumber *map,
									   int map_size, Index varno, Oid rowtype,
									   TupleDesc chunk_desc)
{
	bool		found_whole_row;

	Assert(returning_clauses != NIL);

	/* map hypertable attnos -> chunk attnos */
	returning_clauses = (List *) map_variable_attnos_compat((Node *) returning_clauses, varno, 0, map, map_size,
															rowtype, &found_whole_row);

	return ExecBuildProjectionInfoCompat(returning_clauses, orig->pi_exprContext,
										 get_projection_info_slot_compat(orig), NULL, chunk_desc);
}

static ExprState *
get_adjusted_onconflictupdate_where(ExprState *hyper_where_state, List *where_quals, AttrNumber *map,
									int map_size, Index varno, Oid rowtype)
{
	bool		found_whole_row;

	Assert(where_quals != NIL);
	/* map hypertable attnos -> chunk attnos for the hypertable */
	where_quals = (List *) map_variable_attnos_compat((Node *) where_quals, varno, 0, map, map_size,
													  rowtype, &found_whole_row);
	/* map hypertable attnos -> chunk attnos for the "excluded" table */
	where_quals = (List *) map_variable_attnos_compat((Node *) where_quals, INNER_VAR, 0, map, map_size,
													  rowtype, &found_whole_row);
#if PG96
	return ExecInitExpr((Expr *) where_quals, NULL);
#else
	return ExecInitQual(where_quals, NULL);
#endif
}

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
	List	   *new_tlist = NIL;
	TupleDesc	chunk_tupdesc = map->outdesc;
	AttrNumber *attrMap = map->attrMap;
	AttrNumber	chunk_attrno;

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
			Const	   *expr;

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
							 true,	/* isnull */
							 true /* byval */ );
			tle = makeTargetEntry((Expr *) expr,
								  chunk_attrno,
								  pstrdup(NameStr(att_tup->attname)),
								  false);
		}
		new_tlist = lappend(new_tlist, tle);
	}
	return new_tlist;
}

static ProjectionInfo *
get_adjusted_projection_info_onconflicupdate(ProjectionInfo *orig, List *update_tles, AttrNumber *variable_attnos_map,
											 int variable_attnos_map_size, Index varno, Oid rowtype,
											 TupleDesc chunk_desc, TupleConversionMap *hypertable_to_chunk_map)
{
	bool		found_whole_row;

	Assert(update_tles != NIL);
	/* copy the tles so as not to scribble on the hypertable tles */
	update_tles = copyObject(update_tles);

	/* map hypertable attnos -> chunk attnos for the hypertable */
	update_tles = (List *) map_variable_attnos_compat((Node *) update_tles, varno, 0, variable_attnos_map, variable_attnos_map_size,
													  rowtype, &found_whole_row);
	/* map hypertable attnos -> chunk attnos for the "excluded" table */
	update_tles = (List *) map_variable_attnos_compat((Node *) update_tles, INNER_VAR, 0, variable_attnos_map, variable_attnos_map_size,
													  rowtype, &found_whole_row);


	update_tles = adjust_hypertable_tlist(update_tles, hypertable_to_chunk_map);

	/* set the projection result slot to the chunk schema */
	ExecSetSlotDescriptor(get_projection_info_slot_compat(orig), chunk_desc);
	return ExecBuildProjectionInfoCompat(update_tles, orig->pi_exprContext,
										 get_projection_info_slot_compat(orig), NULL, chunk_desc);
}

/* Change the projections to work with chunks instead of hypertables */
static void
adjust_projections(ChunkInsertState *cis, ChunkDispatch *dispatch, Oid rowtype)
{
	ResultRelInfo *rri = cis->result_relation_info;
	AttrNumber *variable_attnos_map;
	int			variable_attnos_map_size;
	TupleDesc	chunk_desc = cis->tup_conv_map->outdesc;
	TupleDesc	hypertable_desc = cis->tup_conv_map->indesc;

	Assert(cis->tup_conv_map != NULL);

	/*
	 * we need the opposite map from cis->tup_conv_map. The map needs to have
	 * the hypertable_desc in the out spot for map_variable_attnos to work
	 * correctly in mapping hypertable attnos->chunk attnos
	 */
	variable_attnos_map = convert_tuples_by_name_map(chunk_desc,
													 hypertable_desc,
													 gettext_noop("could not convert row type"));
	variable_attnos_map_size = hypertable_desc->natts;

	if (rri->ri_projectReturning != NULL)
	{
		rri->ri_projectReturning =
			get_adjusted_projection_info_returning(rri->ri_projectReturning,
												   list_nth(dispatch->returning_lists, dispatch->returning_index),
												   variable_attnos_map,
												   variable_attnos_map_size,
												   dispatch->hypertable_result_rel_info->ri_RangeTableIndex,
												   rowtype,
												   chunk_desc);
	}

	if (rri->ri_onConflictSetProj != NULL)
	{
		rri->ri_onConflictSetProj =
			get_adjusted_projection_info_onconflicupdate(rri->ri_onConflictSetProj,
														 dispatch->on_conflict_set,
														 variable_attnos_map,
														 variable_attnos_map_size,
														 dispatch->hypertable_result_rel_info->ri_RangeTableIndex,
														 rowtype,
														 chunk_desc, cis->tup_conv_map);

		if (rri->ri_onConflictSetWhere != NULL)
		{
#if PG96
			rri->ri_onConflictSetWhere = (List *)
#else
			rri->ri_onConflictSetWhere =
#endif
				get_adjusted_onconflictupdate_where((ExprState *) rri->ri_onConflictSetWhere,
													dispatch->on_conflict_where,
													variable_attnos_map,
													variable_attnos_map_size,
													dispatch->hypertable_result_rel_info->ri_RangeTableIndex,
													rowtype);
		}
	}
}

/*
 * Check if tuple conversion is needed between a chunk and its parent table.
 *
 * Since a chunk should have the same attributes (columns) as its parent, the
 * only reason tuple conversion should be needed is if the parent has had one or
 * more columns removed, leading to a garbage attribute and inflated number of
 * attributes that aren't inherited by new children tables.
 */
static inline bool
tuple_conversion_needed(TupleDesc indesc,
						TupleDesc outdesc)
{
	return (indesc->natts != outdesc->natts ||
			indesc->tdhasoid != outdesc->tdhasoid);
}

/* Translate hypertable indexes to chunk indexes in the arbiter clause */
static void
chunk_insert_state_set_arbiter_indexes(ChunkInsertState *state, ChunkDispatch *dispatch, Relation chunk_rel)
{
	ListCell   *lc;

	state->arbiter_indexes = NIL;
	foreach(lc, dispatch->arbiter_indexes)
	{
		Oid			hypertable_index = lfirst_oid(lc);
		Chunk	   *chunk = chunk_get_by_relid(RelationGetRelid(chunk_rel), 0, true);
		ChunkIndexMapping *cim = chunk_index_get_by_hypertable_indexrelid(chunk, hypertable_index);

		state->arbiter_indexes = lappend_oid(state->arbiter_indexes, cim->indexoid);
	}
}

/*
 * Create new insert chunk state.
 *
 * This is essentially a ResultRelInfo for a chunk. Initialization of the
 * ResultRelInfo should be similar to ExecInitModifyTable().
 */
extern ChunkInsertState *
chunk_insert_state_create(Chunk *chunk, ChunkDispatch *dispatch)
{
	ChunkInsertState *state;
	Relation	rel,
				parent_rel;
	Index		rti;
	MemoryContext old_mcxt;
	MemoryContext cis_context = AllocSetContextCreate(dispatch->estate->es_query_cxt,
													  "chunk insert state memory context",
													  ALLOCSET_DEFAULT_SIZES);
	ResultRelInfo *resrelinfo;

	/* permissions NOT checked here; were checked at hypertable level */
	if (check_enable_rls(chunk->table_id, InvalidOid, false) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support row-level security")));

	/*
	 * We must allocate the range table entry on the executor's per-query
	 * context
	 */
	old_mcxt = MemoryContextSwitchTo(dispatch->estate->es_query_cxt);

	rel = heap_open(chunk->table_id, RowExclusiveLock);

	if (rel->rd_rel->relkind != RELKIND_RELATION)
		elog(ERROR, "insert is not on a table");

	rti = create_chunk_range_table_entry(dispatch, rel);

	MemoryContextSwitchTo(cis_context);
	resrelinfo = create_chunk_result_relation_info(dispatch, rel, rti);
	CheckValidResultRelCompat(resrelinfo, dispatch->cmd_type);

	state = palloc0(sizeof(ChunkInsertState));
	state->mctx = cis_context;
	state->rel = rel;
	state->result_relation_info = resrelinfo;
	state->estate = dispatch->estate;

	if (resrelinfo->ri_RelationDesc->rd_rel->relhasindex &&
		resrelinfo->ri_IndexRelationDescs == NULL)
		ExecOpenIndices(resrelinfo, dispatch->on_conflict != ONCONFLICT_NONE);

	if (resrelinfo->ri_TrigDesc != NULL)
	{
		if (resrelinfo->ri_TrigDesc->trig_insert_instead_row ||
			resrelinfo->ri_TrigDesc->trig_insert_after_statement ||
			resrelinfo->ri_TrigDesc->trig_insert_before_statement)
			elog(ERROR, "insert trigger on chunk table not supported");
	}

	/* Set the chunk's arbiter indexes for ON CONFLICT statements */
	if (dispatch->on_conflict != ONCONFLICT_NONE)
		chunk_insert_state_set_arbiter_indexes(state, dispatch, rel);

	/* Set tuple conversion map, if tuple needs conversion */
	parent_rel = heap_open(dispatch->hypertable->main_table_relid, AccessShareLock);

	if (tuple_conversion_needed(RelationGetDescr(parent_rel), RelationGetDescr(rel)))
	{
		state->tup_conv_map = convert_tuples_by_name(RelationGetDescr(parent_rel),
													 RelationGetDescr(rel),
													 gettext_noop("could not convert row type"));
		adjust_projections(state, dispatch, RelationGetForm(rel)->reltype);
	}

	/* Need a tuple table slot to store converted tuples */
	if (state->tup_conv_map)
		state->slot = MakeTupleTableSlot();

	heap_close(parent_rel, AccessShareLock);

	MemoryContextSwitchTo(old_mcxt);

	return state;
}

extern void
chunk_insert_state_destroy(ChunkInsertState *state)
{
	if (state == NULL)
		return;

	ExecCloseIndices(state->result_relation_info);
	heap_close(state->rel, NoLock);

	/*
	 * Postgres stores cached row types from `get_cached_rowtype` in the
	 * contraint expression and tries to free this type via a callback from
	 * the `per_tuple_exprcontext`. Since we create constraint expressions
	 * within the chunk insert state memory context, this leads to a series of
	 * pointers strutured like: `per_tuple_exprcontext -> constraint expr (in
	 * chunk insert state) -> cached row type` if we try to free the the chunk
	 * insert state MemoryContext while the `es_per_tuple_exprcontext` is
	 * live, postgres tries to dereference a dangling pointer in one of
	 * `es_per_tuple_exprcontext`'s callbacks. Normally postgres allocates the
	 * constraint expressions in a parent context of per_tuple_exprcontext so
	 * there is no issue, however we've run into excessive memory ussage due
	 * to too many constraints, and want to allocate them for a shorter
	 * lifetime. To ensure this doesn't create dangling pointers, we free the
	 * `per_tuple_exprcontext`, and call all its callbacks, here, before the
	 * chunk insert state memory context.
	 *
	 * This may not be entirely safe. Right now it appears that in the case
	 * where the chunk insert both the `es_per_tuple_exprcontext` and chunk
	 * insert state exist they are freed at approximately the same time.
	 * However, postgres does not guarantee this (and indeed does not know
	 * anything about `chunk_insert_state`) so this may not hold true in the
	 * future. A better fix may be to tie the `chunk_insert_state` lifetime to
	 * the lifetime of a known postgres memory context (possibly
	 * tuple_exprcontext's?) and handle cases where the insert state is freed
	 * early.
	 */
	if (state->estate->es_per_tuple_exprcontext != NULL)
		FreeExprContext(state->estate->es_per_tuple_exprcontext, true);

	state->estate->es_per_tuple_exprcontext = NULL;

	if (NULL != state->slot)
		ExecDropSingleTupleTableSlot(state->slot);

	MemoryContextDelete(state->mctx);
}
