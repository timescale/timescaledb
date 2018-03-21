#include <postgres.h>

#include <utils/rel.h>
#include <utils/rls.h>
#include <utils/lsyscache.h>
#include <utils/guc.h>
#include <nodes/plannodes.h>
#include <nodes/relation.h>
#include <access/xact.h>
#include <optimizer/plancat.h>
#include <optimizer/clauses.h>
#include <optimizer/planner.h>
#include <miscadmin.h>
#include <parser/parsetree.h>

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
				 errmsg("Hypertables don't support row-level security")));

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

	if (resrelinfo->ri_RelationDesc->rd_rel->relhasindex &&
		resrelinfo->ri_IndexRelationDescs == NULL)
		ExecOpenIndices(resrelinfo, dispatch->on_conflict != ONCONFLICT_NONE);

	if (resrelinfo->ri_TrigDesc != NULL)
	{
		if (resrelinfo->ri_TrigDesc->trig_insert_instead_row ||
			resrelinfo->ri_TrigDesc->trig_insert_after_statement ||
			resrelinfo->ri_TrigDesc->trig_insert_before_statement)
			elog(ERROR, "Insert trigger on chunk table not supported");
	}

	/* Set the chunk's arbiter indexes for ON CONFLICT statements */
	if (dispatch->on_conflict != ONCONFLICT_NONE)
		chunk_insert_state_set_arbiter_indexes(state, dispatch, rel);

	/* Set tuple conversion map, if tuple needs conversion */
	parent_rel = heap_open(dispatch->hypertable->main_table_relid, AccessShareLock);

	if (tuple_conversion_needed(RelationGetDescr(parent_rel), RelationGetDescr(rel)))
		state->tup_conv_map = convert_tuples_by_name(RelationGetDescr(parent_rel),
													 RelationGetDescr(rel),
													 gettext_noop("could not convert row type"));

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

	if (NULL != state->slot)
		ExecDropSingleTupleTableSlot(state->slot);

	MemoryContextDelete(state->mctx);
}
