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
#include "chunk_insert_state.h"
#include "chunk_dispatch.h"
#include "chunk_data_node.h"
#include "chunk_dispatch_state.h"
#include "chunk_index.h"
#include "indexing.h"

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
 * The first ResultRelInfo in the executor's array (corresponding to the main
 * table's) is used as a template for the chunk's new ResultRelInfo.
 */
static inline ResultRelInfo *
create_chunk_result_relation_info(ChunkDispatch *dispatch, Relation rel)
{
	ResultRelInfo *rri, *rri_orig;
	Index hyper_rti = dispatch->hypertable_result_rel_info->ri_RangeTableIndex;
	rri = palloc0(sizeof(ResultRelInfo));
	NodeSetTag(rri, T_ResultRelInfo);

	InitResultRelInfo(rri, rel, hyper_rti, NULL, dispatch->estate->es_instrument);

	/* Copy options from the main table's (hypertable's) result relation info */
	rri_orig = dispatch->hypertable_result_rel_info;
	rri->ri_WithCheckOptions = rri_orig->ri_WithCheckOptions;
	rri->ri_WithCheckOptionExprs = rri_orig->ri_WithCheckOptionExprs;
	rri->ri_junkFilter = rri_orig->ri_junkFilter;
	rri->ri_projectReturning = rri_orig->ri_projectReturning;

	rri->ri_FdwState = NULL;
	rri->ri_usesFdwDirectModify = dispatch->hypertable_result_rel_info->ri_usesFdwDirectModify;

	if (RelationGetForm(rel)->relkind == RELKIND_FOREIGN_TABLE)
		rri->ri_FdwRoutine = GetFdwRoutineForRelation(rel, true);

	create_chunk_rri_constraint_expr(rri, rel);

	return rri;
}

static inline ResultRelInfo *
create_compress_chunk_result_relation_info(ChunkDispatch *dispatch, Relation compress_rel)
{
	ResultRelInfo *rri, *rri_orig;
	Index hyper_rti = dispatch->hypertable_result_rel_info->ri_RangeTableIndex;
	rri = makeNode(ResultRelInfo);

	InitResultRelInfo(rri, compress_rel, hyper_rti, NULL, dispatch->estate->es_instrument);

	rri_orig = dispatch->hypertable_result_rel_info;
	/* RLS policies are not supported if compression is enabled */
	Assert(rri_orig->ri_WithCheckOptions == NULL && rri_orig->ri_WithCheckOptionExprs == NULL);
	Assert(rri_orig->ri_projectReturning == NULL);
	rri->ri_junkFilter = rri_orig->ri_junkFilter;

	/* compressed rel chunk is on data node. Does not need any FDW access on AN */
	rri->ri_FdwState = NULL;
	rri->ri_usesFdwDirectModify = false;
	rri->ri_FdwRoutine = NULL;
	/* constraints are executed on the orig base chunk. So we do
	 * not call create_chunk_rri_constraint_expr here
	 */
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
									 map_variable_attnos_compat((Node *) returning_clauses,
																varno,
																0,
																map->attrMap,
																map->outdesc->natts,
																rowtype,
																&found_whole_row));

	return ExecBuildProjectionInfo(returning_clauses,
								   orig->pi_exprContext,
								   orig->pi_state.resultslot,
								   NULL,
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
					  map_variable_attnos_compat((Node *) clause,
												 INNER_VAR,
												 0,
												 chunk_map->attrMap,
												 RelationGetDescr(hyper_rel)->natts,
												 RelationGetForm(chunk_rel)->reltype,
												 &found_whole_row));

	/* map hypertable attnos -> chunk attnos for the hypertable */
	clause = castNode(List,
					  map_variable_attnos_compat((Node *) clause,
												 varno,
												 0,
												 chunk_map->attrMap,
												 RelationGetDescr(hyper_rel)->natts,
												 RelationGetForm(chunk_rel)->reltype,
												 &found_whole_row));

	return clause;
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
	List *new_tlist = NIL;
	TupleDesc chunk_tupdesc = map->outdesc;
#if PG13_GE
	AttrNumber *attrMap = map->attrMap->attnums;
#else
	AttrNumber *attrMap = map->attrMap;
#endif
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

static inline ResultRelInfo *
get_chunk_rri(ChunkInsertState *state)
{
	return state->result_relation_info;
}

static inline ResultRelInfo *
get_hyper_rri(ChunkDispatch *dispatch)
{
	return dispatch->hypertable_result_rel_info;
}

/*
 * Create the ON CONFLICT state for a chunk.
 *
 * The hypertable root is used as a template. A shallow copy can be made,
 * e.g., if tuple descriptors match exactly.
 */
static void
init_basic_on_conflict_state(ResultRelInfo *hyper_rri, ResultRelInfo *chunk_rri)
{
	OnConflictSetState *onconfl = makeNode(OnConflictSetState);

	/* If no tuple conversion between the chunk and root hyper relation is
	 * needed, we can get away with a (mostly) shallow copy */
	memcpy(onconfl, hyper_rri->ri_onConflict, sizeof(OnConflictSetState));

	chunk_rri->ri_onConflict = onconfl;
}

static ExprState *
create_on_conflict_where_qual(List *clause)
{
	return ExecInitQual(clause, NULL);
}

static TupleDesc
get_default_confl_tupdesc(ChunkInsertState *state, ChunkDispatch *dispatch)
{
	return get_hyper_rri(dispatch)->ri_onConflict->oc_ProjSlot->tts_tupleDescriptor;
}

static TupleTableSlot *
get_default_confl_slot(ChunkInsertState *state, ChunkDispatch *dispatch)
{
	return get_hyper_rri(dispatch)->ri_onConflict->oc_ProjSlot;
}

static TupleTableSlot *
get_confl_slot(ChunkInsertState *state, ChunkDispatch *dispatch, TupleDesc projtupdesc)
{
	ResultRelInfo *chunk_rri = get_chunk_rri(state);

	/* PG12 has a per-relation projection slot for ON CONFLICT. Usually,
	 * these slots are tied to the executor's tuple table
	 * (estate->es_tupleTable), which tracks all slots and cleans them up
	 * at the end of exection. This doesn't work well in our case, since
	 * chunk insert states do not necessarily live to the end of execution
	 * (in order to keep memory usage down when inserting into lots of
	 * chunks). Therefore, we do NOT tie these slots to the executor
	 * state, and instead manage their lifecycles ourselves. */
	chunk_rri->ri_onConflict->oc_ProjSlot = MakeSingleTupleTableSlot(projtupdesc, &TTSOpsVirtual);

	return chunk_rri->ri_onConflict->oc_ProjSlot;
}

static TupleTableSlot *
get_default_existing_slot(ChunkInsertState *state, ChunkDispatch *dispatch)
{
	ResultRelInfo *chunk_rri = get_chunk_rri(state);

	chunk_rri->ri_onConflict->oc_Existing = table_slot_create(state->rel, NULL);

	return chunk_rri->ri_onConflict->oc_Existing;
}

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
	ResultRelInfo *chunk_rri = get_chunk_rri(state);
	ResultRelInfo *hyper_rri = get_hyper_rri(dispatch);
	Relation chunk_rel = state->result_relation_info->ri_RelationDesc;
	Relation hyper_rel = dispatch->hypertable_result_rel_info->ri_RelationDesc;

	Assert(ts_chunk_dispatch_get_on_conflict_action(dispatch) == ONCONFLICT_UPDATE);
	init_basic_on_conflict_state(hyper_rri, chunk_rri);

	/* Setup default slots for ON CONFLICT handling, in case of no tuple
	 * conversion.  */
	state->existing_slot = get_default_existing_slot(state, dispatch);
	state->conflproj_tupdesc = get_default_confl_tupdesc(state, dispatch);
	state->conflproj_slot = get_default_confl_slot(state, dispatch);

	if (NULL != map)
	{
		ExprContext *econtext = hyper_rri->ri_onConflict->oc_ProjInfo->pi_exprContext;
		Node *onconflict_where = ts_chunk_dispatch_get_on_conflict_where(dispatch);
		List *onconflset;

		Assert(map->outdesc == RelationGetDescr(chunk_rel));

		if (NULL == chunk_map)
			chunk_map = convert_tuples_by_name_compat(RelationGetDescr(chunk_rel),
													  RelationGetDescr(hyper_rel),
													  gettext_noop("could not convert row type"));

		onconflset = translate_clause(ts_chunk_dispatch_get_on_conflict_set(dispatch),
									  chunk_map,
									  hyper_rri->ri_RangeTableIndex,
									  hyper_rel,
									  chunk_rel);

		onconflset = adjust_hypertable_tlist(onconflset, state->hyper_to_chunk_map);

		/* create the tuple slot for the UPDATE SET projection */
		state->conflproj_tupdesc = ExecTypeFromTL(onconflset);
		state->conflproj_slot = get_confl_slot(state, dispatch, state->conflproj_tupdesc);

		/* build UPDATE SET projection state */
		chunk_rri->ri_onConflict->oc_ProjInfo =
			ExecBuildProjectionInfo(onconflset,
									econtext,
									state->conflproj_slot,
									NULL,
									RelationGetDescr(chunk_rel));

		/*
		 * Map attribute numbers in the WHERE clause, if it exists.
		 */
		if (NULL != onconflict_where)
		{
			List *clause = translate_clause(castNode(List, onconflict_where),
											chunk_map,
											hyper_rri->ri_RangeTableIndex,
											hyper_rel,
											chunk_rel);

			chunk_rri->ri_onConflict->oc_WhereClause = create_on_conflict_where_qual(clause);
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
	List *arbiter_indexes = ts_chunk_dispatch_get_arbiter_indexes(dispatch);
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
	OnConflictAction onconflict_action = ts_chunk_dispatch_get_on_conflict_action(dispatch);

	if (ts_chunk_dispatch_has_returning(dispatch))
	{
		/*
		 * We need the opposite map from cis->hyper_to_chunk_map. The map needs
		 * to have the hypertable_desc in the out spot for map_variable_attnos
		 * to work correctly in mapping hypertable attnos->chunk attnos.
		 */
		chunk_map = convert_tuples_by_name_compat(RelationGetDescr(chunk_rel),
												  RelationGetDescr(hyper_rel),
												  gettext_noop("could not convert row type"));

		chunk_rri->ri_projectReturning =
			get_adjusted_projection_info_returning(chunk_rri->ri_projectReturning,
												   ts_chunk_dispatch_get_returning_clauses(
													   dispatch),
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

/* Assumption: we have acquired a lock on the chunk table. This is
 *      important because lock acquisition order is always orignal chunk,
 *      followed by compressed chunk to prevent deadlocks.
 * We now try to acquire a lock on the compressed chunk, if one exists.
 * Note that the insert could have been blocked by a recompress_chunk operation.
 * So the compressed chunk could have moved under us. We need to refetch the chunk
 * to get the correct compressed chunk id (github issue 3400)
 */
static Relation
lock_associated_compressed_chunk(int32 chunk_id, bool *has_compressed_chunk)
{
	Relation compress_rel = NULL;
	Chunk *orig_chunk = ts_chunk_get_by_id(chunk_id, true);
	Oid compress_chunk_relid = InvalidOid;
	*has_compressed_chunk = false;
	if (orig_chunk->fd.compressed_chunk_id)
		compress_chunk_relid = ts_chunk_get_relid(orig_chunk->fd.compressed_chunk_id, false);
	if (compress_chunk_relid != InvalidOid)
	{
		*has_compressed_chunk = true;
		compress_rel = table_open(compress_chunk_relid, RowExclusiveLock);
	}
	return compress_rel;
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
	Relation rel, parent_rel, compress_rel = NULL;
	MemoryContext old_mcxt;
	MemoryContext cis_context = AllocSetContextCreate(dispatch->estate->es_query_cxt,
													  "chunk insert state memory context",
													  ALLOCSET_DEFAULT_SIZES);
	OnConflictAction onconflict_action = ts_chunk_dispatch_get_on_conflict_action(dispatch);
	ResultRelInfo *resrelinfo, *relinfo;
	bool has_compressed_chunk = (chunk->fd.compressed_chunk_id != 0);

	/* permissions NOT checked here; were checked at hypertable level */
	if (check_enable_rls(chunk->table_id, InvalidOid, false) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("hypertables do not support row-level security")));

	if (chunk->relkind != RELKIND_RELATION && chunk->relkind != RELKIND_FOREIGN_TABLE)
		elog(ERROR, "insert is not on a table");

	if (has_compressed_chunk &&
		(onconflict_action != ONCONFLICT_NONE || ts_chunk_dispatch_has_returning(dispatch)))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("insert with ON CONFLICT or RETURNING clause is not supported on "
						"compressed chunks")));

	/*
	 * We must allocate the range table entry on the executor's per-query
	 * context
	 */
	old_mcxt = MemoryContextSwitchTo(dispatch->estate->es_query_cxt);

	rel = table_open(chunk->table_id, RowExclusiveLock);
	if (has_compressed_chunk && ts_indexing_relation_has_primary_or_unique_index(rel))
	{
		table_close(rel, RowExclusiveLock);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("insert into a compressed chunk that has primary or unique constraint is "
						"not supported")));
	}
	compress_rel = lock_associated_compressed_chunk(chunk->fd.id, &has_compressed_chunk);

	MemoryContextSwitchTo(cis_context);
	relinfo = create_chunk_result_relation_info(dispatch, rel);
	if (!has_compressed_chunk)
		resrelinfo = relinfo;
	else
	{
		/* insert the tuple into the compressed chunk instead */
		resrelinfo = create_compress_chunk_result_relation_info(dispatch, compress_rel);
	}
	CheckValidResultRel(resrelinfo, ts_chunk_dispatch_get_cmd_type(dispatch));

	state = palloc0(sizeof(ChunkInsertState));
	state->mctx = cis_context;
	state->rel = rel;
	state->result_relation_info = resrelinfo;
	state->estate = dispatch->estate;

	if (resrelinfo->ri_RelationDesc->rd_rel->relhasindex &&
		resrelinfo->ri_IndexRelationDescs == NULL)
		ExecOpenIndices(resrelinfo, onconflict_action != ONCONFLICT_NONE);

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

		if (has_compressed_chunk && tg->trig_insert_after_row)
		{
			StringInfo trigger_list = makeStringInfo();
			int i = 0;
			Assert(tg->numtriggers > 0);
			while (i < tg->numtriggers)
			{
				appendStringInfoString(trigger_list, tg->triggers[i].tgname);
				if (++i < tg->numtriggers)
					appendStringInfoString(trigger_list, ", ");
			}
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("after insert row trigger on compressed chunk not supported"),
					 errdetail("Triggers: %s", trigger_list->data),
					 errhint("Decompress the chunk first before inserting into it.")));
		}
	}

	parent_rel = table_open(dispatch->hypertable->main_table_relid, AccessShareLock);

	/* Set tuple conversion map, if tuple needs conversion. We don't want to
	 * convert tuples going into foreign tables since these are actually sent to
	 * data nodes for insert on that node's local hypertable. */
	if (chunk->relkind != RELKIND_FOREIGN_TABLE)
		state->hyper_to_chunk_map =
			convert_tuples_by_name_compat(RelationGetDescr(parent_rel),
										  RelationGetDescr(rel),
										  gettext_noop("could not convert row type"));

	adjust_projections(state, dispatch, RelationGetForm(rel)->reltype);

	if (has_compressed_chunk)
	{
		int32 htid = ts_hypertable_relid_to_id(chunk->hypertable_relid);
		/* this is true as compressed chunks are not created on access node */
		Assert(chunk->relkind != RELKIND_FOREIGN_TABLE);
		Assert(compress_rel != NULL);
		state->compress_rel = compress_rel;
		Assert(ts_cm_functions->compress_row_init != NULL);
		/* need a way to convert from chunk tuple to compressed chunk tuple */
		state->compress_state = ts_cm_functions->compress_row_init(htid, rel, compress_rel);
		state->orig_result_relation_info = relinfo;
	}
	else
	{
		state->compress_state = NULL;
	}

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
		RangeTblEntry *rte =
			rt_fetch(resrelinfo->ri_RangeTableIndex, dispatch->estate->es_range_table);

		Assert(NULL != rte);

		state->user_id = OidIsValid(rte->checkAsUser) ? rte->checkAsUser : GetUserId();
		state->chunk_data_nodes = ts_chunk_data_nodes_copy(chunk);
	}

	if (dispatch->hypertable_result_rel_info->ri_usesFdwDirectModify)
	{
		/* If the hypertable is setup for direct modify, we do not really use
		 * the FDW. Instead exploit the FdwPrivate pointer to pass on the
		 * chunk insert state to DataNodeDispatch so that it knows which data nodes
		 * to insert into. */
		resrelinfo->ri_FdwState = state;
	}
	else if (NULL != resrelinfo->ri_FdwRoutine && !resrelinfo->ri_usesFdwDirectModify &&
			 NULL != resrelinfo->ri_FdwRoutine->BeginForeignModify)
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
		resrelinfo->ri_FdwRoutine->BeginForeignModify(mtstate,
													  resrelinfo,
													  fdwprivate,
													  0,
													  dispatch->eflags);
	}

	MemoryContextSwitchTo(old_mcxt);

	return state;
}

extern void
ts_chunk_insert_state_destroy(ChunkInsertState *state)
{
	ResultRelInfo *rri = state->result_relation_info;

	if (NULL != rri->ri_FdwRoutine && !rri->ri_usesFdwDirectModify &&
		NULL != rri->ri_FdwRoutine->EndForeignModify)
		rri->ri_FdwRoutine->EndForeignModify(state->estate, rri);

	destroy_on_conflict_state(state);
	ExecCloseIndices(state->result_relation_info);

	if (state->compress_rel)
	{
		ResultRelInfo *orig_chunk_rri = state->orig_result_relation_info;
		Oid chunk_relid = RelationGetRelid(orig_chunk_rri->ri_RelationDesc);
		ts_cm_functions->compress_row_end(state->compress_state);
		ts_cm_functions->compress_row_destroy(state->compress_state);
		Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
		if (!ts_chunk_is_unordered(chunk))
			ts_chunk_set_unordered(chunk);
		table_close(state->compress_rel, NoLock);
	}
	else if (RelationGetForm(state->result_relation_info->ri_RelationDesc)->relkind ==
			 RELKIND_FOREIGN_TABLE)
	{
		/* If a distributed chunk shows compressed status on AN,
		 * we mark it as unordered , because the insert now goes into
		 * a previously compressed chunk
		 */
		Oid chunk_relid = RelationGetRelid(state->result_relation_info->ri_RelationDesc);
		Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, true);
		if (ts_chunk_is_compressed(chunk) && (!ts_chunk_is_unordered(chunk)))
			ts_chunk_set_unordered(chunk);
	}

	table_close(state->rel, NoLock);
	if (NULL != state->slot)
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
	if (state->estate->es_per_tuple_exprcontext != NULL)
		MemoryContextSetParent(state->mctx,
							   state->estate->es_per_tuple_exprcontext->ecxt_per_tuple_memory);
	else
		MemoryContextDelete(state->mctx);
}
