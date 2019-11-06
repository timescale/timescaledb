/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/pg_list.h>
#if PG12_LT
#include <nodes/relation.h>
#include <optimizer/var.h>
#endif
#include <optimizer/planner.h>
#include <optimizer/cost.h>
#include <optimizer/tlist.h>
#include <parser/parse_func.h>
#include <utils/lsyscache.h>

#include "plan_partialize.h"
#include "extension_constants.h"
#include "utils.h"

#define TS_PARTIALFN "partialize_agg"

typedef struct PartializeWalkerState
{
	bool found_partialize;
	bool found_non_partial_agg;
	bool looking_for_agg;
	Oid fnoid;
} PartializeWalkerState;

/*
 * Look for the partialize function in a target list and mark the wrapped
 * aggregate as a partial aggregate.
 *
 * The partialize function is an expression of the form:
 *
 * _timescaledb_internal.partialize_agg(avg(temp))
 *
 * where avg(temp) can be replaced by any aggregate that can be partialized.
 *
 * When such an expression is found, this function will mark the Aggref node
 * for the aggregate as partial.
 */
static bool
check_for_partialize_function_call(Node *node, PartializeWalkerState *state)
{
	if (node == NULL)
		return false;

	/*
	 * If the last node we saw was partialize, the next one must be aggregate
	 * we're partializing
	 */
	if (state->looking_for_agg && !IsA(node, Aggref))
		elog(ERROR, "the input to partialize must be an aggregate");

	if (IsA(node, Aggref))
	{
		Aggref *aggref = castNode(Aggref, node);

		if (state->looking_for_agg)
		{
			state->looking_for_agg = false;
			aggref->aggsplit = AGGSPLIT_INITIAL_SERIAL;

			if (aggref->aggtranstype == INTERNALOID &&
				DO_AGGSPLIT_SERIALIZE(AGGSPLIT_INITIAL_SERIAL))
				aggref->aggtype = BYTEAOID;
			else
				aggref->aggtype = aggref->aggtranstype;
		}

		/* We currently cannot handle cases like
		 *     SELECT sum(i), partialize(sum(i)) ...
		 *
		 * We check for non-partial aggs to ensure that if any of the aggregates
		 * in a statement are partialized, all of them have to be.
		 */
		else if (aggref->aggsplit != AGGSPLIT_INITIAL_SERIAL)
			state->found_non_partial_agg = true;
	}
	else if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == state->fnoid)
	{
		state->found_partialize = true;
		state->looking_for_agg = true;
	}

	return expression_tree_walker(node, check_for_partialize_function_call, state);
}

static bool
has_partialize_function(Query *parse)
{
	Oid partialfnoid = InvalidOid;
	Oid argtyp[] = { ANYELEMENTOID };

	PartializeWalkerState state = { .found_partialize = false,
									.found_non_partial_agg = false,
									.looking_for_agg = false,
									.fnoid = InvalidOid };
	List *name = list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(TS_PARTIALFN));

	partialfnoid = LookupFuncName(name, lengthof(argtyp), argtyp, false);
	Assert(partialfnoid != InvalidOid);
	state.fnoid = partialfnoid;
	check_for_partialize_function_call((Node *) parse->targetList, &state);

	if (state.found_partialize && state.found_non_partial_agg)
		elog(ERROR, "cannot mix partialized and non-partialized aggregates in the same statement");

	return state.found_partialize;
}

/*
 * Modify all AggPaths in relation to use partial aggregation.
 */
static void
partialize_agg_paths(RelOptInfo *rel)
{
	ListCell *lc;

	foreach (lc, rel->pathlist)
	{
		Path *path = lfirst(lc);

		if (IsA(path, AggPath))
			castNode(AggPath, path)->aggsplit = AGGSPLIT_INITIAL_SERIAL;
	}
}

/*
 * Turn an aggregate relation into a partial aggregate relation if aggregates
 * are enclosed by the partialize_agg function.
 *
 * The partialize_agg function can "manually" partialize an aggregate. For
 * instance:
 *
 *  SELECT time_bucket('1 day', time), device,
 *  __timescaledb_internal.partialize_agg(avg(temp))
 *  GROUP BY 1, 2;
 *
 * Would compute the partial aggregate of avg(temp).
 *
 * The plan to compute the relation must be either entirely non-partial or
 * entirely partial, so it is not possible to mix partials and
 * non-partials. Note that aggregates can appear in both the target list and the
 * HAVING clause, for instance:
 *
 *  SELECT time_bucket('1 day', time), device, avg(temp)
 *  GROUP BY 1, 2
 *  HAVING avg(temp) > 3;
 *
 * Regular partial aggregations executed by the planner (i.e., those not induced
 * by the partialize_agg function) have their HAVING aggregates transparently
 * moved to the target list during planning so that the finalize node can use it
 * when applying the final filter on the resulting groups, obviously omitting
 * the extra columns in the final output/projection. However, it doesn't make
 * much sense to transparently do that when partializing with partialize_agg
 * since it would be odd to return more columns than requested by the
 * user. Therefore, the caller would have to do that manually. This, in fact, is
 * also done when materializing continuous aggregates.
 *
 * For this reason, HAVING clauses with partialize_agg are blocked, except in
 * cases where the planner transparently reduces the having expression to a
 * simple filter (e.g., HAVING device > 3). In such cases, the HAVING clause is
 * removed and replaced by a filter on the input.
 * Returns : true if partial aggs were found, false otherwise.
 */
bool
ts_plan_process_partialize_agg(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel)
{
	Query *parse = root->parse;
#if PG10_GE
	Assert(IS_UPPER_REL(output_rel));
#endif

	if (CMD_SELECT != parse->commandType || !parse->hasAggs)
		return false;

	if (!has_partialize_function(parse))
		return false;

	/* We cannot check root->hasHavingqual here because sometimes the
	 * planner can replace the HAVING clause with a simple filter. But
	 * root->hashavingqual stays true to remember that the query had a
	 * HAVING clause initially. */
	if (NULL != parse->havingQual)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot partialize aggregate with HAVING clause"),
				 errhint("Any aggregates in a HAVING clause need to be partialized in the output "
						 "target list.")));

	partialize_agg_paths(output_rel);
	return true;
}
