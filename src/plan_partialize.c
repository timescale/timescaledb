/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/planner.h>
#include <utils/lsyscache.h>
#include "plan_partialize.h"
#include "extension_constants.h"
#include "utils.h"

#define TS_PARTIALFN "partialize_agg"
typedef struct PartializeWalkerState
{
	bool found_partialize;
	bool looking_for_agg;
	Oid fnoid;
} PartializeWalkerState;

static bool
partialize_function_call_walker(Node *node, PartializeWalkerState *state)
{
	if (node == NULL)
		return false;

	/*
	 * If the last node we saw was partialize, the next one must be aggregate
	 * we're partializing
	 */
	if (state->looking_for_agg)
	{
		Aggref *agg_ref;

		if (!IsA(node, Aggref))
			elog(ERROR, "The input to partialize must be an aggregate");

		agg_ref = castNode(Aggref, node);
		agg_ref->aggsplit = AGGSPLIT_INITIAL_SERIAL;
		if (agg_ref->aggtranstype == INTERNALOID && DO_AGGSPLIT_SERIALIZE(AGGSPLIT_INITIAL_SERIAL))
			agg_ref->aggtype = BYTEAOID;
		else
			agg_ref->aggtype = agg_ref->aggtranstype;

		state->looking_for_agg = false;
	}
	else if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == state->fnoid)
	{
		state->found_partialize = true;
		state->looking_for_agg = true;
	}

	return expression_tree_walker((Node *) node, partialize_function_call_walker, state);
}

/* We currently cannot handle cases like
 *     SELECT sum(i), partialize(sum(i)) ...
 * instead we use this function to ensure that if any of the aggregates in a statement are
 * partialized, all of them are
 */
static bool
ensure_only_partials(Node *node, void *state)
{
	if (node == NULL)
		return false;

	if (IsA(node, Aggref) && castNode(Aggref, node)->aggsplit != AGGSPLIT_INITIAL_SERIAL)
		elog(ERROR, "Cannot mix partialized and non-partialized aggregates in the same statement");

	return expression_tree_walker((Node *) node, ensure_only_partials, state);
}

void
plan_process_partialize_agg(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel)
{
	Oid partialfnoid = InvalidOid;
	Oid argtyp[] = { ANYELEMENTOID };
	Query *parse = root->parse;
	PartializeWalkerState state = { .found_partialize = false,
									.looking_for_agg = false,
									.fnoid = InvalidOid };
	ListCell *lc;

	if (CMD_SELECT != parse->commandType)
		return;
	partialfnoid = get_function_oid(TS_PARTIALFN, INTERNAL_SCHEMA_NAME, lengthof(argtyp), argtyp);
	Assert(partialfnoid != InvalidOid);

	state.fnoid = partialfnoid;
	partialize_function_call_walker((Node *) parse->targetList, &state);

	if (state.found_partialize)
	{
		ensure_only_partials((Node *) parse->targetList, NULL);

		foreach (lc, input_rel->pathlist)
		{
			Path *path = lfirst(lc);

			if (IsA(path, AggPath))
				((AggPath *) path)->aggsplit = AGGSPLIT_INITIAL_SERIAL;
		}

		foreach (lc, output_rel->pathlist)
		{
			Path *path = lfirst(lc);

			if (IsA(path, AggPath))
				((AggPath *) path)->aggsplit = AGGSPLIT_INITIAL_SERIAL;
		}
	}
}
