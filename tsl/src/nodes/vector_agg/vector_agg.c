/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>

#include <commands/explain.h>
#include <executor/executor.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>

#include "vector_agg.h"

#include "nodes/decompress_chunk/compressed_batch.h"
#include "nodes/decompress_chunk/exec.h"

static void
vector_agg_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = castNode(CustomScan, node->ss.ps.plan);
	node->custom_ps =
		lappend(node->custom_ps, ExecInitNode(linitial(cscan->custom_plans), estate, eflags));
}

static void
vector_agg_end(CustomScanState *node)
{
	ExecEndNode(linitial(node->custom_ps));
}

static void
vector_agg_rescan(CustomScanState *node)
{
	if (node->ss.ps.chgParam != NULL)
		UpdateChangedParamSet(linitial(node->custom_ps), node->ss.ps.chgParam);

	ExecReScan(linitial(node->custom_ps));
}

static TupleTableSlot *
vector_agg_exec(CustomScanState *node)
{
	// return ExecProcNode(linitial(node->custom_ps));
	DecompressChunkState *ds = (DecompressChunkState *) linitial(node->custom_ps);
	return decompress_chunk_exec_vector_agg_impl(node,
												 castNode(CustomScan, node->ss.ps.plan)
													 ->custom_scan_tlist,
												 ds);
}

static void
vector_agg_explain(CustomScanState *node, List *ancestors, ExplainState *es)
{
	/* noop? */
}

static struct CustomExecMethods exec_methods = {
	.CustomName = "VectorAgg",
	.BeginCustomScan = vector_agg_begin,
	.ExecCustomScan = vector_agg_exec,
	.EndCustomScan = vector_agg_end,
	.ReScanCustomScan = vector_agg_rescan,
	.ExplainCustomScan = vector_agg_explain,
};

static struct CustomScanMethods scan_methods = { .CustomName = "VectorAgg",
												 .CreateCustomScanState = vector_agg_state_create };

static inline List *
CustomBuildTargetList(List *tlist, Index varNo)
{
	List *result_tlist = NIL;
	ListCell *lc;

	foreach (lc, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);

		Var *var = makeVar(varNo,
						   tle->resno,
						   exprType((Node *) tle->expr),
						   exprTypmod((Node *) tle->expr),
						   exprCollation((Node *) tle->expr),
						   0);

		TargetEntry *newtle = makeTargetEntry((Expr *) var, tle->resno, tle->resname, tle->resjunk);

		result_tlist = lappend(result_tlist, newtle);
	}

	return result_tlist;
}

static Node *
replace_special_vars_mutator(Node *node, void *context)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (!IsA(node, Var))
	{
		return expression_tree_mutator(node, replace_special_vars_mutator, context);
	}

	Var *var = castNode(Var, node);
	if (var->varno != OUTER_VAR)
	{
		return node;
	}

	var = copyObject(var);
	var->varno = DatumGetInt32(PointerGetDatum(context));
	return (Node *) var;
}

static List *
replace_special_vars(List *input, int target_varno)
{
	return castNode(List,
					replace_special_vars_mutator((Node *) input,
												 DatumGetPointer(Int32GetDatum(target_varno))));
}

Plan *
vector_agg_plan_create(Agg *agg, CustomScan *decompress_chunk)
{
	CustomScan *custom = (CustomScan *) makeNode(CustomScan);
	custom->custom_plans = list_make1(decompress_chunk);
	custom->methods = &scan_methods;
	custom->scan.plan.targetlist = CustomBuildTargetList(agg->plan.targetlist, INDEX_VAR);
	// custom->scan.plan.targetlist = replace_special_vars(agg->plan.targetlist);
	//  custom->scan.plan.targetlist = agg->plan.targetlist;
	//	fprintf(stderr, "source agg tagetlist:\n");
	//	my_print(agg->plan.targetlist);
	//	fprintf(stderr, "build targetlist:\n");
	//	my_print(custom->scan.plan.targetlist);
	//   custom->custom_scan_tlist = CustomBuildTargetList(decompress_chunk->scan.plan.targetlist,
	//   INDEX_VAR);
	//   custom->custom_scan_tlist = decompress_chunk->scan.plan.targetlist;
	// custom->custom_scan_tlist = custom->scan.plan.targetlist;
	custom->custom_scan_tlist =
		replace_special_vars(agg->plan.targetlist, decompress_chunk->scan.scanrelid);

	// custom->scan.plan.lefttree = agg->plan.lefttree;

//	fprintf(stderr, "created:\n");
//	my_print(custom);

	(void) CustomBuildTargetList;

	return (Plan *) custom;
}

Node *
vector_agg_state_create(CustomScan *cscan)
{
	CustomScanState *state = makeNode(CustomScanState);
	state->methods = &exec_methods;
	return (Node *) state;
}

void
_vector_agg_init(void)
{
	TryRegisterCustomScanMethods(&scan_methods);
}
