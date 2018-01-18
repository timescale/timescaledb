#include <postgres.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/readfuncs.h>
#include <utils/rel.h>
#include <catalog/pg_type.h>
#include <rewrite/rewriteManip.h>

#include "chunk_dispatch_plan.h"
#include "chunk_dispatch_state.h"
#include "chunk_dispatch_info.h"

/*
 * Create a ChunkDispatchState node from this plan. This is the full execution
 * state that replaces the plan node as the plan moves from planning to
 * execution.
 */
static Node *
create_chunk_dispatch_state(CustomScan *cscan)
{
	return (Node *) chunk_dispatch_state_create(linitial(cscan->custom_private),
												linitial(cscan->custom_plans));
}

static CustomScanMethods chunk_dispatch_plan_methods = {
	.CustomName = "ChunkDispatch",
	.CreateCustomScanState = create_chunk_dispatch_state,
};

/*
 * Build a target list for a CustomScan node.
 *
 * The CustomScan node needs to provide a modified target list in case of
 * subplan nodes that have target lists with expressions that involve, e.g.,
 * aggregates. Such target lists need a matching parent node type (e.g., a
 * target list with an AggDef needs a Agg node parent). If we simply use the
 * subplan's target list on a CustomScan parent node, there might be a mismatch.
 *
 * The code here is similar to ExecCheckPlanOutput() in nodeModifyTable.c in the
 * PostgreSQL source code, but here we construct a modified target list for the
 * CustomScan node instead of simply just checking validity.
 *
 */
static List *
build_customscan_targetlist(Relation rel, List *targetlist)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	List	   *new_targetlist = NIL;
	ListCell   *lc;
	int			attno = 0;

	foreach(lc, targetlist)
	{
		TargetEntry *te = lfirst(lc);
		Form_pg_attribute attr;
		Expr	   *expr;

		/* Ignore junk items */
		if (te->resjunk)
			continue;

		if (attno >= tupdesc->natts)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("table row type and query-specified row type do not match"),
					 errdetail("Query has too many columns.")));

		attr = tupdesc->attrs[attno++];

		if (attr->attisdropped)
			expr = &makeConst(INT4OID,
							  -1,
							  InvalidOid,
							  sizeof(int32),
							  (Datum) 0,
							  true,
							  true)->xpr;
		else

			/*
			 * According to nodes/primnodes.h, the INDEX_VAR Var type is
			 * abused in CustomScan nodes to reference custom scan tuple
			 * types.
			 */
			expr = &makeVar(INDEX_VAR,
							attno,
							exprType((Node *) te->expr),
							exprTypmod((Node *) te->expr),
							exprCollation((Node *) te->expr),
							0)->xpr;

		new_targetlist = lappend(new_targetlist,
								 makeTargetEntry(expr,
												 attno,
												 NULL,
												 te->resjunk));
	}

	if (attno != tupdesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("table row type and query-specified row type do not match"),
				 errdetail("Query has too few columns.")));

	return new_targetlist;
}

/* Create a chunk dispatch plan node in the form of a CustomScan node. The
 * purpose of this plan node is to dispatch (route) tuples to the correct chunk
 * in a hypertable.
 *
 * Note that CustomScan nodes cannot be extended (by struct embedding) because
 * they might be copied, therefore we pass any extra info as a ChunkDispatchInfo
 * in the custom_private field.
 *
 * The chunk dispatch plan takes the original tuple-producing subplan, which was
 * part of a ModifyTable node, and imposes itself inbetween the ModifyTable plan
 * and the subplan. During execution, the subplan will produce the new tuples
 * that the chunk dispatch node routes before passing them up to the ModifyTable
 * node.
 */
CustomScan *
chunk_dispatch_plan_create(Plan *subplan, Index hypertable_rti, Oid hypertable_relid, Query *parse)
{
	CustomScan *cscan = makeNode(CustomScan);
	ChunkDispatchInfo *info = chunk_dispatch_info_create(hypertable_relid, parse);
	Relation	rel;

	cscan->custom_private = list_make1(info);
	cscan->methods = &chunk_dispatch_plan_methods;
	cscan->custom_plans = list_make1(subplan);
	cscan->scan.scanrelid = 0;	/* Indicate this is not a real relation we are
								 * scanning */

	/* Copy costs from the original plan */
	cscan->scan.plan.startup_cost = subplan->startup_cost;
	cscan->scan.plan.total_cost = subplan->total_cost;
	cscan->scan.plan.plan_rows = subplan->plan_rows;
	cscan->scan.plan.plan_width = subplan->plan_width;

	/*
	 * Create a modified target list for the CustomScan based on the subplan's
	 * original target list
	 */
	rel = relation_open(hypertable_relid, AccessShareLock);
	cscan->scan.plan.targetlist = build_customscan_targetlist(rel, subplan->targetlist);
	RelationClose(rel);

	/*
	 * We need to set a custom_scan_tlist for EXPLAIN (verbose). But this
	 * target list needs to reference the proper rangetable entry so we must
	 * replace all INDEX_VAR attnos with the hypertable's rangetable index.
	 */
	cscan->custom_scan_tlist = copyObject(cscan->scan.plan.targetlist);
	ChangeVarNodes((Node *) cscan->custom_scan_tlist, INDEX_VAR, hypertable_rti, 0);

	return cscan;
}
