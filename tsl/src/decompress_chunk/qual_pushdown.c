/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/relation.h>
#include <optimizer/clauses.h>
#include <optimizer/pathnode.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <optimizer/var.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/typcache.h>

#include "compat.h"
#include "decompress_chunk/decompress_chunk.h"
#include "decompress_chunk/qual_pushdown.h"
#include "hypertable_compression.h"

typedef struct QualPushdownContext
{
	RelOptInfo *chunk_rel;
	RelOptInfo *compressed_rel;
	RangeTblEntry *chunk_rte;
	RangeTblEntry *compressed_rte;
	List *compression_info;
	bool can_pushdown;
} QualPushdownContext;

static bool adjust_expression(Node *node, QualPushdownContext *context);

void
pushdown_quals(PlannerInfo *root, RelOptInfo *chunk_rel, RelOptInfo *compressed_rel,
			   List *compression_info)
{
	ListCell *lc;
	QualPushdownContext context = {
		.chunk_rel = chunk_rel,
		.compressed_rel = compressed_rel,
		.chunk_rte = planner_rt_fetch(chunk_rel->relid, root),
		.compressed_rte = planner_rt_fetch(compressed_rel->relid, root),
		.compression_info = compression_info,
	};

	foreach (lc, chunk_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst(lc);
		Expr *expr = copyObject(ri->clause);
		context.can_pushdown = true;
		adjust_expression((Node *) expr, &context);
		if (context.can_pushdown)
		{
			compressed_rel->baserestrictinfo =
				lappend(compressed_rel->baserestrictinfo, make_simple_restrictinfo(expr));
		}
	}
}

static bool
adjust_expression(Node *node, QualPushdownContext *context)
{
	if (node == NULL)
		return false;

	switch (nodeTag(node))
	{
		case T_OpExpr:
		case T_ScalarArrayOpExpr:
		case T_List:
		case T_Const:
		case T_NullTest:
			break;
		case T_Var:
		{
			Var *var = castNode(Var, node);
			char *column_name;
			FormData_hypertable_compression *compressioninfo;
			AttrNumber compressed_attno;

			/* ignore system attibutes or whole row references */
			if (var->varattno <= 0)
			{
				context->can_pushdown = false;
				return true;
			}

			column_name = get_attname_compat(context->chunk_rte->relid, var->varattno, false);
			compressioninfo = get_column_compressioninfo(context->compression_info, column_name);

			/* we can only push down quals for segmentby columns */
			if (compressioninfo->segmentby_column_index <= 0)
			{
				context->can_pushdown = false;
				return true;
			}

			compressed_attno = get_attnum(context->compressed_rte->relid, column_name);
			var->varno = context->compressed_rel->relid;
			var->varattno = compressed_attno;

			break;
		}
		default:
			context->can_pushdown = false;
			return true;
			break;
	}

	return expression_tree_walker(node, adjust_expression, context);
}
