/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
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

static void pushdown_nulltest(DecompressChunkPath *path, NullTest *op);
static void pushdown_opexpr(DecompressChunkPath *path, OpExpr *op);
static void pushdown_scalararrayopexpr(DecompressChunkPath *path, ScalarArrayOpExpr *op);
static bool can_pushdown_var(DecompressChunkPath *path, Var *chunk_var, Var **compressed_var);

void
pushdown_quals(DecompressChunkPath *path)
{
	ListCell *lc;

	foreach (lc, path->chunk_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst(lc);

		switch (nodeTag(ri->clause))
		{
			case T_NullTest:
				pushdown_nulltest(path, castNode(NullTest, ri->clause));
				break;
			case T_OpExpr:
				pushdown_opexpr(path, castNode(OpExpr, ri->clause));
				break;
			case T_ScalarArrayOpExpr:
				pushdown_scalararrayopexpr(path, castNode(ScalarArrayOpExpr, ri->clause));
				break;
			default:
				break;
		}
	}
}

static bool
can_pushdown_var(DecompressChunkPath *path, Var *chunk_var, Var **compressed_var)
{
	char *column_name;
	FormData_hypertable_compression *compressioninfo;

	Assert(chunk_var->varno == path->chunk_rel->relid);

	/* ignore system attibutes or whole row references */
	if (!IsA(chunk_var, Var) || chunk_var->varattno <= 0)
		return false;

	column_name = get_attname_compat(path->chunk_rte->relid, chunk_var->varattno, false);
	compressioninfo = get_column_compressioninfo(path->compression_info, column_name);

	/* we can only push down quals for segmentby columns */
	if (compressioninfo->segmentby_column_index > 0)
	{
		AttrNumber compressed_attno = get_attnum(path->compressed_rte->relid, column_name);
		Var *var = copyObject(chunk_var);

		var->varno = path->compressed_rel->relid;
		var->varattno = compressed_attno;

		*compressed_var = var;

		return true;
	}

	return false;
}

static void
pushdown_nulltest(DecompressChunkPath *path, NullTest *op)
{
	Var *compressed_var;

	if (!IsA(op->arg, Var))
		return;

	if (can_pushdown_var(path, castNode(Var, op->arg), &compressed_var))
	{
		NullTest *compressed_op = copyObject(op);
		RestrictInfo *compressed_ri;
		compressed_op->arg = (Expr *) compressed_var;

		compressed_ri = make_simple_restrictinfo((Expr *) compressed_op);

		path->compressed_rel->baserestrictinfo =
			lappend(path->compressed_rel->baserestrictinfo, compressed_ri);
	}
}

static void
pushdown_opexpr(DecompressChunkPath *path, OpExpr *op)
{
	bool var_on_left = false;
	Expr *left, *right;

	if (list_length(op->args) != 2)
		return;

	left = linitial(op->args);
	right = lsecond(op->args);

	/* we only support Var OP Const / Const OP Var for now */
	if ((IsA(left, Var) && IsA(right, Const)) || (IsA(left, Const) && IsA(right, Var)))
	{
		Var *var, *compressed_var;

		if (IsA(left, Var))
			var_on_left = true;

		var = var_on_left ? (Var *) left : (Var *) right;

		/* we can only push down quals for segmentby columns */
		if (can_pushdown_var(path, var, &compressed_var))
		{
			OpExpr *compressed_op = copyObject(op);
			RestrictInfo *compressed_ri;

			if (var_on_left)
				compressed_op->args = list_make2(compressed_var, copyObject(right));
			else
				compressed_op->args = list_make2(copyObject(left), compressed_var);

			compressed_ri = make_simple_restrictinfo((Expr *) compressed_op);

			path->compressed_rel->baserestrictinfo =
				lappend(path->compressed_rel->baserestrictinfo, compressed_ri);
		}
	}
}

static void
pushdown_scalararrayopexpr(DecompressChunkPath *path, ScalarArrayOpExpr *op)
{
	Expr *left, *right;

	if (list_length(op->args) != 2)
		return;

	left = linitial(op->args);
	right = lsecond(op->args);

	if (IsA(left, Var) && IsA(right, Const))
	{
		Var *compressed_var;

		/* we can only push down quals for segmentby columns */
		if (can_pushdown_var(path, castNode(Var, left), &compressed_var))
		{
			ScalarArrayOpExpr *compressed_op = copyObject(op);
			RestrictInfo *compressed_ri;

			compressed_op->args = list_make2(compressed_var, copyObject(right));

			compressed_ri = make_simple_restrictinfo((Expr *) compressed_op);

			path->compressed_rel->baserestrictinfo =
				lappend(path->compressed_rel->baserestrictinfo, compressed_ri);
		}
	}
}
