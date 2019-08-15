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

void
pushdown_quals(PlannerInfo *root, DecompressChunkPath *path)
{
	ListCell *lc;
	List *compressed_ris = NIL;

	foreach (lc, path->chunk_rel->baserestrictinfo)
	{
		RestrictInfo *ri = lfirst(lc);
		OpExpr *op = (OpExpr *) ri->clause;
		bool var_on_left;
		Expr *left, *right;

		if (!IsA(op, OpExpr) || list_length(op->args) != 2)
			continue;

		left = linitial(op->args);
		right = lsecond(op->args);

		/* we only support Var OP Const / Const OP Var for now */
		if ((IsA(left, Var) && IsA(right, Const)) || (IsA(left, Const) && IsA(right, Var)))
		{
			Var *var;
			char *column_name;
			FormData_hypertable_compression *compressioninfo;

			if (IsA(left, Var))
				var_on_left = true;

			var = var_on_left ? (Var *) left : (Var *) right;

			Assert(var->varno == path->chunk_rel->relid);
			/* ignore system attibutes or whole column references */
			if (var->varattno <= 0)
				continue;

			column_name = get_attname_compat(path->chunk_rte->relid, var->varattno, false);
			compressioninfo = get_column_compressioninfo(path->compression_info, column_name);

			/* we can only push down quals for segmentby columns */
			if (compressioninfo->segmentby_column_index > 0)
			{
				OpExpr *compressed_op = copyObject(op);
				AttrNumber compressed_attno = get_attnum(path->compressed_rte->relid, column_name);
				Var *compressed_var = copyObject(var);
				List *args;

				compressed_var->varno = path->compressed_rel->relid;
				compressed_var->varattno = compressed_attno;

				if (var_on_left)
					args = list_make2(compressed_var, copyObject(right));
				else
					args = list_make2(copyObject(left), compressed_var);

				compressed_op->args = args;

				compressed_ris =
					lappend(compressed_ris, make_simple_restrictinfo((Expr *) compressed_op));
			}
		}
	}
	path->compressed_rel->baserestrictinfo = compressed_ris;
}
