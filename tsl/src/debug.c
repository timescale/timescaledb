/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 *
 * The code is partially copied from nodes/print.c and
 * backend/optimizer/path/allpaths.c in the PostgreSQL source code, but we
 * cannot use it out of the box for two reasons:
 *
 * The first reason is that the PostgreSQL code prints to standard output
 * (hence to the log) and we want to build a string buffer to send back in a
 * notice, we cannot use the functions as they are but have re-implement them.
 *
 * We want to send back paths and plans in a notice to the client, to make it
 * possible to interactively investigate what paths and plans that queries
 * generate without having to access the log.
 *
 * The second reason is that the PostgreSQL code is not aware of our custom
 * nodes and the hierarchy below them, so we need to have special handling of
 * custom nodes to get out more information.
 *
 * (A third reason is that the printing functions are incomplete and do not
 * print items below certain nodes, such as Append and MergeAppend, and we are
 * using them for our purposes and need to have more information about
 * subpaths than what PostgreSQL prints.)
 */

#include <postgres.h>
#include <foreign/fdwapi.h>

#include "compat.h"
#include <access/printtup.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/clauses.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#if PG11_GE
#include <utils/varlena.h>
#endif
#include <nodes/extensible.h>

#include "debug.h"

static void appendExpr(StringInfo buf, const Node *expr, const List *rtable);

static const char *reloptkind_name[] = {
	[RELOPT_BASEREL] = "BASEREL",
	[RELOPT_JOINREL] = "JOINREL",
	[RELOPT_OTHER_MEMBER_REL] = "OTHER_MEMBER_REL",
#if PG11_GE
	[RELOPT_OTHER_JOINREL] = "OTHER_JOINREL",
#endif
	[RELOPT_UPPER_REL] = "UPPER_REL",
#if PG11_GE
	[RELOPT_OTHER_UPPER_REL] = "OTHER_UPPER_REL",
#endif
	[RELOPT_DEADREL] = "DEADREL",
};

const char *upperrel_stage_name[] = {
	[UPPERREL_SETOP] = "SETOP",
#if PG11_GE
	[UPPERREL_PARTIAL_GROUP_AGG] = "PARTIAL_GROUP_AGG",
#endif
	[UPPERREL_GROUP_AGG] = "GROUP_AGG",
	[UPPERREL_WINDOW] = "WINDOW",
	[UPPERREL_DISTINCT] = "DISTINCT",
	[UPPERREL_ORDERED] = "ORDERED",
	[UPPERREL_FINAL] = "FINAL",
};

static void
appendVarExpr(StringInfo buf, const Node *expr, const List *rtable)
{
	const Var *var = (const Var *) expr;
	char *relname, *attname;

	switch (var->varno)
	{
		case INNER_VAR:
			relname = "INNER";
			attname = "?";
			break;
		case OUTER_VAR:
			relname = "OUTER";
			attname = "?";
			break;
		case INDEX_VAR:
			relname = "INDEX";
			attname = "?";
			break;
		default:
		{
			RangeTblEntry *rte;

			Assert(var->varno > 0 && (int) var->varno <= list_length(rtable));
			rte = rt_fetch(var->varno, rtable);
			relname = rte->eref->aliasname;
			attname = get_rte_attribute_name(rte, var->varattno);
		}
		break;
	}
	appendStringInfo(buf, "%s.%s", relname, attname);
}

static void
appendConstExpr(StringInfo buf, const Node *expr, const List *rtable)
{
	const Const *c = (const Const *) expr;
	Oid typoutput;
	bool typIsVarlena;
	char *outputstr;

	if (c->constisnull)
	{
		appendStringInfo(buf, "NULL");
		return;
	}

	getTypeOutputInfo(c->consttype, &typoutput, &typIsVarlena);

	outputstr = OidOutputFunctionCall(typoutput, c->constvalue);
	appendStringInfo(buf, "%s", outputstr);
	pfree(outputstr);
}

static void
appendOpExpr(StringInfo buf, const Node *expr, const List *rtable)
{
	const OpExpr *e = (const OpExpr *) expr;
	char *opname = get_opname(e->opno);
	if (list_length(e->args) > 1)
	{
		appendExpr(buf, get_leftop((const Expr *) e), rtable);
		appendStringInfo(buf, " %s ", ((opname != NULL) ? opname : "(invalid operator)"));
		appendExpr(buf, get_rightop((const Expr *) e), rtable);
	}
	else
	{
		appendStringInfo(buf, "%s ", ((opname != NULL) ? opname : "(invalid operator)"));
		appendExpr(buf, get_leftop((const Expr *) e), rtable);
	}
}

static void
appendFuncExpr(StringInfo buf, const Node *expr, const List *rtable)
{
	const FuncExpr *e = (const FuncExpr *) expr;
	char *funcname = get_func_name(e->funcid);
	ListCell *l;

	appendStringInfo(buf, "%s(", ((funcname != NULL) ? funcname : "(invalid function)"));
	foreach (l, e->args)
	{
		appendExpr(buf, lfirst(l), rtable);
		if (lnext(l))
			appendStringInfoString(buf, ", ");
	}
	appendStringInfoChar(buf, ')');
}

static void
appendExpr(StringInfo buf, const Node *expr, const List *rtable)
{
	if (expr == NULL)
	{
		appendStringInfo(buf, "<>");
		return;
	}

	switch (nodeTag(expr))
	{
		case T_Var:
			appendVarExpr(buf, expr, rtable);
			break;

		case T_Const:
			appendConstExpr(buf, expr, rtable);
			break;

		case T_OpExpr:
			appendOpExpr(buf, expr, rtable);
			break;

		case T_FuncExpr:
			appendFuncExpr(buf, expr, rtable);
			break;

		default:
			appendStringInfo(buf, "unknown expr");
			break;
	}
}

static void
appendRestrictClauses(StringInfo buf, PlannerInfo *root, List *clauses)
{
	ListCell *cell;

	foreach (cell, clauses)
	{
		RestrictInfo *c = lfirst(cell);

		appendExpr(buf, (Node *) c->clause, root->parse->rtable);
		if (lnext(cell))
			appendStringInfoString(buf, ", ");
	}
}

static void
appendRelids(StringInfo buf, PlannerInfo *root, Relids relids)
{
	int x = -1;
	bool first = true;

	while ((x = bms_next_member(relids, x)) >= 0)
	{
		if (!first)
			appendStringInfoChar(buf, ' ');
		if (x < root->simple_rel_array_size && root->simple_rte_array[x])
			appendStringInfo(buf, "%s", root->simple_rte_array[x]->eref->aliasname);
		else
			appendStringInfo(buf, "%d", x);
		first = false;
	}
}

void
tsl_debug_append_path(StringInfo buf, PlannerInfo *root, Path *path, int indent)
{
	const char *ptype;
	const char *extra_info = NULL;
	bool join = false;
	Path *subpath = NULL;
	List *subpath_list = NULL;
	int i;

	switch (nodeTag(path))
	{
		case T_Path:
			switch (path->pathtype)
			{
				case T_SeqScan:
					ptype = "SeqScan";
					break;
				case T_SampleScan:
					ptype = "SampleScan";
					break;
				case T_SubqueryScan:
					ptype = "SubqueryScan";
					break;
				case T_FunctionScan:
					ptype = "FunctionScan";
					break;
#if PG11_GE
				case T_TableFuncScan:
					ptype = "TableFuncScan";
					break;
#endif
				case T_ValuesScan:
					ptype = "ValuesScan";
					break;
				case T_CteScan:
					ptype = "CteScan";
					break;
				case T_WorkTableScan:
					ptype = "WorkTableScan";
					break;
				default:
					ptype = "???Path";
					break;
			}
			break;
		case T_IndexPath:
			ptype = "IdxScan";
			break;
		case T_BitmapHeapPath:
			ptype = "BitmapHeapScan";
			break;
		case T_BitmapAndPath:
			ptype = "BitmapAndPath";
			break;
		case T_BitmapOrPath:
			ptype = "BitmapOrPath";
			break;
		case T_TidPath:
			ptype = "TidScan";
			break;
		case T_SubqueryScanPath:
			ptype = "SubqueryScanScan";
			subpath = castNode(SubqueryScanPath, path)->subpath;
			break;
		case T_ForeignPath:
			ptype = "ForeignScan";
			break;
		case T_CustomPath:
			ptype = "CustomScan";
			subpath_list = castNode(CustomPath, path)->custom_paths;
			extra_info = castNode(CustomPath, path)->methods->CustomName;
			break;
		case T_NestPath:
			ptype = "NestLoop";
			join = true;
			break;
		case T_MergePath:
			ptype = "MergeJoin";
			join = true;
			break;
		case T_HashPath:
			ptype = "HashJoin";
			join = true;
			break;
		case T_AppendPath:
			ptype = "Append";
			subpath_list = castNode(AppendPath, path)->subpaths;
			break;
		case T_MergeAppendPath:
			ptype = "MergeAppend";
			subpath_list = castNode(MergeAppendPath, path)->subpaths;
			break;
#if PG12_GE
		case T_GroupResultPath:
			ptype = "GroupResult";
			break;
#else
		case T_ResultPath:
			ptype = "Result";
			break;
#endif
		case T_MaterialPath:
			ptype = "Material";
			subpath = castNode(MaterialPath, path)->subpath;
			break;
		case T_UniquePath:
			ptype = "Unique";
			subpath = castNode(UniquePath, path)->subpath;
			break;
		case T_GatherPath:
			ptype = "Gather";
			subpath = castNode(GatherPath, path)->subpath;
			break;
#if PG11_GE
		case T_GatherMergePath:
			ptype = "GatherMerge";
			subpath = castNode(GatherMergePath, path)->subpath;
			break;
#endif
		case T_ProjectionPath:
			ptype = "Projection";
			subpath = castNode(ProjectionPath, path)->subpath;
			break;
#if PG11_GE
		case T_ProjectSetPath:
			ptype = "ProjectSet";
			subpath = castNode(ProjectSetPath, path)->subpath;
			break;
#endif
		case T_SortPath:
			ptype = "Sort";
			subpath = castNode(SortPath, path)->subpath;
			break;
		case T_GroupPath:
			ptype = "Group";
			subpath = castNode(GroupPath, path)->subpath;
			break;
		case T_UpperUniquePath:
			ptype = "UpperUnique";
			subpath = castNode(UpperUniquePath, path)->subpath;
			break;
		case T_AggPath:
			ptype = "Agg";
			subpath = castNode(AggPath, path)->subpath;
			break;
		case T_GroupingSetsPath:
			ptype = "GroupingSets";
			subpath = castNode(GroupingSetsPath, path)->subpath;
			break;
		case T_MinMaxAggPath:
			ptype = "MinMaxAgg";
			break;
		case T_WindowAggPath:
			ptype = "WindowAgg";
			subpath = castNode(WindowAggPath, path)->subpath;
			break;
		case T_SetOpPath:
			ptype = "SetOp";
			subpath = castNode(SetOpPath, path)->subpath;
			break;
		case T_RecursiveUnionPath:
			ptype = "RecursiveUnion";
			break;
		case T_LockRowsPath:
			ptype = "LockRows";
			subpath = castNode(LockRowsPath, path)->subpath;
			break;
		case T_ModifyTablePath:
			ptype = "ModifyTable";
			subpath_list = castNode(ModifyTablePath, path)->subpaths;
			break;
		case T_LimitPath:
			ptype = "Limit";
			subpath = castNode(LimitPath, path)->subpath;
			break;
		default:
			ptype = "???Path";
			break;
	}

	for (i = 0; i < indent; i++)
		appendStringInfo(buf, "\t");
	appendStringInfo(buf, "%s", ptype);
	if (extra_info)
		appendStringInfo(buf, " (%s)", extra_info);
	if (path->parent)
	{
		appendStringInfoString(buf, " [parents: ");
		appendRelids(buf, root, path->parent->relids);
		appendStringInfoString(buf, "]");
	}
	if (path->param_info)
	{
		appendStringInfoString(buf, " required_outer (");
		appendRelids(buf, root, path->param_info->ppi_req_outer);
		appendStringInfoString(buf, ")");
	}

	appendStringInfo(buf, " rows=%.0f", path->rows);

	if (path->pathkeys)
		appendStringInfoString(buf, " has pathkeys");

	appendStringInfoString(buf, "\n");

	if (join)
	{
		JoinPath *jp = (JoinPath *) path;

		for (i = 0; i < indent; i++)
			appendStringInfoString(buf, "\t");
		appendStringInfoString(buf, "  clauses: ");
		appendRestrictClauses(buf, root, jp->joinrestrictinfo);
		appendStringInfoString(buf, "\n");

		if (IsA(path, MergePath))
		{
			MergePath *mp = castNode(MergePath, path);

			for (i = 0; i < indent; i++)
				appendStringInfo(buf, "\t");
			appendStringInfo(buf,
							 "  sortouter=%d sortinner=%d materializeinner=%d\n",
							 ((mp->outersortkeys) ? 1 : 0),
							 ((mp->innersortkeys) ? 1 : 0),
							 ((mp->materialize_inner) ? 1 : 0));
		}

		tsl_debug_append_path(buf, root, jp->outerjoinpath, indent + 1);
		tsl_debug_append_path(buf, root, jp->innerjoinpath, indent + 1);
	}

	if (subpath)
		tsl_debug_append_path(buf, root, subpath, indent + 1);
	if (subpath_list)
		tsl_debug_append_pathlist(buf, root, subpath_list, indent + 1);
}

void
tsl_debug_append_pathlist(StringInfo buf, PlannerInfo *root, List *pathlist, int indent)
{
	ListCell *cell;
	foreach (cell, pathlist)
		tsl_debug_append_path(buf, root, lfirst(cell), indent);
}

void
tsl_debug_append_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel)
{
	appendStringInfo(buf, "RELOPTINFO [%s] (names: ", reloptkind_name[rel->reloptkind]);
	appendRelids(buf, root, rel->relids);
	appendStringInfo(buf, "): rows=%.0f width=%d\n", rel->rows, rel->reltarget->width);

	appendStringInfoString(buf, "\tpath list:\n");
	tsl_debug_append_pathlist(buf, root, rel->pathlist, 1);

	if (rel->cheapest_parameterized_paths)
	{
		appendStringInfoString(buf, "\n\tcheapest parameterized paths:\n");
		tsl_debug_append_pathlist(buf, root, rel->cheapest_parameterized_paths, 1);
	}

	if (rel->cheapest_startup_path)
	{
		appendStringInfoString(buf, "\n\tcheapest startup path:\n");
		tsl_debug_append_path(buf, root, rel->cheapest_startup_path, 1);
	}

	if (rel->cheapest_total_path)
	{
		appendStringInfoString(buf, "\n\tcheapest total path:\n");
		tsl_debug_append_path(buf, root, rel->cheapest_total_path, 1);
	}
	appendStringInfoString(buf, "\n");
}
