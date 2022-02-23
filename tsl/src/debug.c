/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */

/*
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

#include <access/printtup.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pg_list.h>
#include <optimizer/clauses.h>
#include <parser/parsetree.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/varlena.h>
#include <nodes/extensible.h>

#include <compat/compat.h>
#include "fdw/relinfo.h"
#include "fdw/fdw_utils.h"
#include "debug.h"
#include "utils.h"

static void append_expr(StringInfo buf, const Node *expr, const List *rtable);
static void tsl_debug_append_pathlist(StringInfo buf, PlannerInfo *root, List *pathlist, int indent,
									  bool isconsidered);

static const char *reloptkind_name[] = {
	[RELOPT_BASEREL] = "BASEREL",
	[RELOPT_JOINREL] = "JOINREL",
	[RELOPT_OTHER_MEMBER_REL] = "OTHER_MEMBER_REL",
	[RELOPT_OTHER_JOINREL] = "OTHER_JOINREL",
	[RELOPT_UPPER_REL] = "UPPER_REL",
	[RELOPT_OTHER_UPPER_REL] = "OTHER_UPPER_REL",
	[RELOPT_DEADREL] = "DEADREL",
};

/* clang-format off */
static const char *upperrel_stage_name[] = {
	[UPPERREL_SETOP] = "SETOP",
	[UPPERREL_PARTIAL_GROUP_AGG] = "PARTIAL_GROUP_AGG",
	[UPPERREL_GROUP_AGG] = "GROUP_AGG",
	[UPPERREL_WINDOW] = "WINDOW",
	[UPPERREL_DISTINCT] = "DISTINCT",
	[UPPERREL_ORDERED] = "ORDERED",
	[UPPERREL_FINAL] = "FINAL",
};
/* clang-format on */

static const char *fdw_rel_type_names[] = {
	[TS_FDW_RELINFO_HYPERTABLE_DATA_NODE] = "DATA_NODE",
	[TS_FDW_RELINFO_HYPERTABLE] = "HYPERTABLE",
	[TS_FDW_RELINFO_FOREIGN_TABLE] = "FOREIGN_TABLE",
};

static void
append_var_expr(StringInfo buf, const Node *expr, const List *rtable)
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
append_const_expr(StringInfo buf, const Node *expr, const List *rtable)
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
append_op_expr(StringInfo buf, const Node *expr, const List *rtable)
{
	const OpExpr *e = (const OpExpr *) expr;
	char *opname = get_opname(e->opno);
	if (list_length(e->args) > 1)
	{
		append_expr(buf, get_leftop((const Expr *) e), rtable);
		appendStringInfo(buf, " %s ", ((opname != NULL) ? opname : "(invalid operator)"));
		append_expr(buf, get_rightop((const Expr *) e), rtable);
	}
	else
	{
		appendStringInfo(buf, "%s ", ((opname != NULL) ? opname : "(invalid operator)"));
		append_expr(buf, get_leftop((const Expr *) e), rtable);
	}
}

static void
append_func_expr(StringInfo buf, const Node *expr, const List *rtable)
{
	const FuncExpr *e = (const FuncExpr *) expr;
	char *funcname = get_func_name(e->funcid);
	ListCell *l;

	appendStringInfo(buf, "%s(", ((funcname != NULL) ? funcname : "(invalid function)"));
	foreach (l, e->args)
	{
		append_expr(buf, lfirst(l), rtable);
		if (lnext_compat(e->args, l))
			appendStringInfoString(buf, ", ");
	}
	appendStringInfoChar(buf, ')');
}

static void
append_expr(StringInfo buf, const Node *expr, const List *rtable)
{
	if (expr == NULL)
	{
		appendStringInfo(buf, "<>");
		return;
	}

	switch (nodeTag(expr))
	{
		case T_Var:
			append_var_expr(buf, expr, rtable);
			break;

		case T_Const:
			append_const_expr(buf, expr, rtable);
			break;

		case T_OpExpr:
			append_op_expr(buf, expr, rtable);
			break;

		case T_FuncExpr:
			append_func_expr(buf, expr, rtable);
			break;

		default:
			appendStringInfo(buf, "unknown expr");
			break;
	}
}

static void
append_restrict_clauses(StringInfo buf, PlannerInfo *root, List *clauses)
{
	ListCell *cell;

	foreach (cell, clauses)
	{
		RestrictInfo *c = lfirst(cell);

		append_expr(buf, (Node *) c->clause, root->parse->rtable);
		if (lnext_compat(clauses, cell))
			appendStringInfoString(buf, ", ");
	}
}

static void
append_relids(StringInfo buf, PlannerInfo *root, Relids relids)
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

static void
append_pathkeys(StringInfo buf, const List *pathkeys, const List *rtable)
{
	const ListCell *i;

	appendStringInfoChar(buf, '(');
	foreach (i, pathkeys)
	{
		PathKey *pathkey = (PathKey *) lfirst(i);
		EquivalenceClass *eclass;
		ListCell *k;
		bool first = true;

		eclass = pathkey->pk_eclass;
		/* chase up, in case pathkey is non-canonical */
		while (eclass->ec_merged)
			eclass = eclass->ec_merged;

		appendStringInfoChar(buf, '(');
		foreach (k, eclass->ec_members)
		{
			EquivalenceMember *mem = (EquivalenceMember *) lfirst(k);

			if (first)
				first = false;
			else
				appendStringInfoString(buf, ", ");
			append_expr(buf, (Node *) mem->em_expr, rtable);
		}
		appendStringInfoChar(buf, ')');
		if (lnext_compat(pathkeys, i))
			appendStringInfoString(buf, ", ");
	}
	appendStringInfoChar(buf, ')');
}

/*
 * Return a relation's name.
 *
 * This function guarantees the return of a valid name string for a
 * relation. For relations that have no unique name we return "-".
 */
static const char *
get_relation_name(PlannerInfo *root, RelOptInfo *rel)
{
	TsFdwRelInfo *fdw_info = fdw_relinfo_get(rel);

	if (NULL != fdw_info)
		return fdw_info->relation_name->data;

	if (rel->reloptkind == RELOPT_BASEREL)
	{
		RangeTblEntry *rte = planner_rt_fetch(rel->relid, root);

		return get_rel_name(rte->relid);
	}

	return "-";
}

/*
 * Return a string name for the FDW type of a relation.
 *
 * For relations that are not an FDW relation we simply return "-".
 */
static const char *
get_fdw_relation_typename(RelOptInfo *rel)
{
	TsFdwRelInfo *fdw_info = fdw_relinfo_get(rel);

	if (NULL != fdw_info)
		return fdw_rel_type_names[fdw_info->type];

	return "-";
}

static void
tsl_debug_append_path(StringInfo buf, PlannerInfo *root, Path *path, int indent)
{
	const char *ptype;
	bool join = false;
	Path *subpath = NULL;
	List *subpath_list = NULL;
	int i;

	ptype = ts_get_node_name((Node *) path);
	switch (nodeTag(path))
	{
		case T_SubqueryScanPath:
			subpath = castNode(SubqueryScanPath, path)->subpath;
			break;
		case T_CustomPath:
			subpath_list = castNode(CustomPath, path)->custom_paths;
			break;
		case T_NestPath:
		case T_MergePath:
		case T_HashPath:
			join = true;
			break;
		case T_AppendPath:
			subpath_list = castNode(AppendPath, path)->subpaths;
			break;
		case T_MergeAppendPath:
			subpath_list = castNode(MergeAppendPath, path)->subpaths;
			break;
		case T_MaterialPath:
			subpath = castNode(MaterialPath, path)->subpath;
			break;
		case T_UniquePath:
			subpath = castNode(UniquePath, path)->subpath;
			break;
		case T_GatherPath:
			subpath = castNode(GatherPath, path)->subpath;
			break;
		case T_GatherMergePath:
			subpath = castNode(GatherMergePath, path)->subpath;
			break;
		case T_ProjectionPath:
			subpath = castNode(ProjectionPath, path)->subpath;
			break;
		case T_ProjectSetPath:
			subpath = castNode(ProjectSetPath, path)->subpath;
			break;
		case T_SortPath:
			subpath = castNode(SortPath, path)->subpath;
			break;
		case T_GroupPath:
			subpath = castNode(GroupPath, path)->subpath;
			break;
		case T_UpperUniquePath:
			subpath = castNode(UpperUniquePath, path)->subpath;
			break;
		case T_AggPath:
			subpath = castNode(AggPath, path)->subpath;
			break;
		case T_GroupingSetsPath:
			subpath = castNode(GroupingSetsPath, path)->subpath;
			break;
		case T_MinMaxAggPath:
			break;
		case T_WindowAggPath:
			subpath = castNode(WindowAggPath, path)->subpath;
			break;
		case T_SetOpPath:
			subpath = castNode(SetOpPath, path)->subpath;
			break;
		case T_RecursiveUnionPath:
			break;
		case T_LockRowsPath:
			subpath = castNode(LockRowsPath, path)->subpath;
			break;
		case T_ModifyTablePath:
#if PG14_LT
			subpath_list = castNode(ModifyTablePath, path)->subpaths;
#else
			subpath_list = list_make1(castNode(ModifyTablePath, path)->subpath);
#endif
			break;
		case T_LimitPath:
			subpath = castNode(LimitPath, path)->subpath;
			break;
		default:
			break;
	}

	for (i = 0; i < indent; i++)
		appendStringInfo(buf, "\t");
	appendStringInfo(buf, "%s", ptype);

	if (path->parent)
	{
		appendStringInfo(buf,
						 " [rel type: %s, kind: %s",
						 get_fdw_relation_typename(path->parent),
						 reloptkind_name[path->parent->reloptkind]);
		appendStringInfoString(buf, ", parent's base rels: ");
		append_relids(buf, root, path->parent->relids);
		appendStringInfoChar(buf, ']');
	}

	if (path->param_info)
	{
		appendStringInfoString(buf, " required_outer (");
		append_relids(buf, root, path->param_info->ppi_req_outer);
		appendStringInfoChar(buf, ')');
	}

	appendStringInfo(buf, " rows=%.0f", path->rows);

	if (path->pathkeys)
	{
		appendStringInfoString(buf, " with pathkeys: ");
		append_pathkeys(buf, path->pathkeys, root->parse->rtable);
	}

	appendStringInfoString(buf, "\n");

	if (join)
	{
		JoinPath *jp = (JoinPath *) path;

		for (i = 0; i < indent; i++)
			appendStringInfoString(buf, "\t");
		appendStringInfoString(buf, "  clauses: ");
		append_restrict_clauses(buf, root, jp->joinrestrictinfo);
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
		tsl_debug_append_pathlist(buf, root, subpath_list, indent + 1, false);
}

static void
tsl_debug_append_pathlist(StringInfo buf, PlannerInfo *root, List *pathlist, int indent,
						  bool isconsidered)
{
	ListCell *cell;
	foreach (cell, pathlist)
	{
		Path *path = isconsidered ? ((ConsideredPath *) lfirst(cell))->path : lfirst(cell);
		tsl_debug_append_path(buf, root, path, indent);
	}
}

/*
 * Check whether a path is the origin of a considered path.
 *
 * It is not possible to do a simple memcmp() of paths here because a path
 * could be a (semi-)shallow copy. Therefore we use the origin of the
 * ConsideredPath object.
 */
static bool
path_is_origin(const Path *p1, const ConsideredPath *p2)
{
	return p2->origin == (uintptr_t) p1;
}

/*
 * Print paths that were pruned during planning.
 *
 * The pruned paths are those that have been considered but are not in the
 * rel's pathlist.
 */
static void
tsl_debug_append_pruned_pathlist(StringInfo buf, PlannerInfo *root, RelOptInfo *rel, int indent)
{
	TsFdwRelInfo *fdw_info = fdw_relinfo_get(rel);
	ListCell *lc1;

	if (NULL == fdw_info || fdw_info->considered_paths == NIL)
		return;

	foreach (lc1, rel->pathlist)
	{
		Path *p1 = (Path *) lfirst(lc1);
		ListCell *lc2;
#if PG13_LT
		ListCell *prev = NULL;
#endif

		foreach (lc2, fdw_info->considered_paths)
		{
			ConsideredPath *p2 = (ConsideredPath *) lfirst(lc2);

			if (path_is_origin(p1, p2))
			{
				fdw_info->considered_paths =
					list_delete_cell_compat(fdw_info->considered_paths, lc2, prev);
				fdw_utils_free_path(p2);
				break;
			}
#if PG13_LT
			prev = lc2;
#endif
		}
	}

	if (fdw_info->considered_paths == NIL)
		return;

	appendStringInfoString(buf, "Pruned paths:\n");
	tsl_debug_append_pathlist(buf, root, fdw_info->considered_paths, indent, true);

	foreach (lc1, fdw_info->considered_paths)
		fdw_utils_free_path(lfirst(lc1));

	fdw_info->considered_paths = NIL;
}

void
tsl_debug_log_rel_with_paths(PlannerInfo *root, RelOptInfo *rel, UpperRelationKind *upper_stage)
{
	StringInfo buf = makeStringInfo();

	if (upper_stage != NULL)
		appendStringInfo(buf, "Upper rel stage %s:\n", upperrel_stage_name[*upper_stage]);

	appendStringInfo(buf,
					 "RELOPTINFO [rel name: %s, type: %s, kind: %s, base rel names: ",
					 get_relation_name(root, rel),
					 get_fdw_relation_typename(rel),
					 reloptkind_name[rel->reloptkind]);
	append_relids(buf, root, rel->relids);
	appendStringInfoChar(buf, ']');
	appendStringInfo(buf, " rows=%.0f width=%d\n", rel->rows, rel->reltarget->width);

	appendStringInfoString(buf, "Path list:\n");
	tsl_debug_append_pathlist(buf, root, rel->pathlist, 1, false);
	tsl_debug_append_pruned_pathlist(buf, root, rel, 1);

	if (rel->cheapest_parameterized_paths)
	{
		appendStringInfoString(buf, "\nCheapest parameterized paths:\n");
		tsl_debug_append_pathlist(buf, root, rel->cheapest_parameterized_paths, 1, false);
	}

	if (rel->cheapest_startup_path)
	{
		appendStringInfoString(buf, "\nCheapest startup path:\n");
		tsl_debug_append_path(buf, root, rel->cheapest_startup_path, 1);
	}

	if (rel->cheapest_total_path)
	{
		appendStringInfoString(buf, "\nCheapest total path:\n");
		tsl_debug_append_path(buf, root, rel->cheapest_total_path, 1);
	}

	appendStringInfoString(buf, "\n");
	ereport(DEBUG2, (errmsg_internal("%s", buf->data)));
}
