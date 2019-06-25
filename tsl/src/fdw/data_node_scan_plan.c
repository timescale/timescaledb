/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <parser/parsetree.h>
#include <nodes/plannodes.h>
#include <nodes/extensible.h>
#include <nodes/relation.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/paths.h>
#include <optimizer/pathnode.h>
#include <optimizer/prep.h>
#include <optimizer/clauses.h>
#include <optimizer/var.h>
#include <optimizer/tlist.h>
#include <optimizer/restrictinfo.h>
#include <access/sysattr.h>
#include <utils/memutils.h>
#include <foreign/fdwapi.h>

#include <export.h>
#include <chunk_data_node.h>
#include <hypertable_cache.h>
#include <planner.h>
#include <planner_import.h>
#include <func_cache.h>
#include <dimension.h>
#include <compat.h>

#include "relinfo.h"
#include "data_node_chunk_assignment.h"
#include "scan_plan.h"
#include "data_node_scan_plan.h"
#include "data_node_scan_exec.h"

/*
 * ServerScan is a custom scan implementation for scanning hypertables on
 * remote servers instead of scanning individual remote chunks.
 *
 * A ServerScan plan is created by taking a regular per-chunk scan plan and
 * then assigning each chunk to a server, and treating each server as a
 * "partition" of the distributed hypertable. For each resulting server, we
 * create a server rel which is essentially a base rel representing a remote
 * hypertable partition. Since we treat a server rel as a base rel, although
 * it has no corresponding server table, we point each server rel to the root
 * hypertable. This is conceptually the right thing to do, since each server
 * rel is a partition of the same distributed hypertable.
 *
 * For each server rel, we plan a ServerScan instead of a ForeignScan since a
 * server rel does not correspond to a real foreign table. A ForeignScan of a
 * server rel would fail when trying to lookup the ForeignServer via the
 * server rel's RTE relid. The only other option to get around the
 * ForeignTable lookup is to make a server rel an upper rel instead of a base
 * rel (see nodeForeignscan.c). However, that leads to other issues in
 * setrefs.c that messes up our target lists for some queries.
 */

static Path *server_scan_path_create(PlannerInfo *root, RelOptInfo *rel, PathTarget *target,
									 double rows, Cost startup_cost, Cost total_cost,
									 List *pathkeys, Relids required_outer, Path *fdw_outerpath,
									 List *private);

static AppendRelInfo *
create_append_rel_info(PlannerInfo *root, RelOptInfo *childrel, RelOptInfo *parentrel)
{
	RangeTblEntry *parent_rte = planner_rt_fetch(parentrel->relid, root);
	Relation relation = heap_open(parent_rte->relid, NoLock);
	AppendRelInfo *appinfo;

	appinfo = makeNode(AppendRelInfo);
	appinfo->parent_relid = parentrel->relid;
	appinfo->child_relid = childrel->relid;
	appinfo->parent_reltype = relation->rd_rel->reltype;
	appinfo->child_reltype = relation->rd_rel->reltype;
	ts_make_inh_translation_list(relation, relation, childrel->relid, &appinfo->translated_vars);
	appinfo->parent_reloid = parent_rte->relid;
	heap_close(relation, NoLock);

	return appinfo;
}

/*
 * Build a new RelOptInfo representing a server.
 *
 * Note that the relid index should point to the corresponding range table
 * entry (RTE) we created for the server rel when expanding the
 * hypertable. Each such RTE's relid (OID) refers to the hypertable's root
 * table. This has the upside that the planner can use the hypertable's
 * indexes to plan remote queries more efficiently. In contrast, chunks are
 * foreign tables and they cannot have indexes.
 */
static RelOptInfo *
build_server_rel(PlannerInfo *root, Index relid, Oid serverid, RelOptInfo *parent)
{
	RelOptInfo *rel = build_simple_rel(root, relid, parent);

	/* Use relevant exprs and restrictinfos from the parent rel. These will be
	 * adjusted to match the server rel's relid later. */
	rel->reltarget->exprs = copyObject(parent->reltarget->exprs);
	rel->baserestrictinfo = parent->baserestrictinfo;
	rel->baserestrictcost = parent->baserestrictcost;
	rel->baserestrict_min_security = parent->baserestrict_min_security;
	rel->lateral_vars = parent->lateral_vars;
	rel->lateral_referencers = parent->lateral_referencers;
	rel->lateral_relids = parent->lateral_relids;
	rel->serverid = serverid;

	/* We need to use the FDW interface to get called by the planner for
	 * partial aggs. For some reason, the standard upper_paths_hook is never
	 * called for upper rels of type UPPERREL_PARTIAL_GROUP_AGG, which is odd
	 * (see end of PostgreSQL planner.c:create_partial_grouping_paths). Until
	 * this gets fixed in the PostgreSQL planner, we're forced to set
	 * fdwroutine here although we will scan this rel with a ServerScan and
	 * not a ForeignScan. */
	rel->fdwroutine = GetFdwRoutineByServerId(serverid);

	return rel;
}

/*
 * Adjust the attributes of server rel quals.
 *
 * Code adapted from allpaths.c: set_append_rel_size.
 *
 * For each server child rel, copy the quals/restrictions from the parent
 * (hypertable) rel and adjust the attributes (e.g., Vars) to point to the
 * child rel instead of the parent.
 *
 * Normally, this happens as part of estimating the rel size of an append
 * relation in standard planning, where constraint exclusion and partition
 * pruning also happens for each child. Here, however, we don't prune any
 * server rels since they are created based on assignment of already pruned
 * chunk child rels at an earlier stage. Server rels that aren't assigned any
 * chunks will never be created in the first place.
 */
static void
adjust_server_rel_attrs(PlannerInfo *root, RelOptInfo *server_rel, RelOptInfo *hyper_rel,
						AppendRelInfo *appinfo)
{
	List *serverquals = NIL;
	ListCell *lc;

	foreach (lc, hyper_rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Node *serverqual;
		ListCell *lc2;

		serverqual = adjust_appendrel_attrs_compat(root, (Node *) rinfo->clause, appinfo);

		serverqual = eval_const_expressions(root, serverqual);

		/* might have gotten an AND clause, if so flatten it */
		foreach (lc2, make_ands_implicit((Expr *) serverqual))
		{
			Node *onecq = (Node *) lfirst(lc2);
			bool pseudoconstant;

			/* check for pseudoconstant (no Vars or volatile functions) */
			pseudoconstant = !contain_vars_of_level(onecq, 0) && !contain_volatile_functions(onecq);
			if (pseudoconstant)
			{
				/* tell createplan.c to check for gating quals */
				root->hasPseudoConstantQuals = true;
			}
			/* reconstitute RestrictInfo with appropriate properties */
			serverquals = lappend(serverquals,
								  make_restrictinfo((Expr *) onecq,
													rinfo->is_pushed_down,
													rinfo->outerjoin_delayed,
													pseudoconstant,
													rinfo->security_level,
													NULL,
													NULL,
													NULL));
		}
	}

	server_rel->baserestrictinfo = serverquals;
	server_rel->joininfo =
		(List *) adjust_appendrel_attrs_compat(root, (Node *) hyper_rel->joininfo, appinfo);

	server_rel->reltarget->exprs =
		(List *) adjust_appendrel_attrs_compat(root, (Node *) hyper_rel->reltarget->exprs, appinfo);

	/* Add equivalence class for rel to push down joins and sorts */
	if (hyper_rel->has_eclass_joins || has_useful_pathkeys(root, hyper_rel))
		add_child_rel_equivalences(root, appinfo, hyper_rel, server_rel);

	server_rel->has_eclass_joins = hyper_rel->has_eclass_joins;
}

/*
 * Build RelOptInfos for each server.
 *
 * Each server rel will point to the root hypertable table, which is
 * conceptually correct since we query the identical (partial) hypertables on
 * the servers.
 */
static RelOptInfo **
build_server_part_rels(PlannerInfo *root, RelOptInfo *hyper_rel, int *nparts)
{
	TimescaleDBPrivate *priv = hyper_rel->fdw_private;
	/* Update the partitioning to reflect the new per-server plan */
	RelOptInfo **part_rels = palloc(sizeof(RelOptInfo *) * list_length(priv->serverids));
	ListCell *lc;
	int n = 0;
	int i;

	Assert(list_length(priv->serverids) == bms_num_members(priv->server_relids));
	i = -1;

	foreach (lc, priv->serverids)
	{
		Oid serverid = lfirst_oid(lc);
		RelOptInfo *server_rel;
		AppendRelInfo *appinfo;

		i = bms_next_member(priv->server_relids, i);

		Assert(i > 0);

		server_rel = build_server_rel(root, i, serverid, hyper_rel);

		part_rels[n++] = server_rel;

		/* The planner expects an AppendRelInfo for any part_rels */
		appinfo = create_append_rel_info(root, server_rel, hyper_rel);
		root->append_rel_array[server_rel->relid] = appinfo;
		adjust_server_rel_attrs(root, server_rel, hyper_rel, appinfo);
	}

	if (nparts != NULL)
		*nparts = n;

	return part_rels;
}

static void
add_server_scan_paths(PlannerInfo *root, RelOptInfo *baserel)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(baserel);
	Path *path;

	if (baserel->reloptkind == RELOPT_JOINREL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign joins are not supported")));

	path = server_scan_path_create(root,
								   baserel,
								   NULL, /* default pathtarget */
								   fpinfo->rows,
								   fpinfo->startup_cost,
								   fpinfo->total_cost,
								   NIL,  /* no pathkeys */
								   NULL, /* no outer rel either */
								   NULL /* no extra plan */,
								   NIL);
	add_path(baserel, path);

	/* Add paths with pathkeys */
	fdw_add_paths_with_pathkeys_for_rel(root, baserel, NULL, server_scan_path_create);
}

typedef struct FuncExprContext
{
	Index varno;
	List *funcexprs;
} FuncExprContext;

static bool
find_bucketing_exprs_walker(Node *node, FuncExprContext *ctx)
{
	if (node == NULL)
		return false;

	if (IsA(node, FuncExpr))
	{
		FuncExpr *fexpr = (FuncExpr *) node;
		FuncInfo *finfo = ts_func_cache_get_bucketing_func(fexpr->funcid);

		if (NULL != finfo)
		{
			Relids relids = pull_varnos((Node *) fexpr->args);

			if (bms_is_member(ctx->varno, relids))
				ctx->funcexprs = lappend(ctx->funcexprs, fexpr);
		}

		return false;
	}

	return expression_tree_walker(node, find_bucketing_exprs_walker, (void *) ctx);
}

/*
 * Find function expressions that include a bucketing function.
 *
 * Given an expression tree, return a list of FuncExpr:s that reference a
 * bucketing function on the given relation (varno).
 */
static List *
find_bucketing_func_exprs(Node *expr, Index varno)
{
	FuncExprContext ctx = {
		.varno = varno,
		.funcexprs = NIL,
	};

	find_bucketing_exprs_walker(expr, &ctx);

	return ctx.funcexprs;
}

/*
 * Get function expressions with the given attribute (column) as argument.
 *
 * Given a list of FuncExpr:s, return the subset that has the given attribute
 * as an argument.
 */
static List *
get_bucketing_func_exprs_by_attno(List *funcexprs, AttrNumber varattno)
{
	ListCell *lc;
	List *result = NIL;

	foreach (lc, funcexprs)
	{
		FuncExpr *funcexpr = lfirst(lc);
		List *vars;
		ListCell *lc_var;

		Assert(IsA(funcexpr, FuncExpr));

		vars = pull_var_clause((Node *) funcexpr->args, 0);

		foreach (lc_var, vars)
		{
			Var *var = lfirst(lc_var);

			if (var->varattno == varattno)
			{
				result = lappend(result, funcexpr);
				break;
			}
		}
	}

	return result;
}

static void
add_bucketing_exprs_for_dimension(Dimension *dim, RelOptInfo *hyper_rel, List *funcexprs,
								  int partexpr_index)
{
	List *result;

	if (!IS_OPEN_DIMENSION(dim))
		return;

	result = get_bucketing_func_exprs_by_attno(funcexprs, dim->column_attno);

	if (NIL != result)
		hyper_rel->partexprs[partexpr_index] =
			list_concat(hyper_rel->partexprs[partexpr_index], result);
}

/*
 * Force GROUP BY aggregates to be pushed down.
 *
 * Push downs are forced by making the GROUP BY expression in the query become
 * the partitioning keys, even if this is not compatible with
 * partitioning. This makes the planner believe partitioning and GROUP BYs
 * line up perfectly. Forcing a push down is useful because the PostgreSQL
 * planner is not smart enough to realize it can always push things down if
 * there's, e.g., only one partition (or server) involved in the query.
 */
static void
force_group_by_push_down(PlannerInfo *root, RelOptInfo *hyper_rel)
{
	List *groupexprs;
	int num_group_exprs;
	ListCell *lc;
	int i = 0;

	groupexprs = get_sortgrouplist_exprs(root->parse->groupClause, root->parse->targetList);

	num_group_exprs = list_length(groupexprs);

	if (hyper_rel->part_scheme->partnatts < num_group_exprs)
	{
		hyper_rel->part_scheme->partnatts = list_length(groupexprs);
		hyper_rel->partexprs =
			(List **) palloc0(sizeof(List *) * hyper_rel->part_scheme->partnatts);
	}

	foreach (lc, groupexprs)
	{
		List *expr = lfirst(lc);

		hyper_rel->partexprs[i++] = list_make1(expr);
	}
}

/*
 * Check if it is safe to push down GROUP BYs to remote nodes. A push down is
 * safe if the chunks that are part of the query are disjointedly partitioned
 * on servers along the first closed "space" dimension, or all dimensions are
 * covered in the GROUP BY expresssion.
 *
 * If we knew that the GROUP BY covers all partitioning keys, we would not
 * need to check overlaps. Such a check is done in
 * planner.c:group_by_has_partkey(), but this function is not public. We
 * could copy it here to avoid some unnecessary work.
 *
 * There are other "base" cases when we can always safely push down--even if
 * the GROUP BY does NOT cover the partitioning keys--for instance, when only
 * one server is involved in the query. We try to account for such cases too
 * and "trick" the PG planner to do the "right" thing.
 *
 * We also want to add any bucketing expression (on, e.g., time) as a "meta"
 * partitioning key (in rel->partexprs). This will make the partitionwise
 * planner accept the GROUP BY clause for push down even though the expression
 * on time is a "derived" partitioning key.
 */
static void
push_down_group_bys(PlannerInfo *root, RelOptInfo *hyper_rel, Hyperspace *hs,
					DataNodeChunkAssignments *scas)
{
	Dimension *dim;
	List *targetlist = root->parse->targetList;
	bool overlaps;

	Assert(hs->num_dimensions >= 1);
	Assert(hyper_rel->part_scheme->partnatts == hs->num_dimensions);

	/* Check for special case when there is only one server with chunks. This
	 * can always be safely pushed down irrespective of partitioning */
	if (scas->num_nodes_with_chunks == 1)
	{
		force_group_by_push_down(root, hyper_rel);
		return;
	}

	/* Get first closed dimension that we use for assigning chunks to
	 * servers. If there is no closed dimension, we are done. */
	dim = hyperspace_get_closed_dimension(hs, 0);

	if (NULL == dim)
		return;

	overlaps = data_node_chunk_assignments_are_overlapping(scas, dim->fd.id);

	if (overlaps)
	{
		List *funcexprs;
		int i;

		/* When server chunk assignments are overlapping we require that the
		 * GROUP BY covers all partitioning keys. To push down GROUP BYs with
		 * bucketing, we need to also add the bucketing expressions as
		 * partition key expressions. */
		funcexprs = find_bucketing_func_exprs((Node *) targetlist, hyper_rel->relid);

		if (NIL == funcexprs)
			return;

		for (i = 0; i < hs->num_dimensions; i++)
			add_bucketing_exprs_for_dimension(&hs->dimensions[i], hyper_rel, funcexprs, i);
	}

	/* If server chunk assignments are non-overlapping along the "space"
	 * dimension, we can treat this as a one-dimensional partitioned table
	 * since any aggregate GROUP BY that includes the server-assignment
	 * dimension is safe to execute independently on each server. */
	Assert(NULL != dim);
	hyper_rel->partexprs[0] = ts_dimension_get_partexprs(dim, hyper_rel->relid);
	hyper_rel->part_scheme->partnatts = 1;
}

/*
 * Turn chunk append paths into server append paths.
 *
 * By default, a hypertable produces append plans where each child is a chunk
 * to be scanned. This function computes alternative append plans where each
 * child corresponds to a server.
 *
 * In the future, additional assignment algorithms can create their own
 * append paths and have the cost optimizer pick the best one.
 */
void
data_node_scan_add_node_paths(PlannerInfo *root, RelOptInfo *hyper_rel)
{
	RelOptInfo **chunk_rels = hyper_rel->part_rels;
	int nchunk_rels = hyper_rel->nparts;
	RangeTblEntry *hyper_rte = planner_rt_fetch(hyper_rel->relid, root);
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, hyper_rte->relid);
	List *server_rels_list = NIL;
	RelOptInfo **server_rels;
	int nserver_rels;
	DataNodeChunkAssignments scas;
	int i;

	Assert(NULL != ht);

	if (nchunk_rels <= 0)
	{
		ts_cache_release(hcache);
		return;
	}

	/* Create the RelOptInfo for each server */
	server_rels = build_server_part_rels(root, hyper_rel, &nserver_rels);

	Assert(nserver_rels > 0);

	data_node_chunk_assignments_init(&scas, SCA_STRATEGY_ATTACHED_DATA_NODE, root, nserver_rels);

	/* Assign chunks to servers */
	data_node_chunk_assignment_assign_chunks(&scas, chunk_rels, nchunk_rels);

	/* Try to push down GROUP BY expressions and bucketing, if possible */
	push_down_group_bys(root, hyper_rel, ht->space, &scas);

	/* Create estimates and paths for each server rel based on server chunk
	 * assignments */
	for (i = 0; i < nserver_rels; i++)
	{
		RelOptInfo *server_rel = server_rels[i];
		DataNodeChunkAssignment *sca = data_node_chunk_assignment_get_or_create(&scas, server_rel);
		TsFdwRelInfo *fpinfo;

		/* Update the number of tuples and rows based on the chunk
		 * assignments */
		server_rel->tuples = sca->tuples;
		server_rel->rows = sca->rows;

		fpinfo = fdw_relinfo_create(root,
									server_rel,
									server_rel->serverid,
									hyper_rte->relid,
									TS_FDW_RELINFO_HYPERTABLE_DATA_NODE);
		fpinfo->sca = sca;

		if (!bms_is_empty(sca->chunk_relids))
		{
			add_server_scan_paths(root, server_rel);
			server_rels_list = lappend(server_rels_list, server_rel);
		}
		else
			set_dummy_rel_pathlist(server_rel);

		set_cheapest(server_rel);
	}

	Assert(list_length(server_rels_list) > 0);

	/* Reset the pathlist since server scans are preferred */
	hyper_rel->pathlist = NIL;

	/* Must keep partitioning info consistent with the append paths we create */
	hyper_rel->part_rels = server_rels;
	hyper_rel->nparts = nserver_rels;

	add_paths_to_append_rel(root, hyper_rel, server_rels_list);
	ts_cache_release(hcache);
}

void
data_node_scan_create_upper_paths(PlannerInfo *root, UpperRelationKind stage, RelOptInfo *input_rel,
								  RelOptInfo *output_rel, void *extra)
{
	TimescaleDBPrivate *rel_private = input_rel->fdw_private;
	TsFdwRelInfo *fpinfo;

	if (rel_private == NULL || rel_private->fdw_relation_info == NULL)
		/* Not a rel we're interested in */
		return;

	fpinfo = fdw_relinfo_get(input_rel);

	/* Verify that this is a server rel */
	if (NULL == fpinfo || fpinfo->type != TS_FDW_RELINFO_HYPERTABLE_DATA_NODE)
		return;

	fdw_create_upper_paths(fpinfo,
						   root,
						   stage,
						   input_rel,
						   output_rel,
						   extra,
						   server_scan_path_create);
}

static CustomScanMethods server_scan_plan_methods = {
	.CustomName = "ServerScan",
	.CreateCustomScanState = data_node_scan_state_create,
};

typedef struct ServerScanPath
{
	CustomPath cpath;
} ServerScanPath;

static Plan *
server_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist,
						List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	ScanInfo scaninfo = {};

	fdw_scan_info_init(&scaninfo, root, rel, &best_path->path, clauses);

	cscan->methods = &server_scan_plan_methods;
	cscan->custom_plans = custom_plans;
	cscan->scan.plan.targetlist = tlist;
	cscan->scan.scanrelid = scaninfo.scan_relid;
	cscan->custom_scan_tlist = scaninfo.fdw_scan_tlist;
	cscan->scan.plan.qual = scaninfo.local_exprs;
	cscan->custom_exprs = list_make2(scaninfo.params_list, scaninfo.fdw_recheck_quals);

	/*
	 * If this is a join, and to make it valid to push down we had to assume
	 * that the current user is the same as some user explicitly named in the
	 * query, mark the finished plan as depending on the current user.
	 */
	if (rel->useridiscurrent)
		root->glob->dependsOnRole = true;

	/*
	 * If rel is a base relation, detect whether any system columns are
	 * requested from the rel.  (If rel is a join relation, rel->relid will be
	 * 0, but there can be no Var with relid 0 in the rel's targetlist or the
	 * restriction clauses, so we skip this in that case.  Note that any such
	 * columns in base relations that were joined are assumed to be contained
	 * in fdw_scan_tlist.)	This is a bit of a kluge and might go away
	 * someday, so we intentionally leave it out of the API presented to FDWs.
	 */

	scaninfo.systemcol = false;

	if (scaninfo.scan_relid > 0)
	{
		Bitmapset *attrs_used = NULL;
		ListCell *lc;
		int i;

		/*
		 * First, examine all the attributes needed for joins or final output.
		 * Note: we must look at rel's targetlist, not the attr_needed data,
		 * because attr_needed isn't computed for inheritance child rels.
		 */
		pull_varattnos((Node *) rel->reltarget->exprs, scaninfo.scan_relid, &attrs_used);

		/* Add all the attributes used by restriction clauses. */
		foreach (lc, rel->baserestrictinfo)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

			pull_varattnos((Node *) rinfo->clause, scaninfo.scan_relid, &attrs_used);
		}

		/* Now, are any system columns requested from rel? */
		for (i = FirstLowInvalidHeapAttributeNumber + 1; i < 0; i++)
		{
			if (bms_is_member(i - FirstLowInvalidHeapAttributeNumber, attrs_used))
			{
				scaninfo.systemcol = true;
				break;
			}
		}

		bms_free(attrs_used);
	}

	cscan->custom_private = list_make2(scaninfo.fdw_private, list_make1_int(scaninfo.systemcol));

	return &cscan->scan.plan;
}

static CustomPathMethods server_scan_path_methods = {
	.CustomName = "ServerScanPath",
	.PlanCustomPath = server_scan_plan_create,
};

static Path *
server_scan_path_create(PlannerInfo *root, RelOptInfo *rel, PathTarget *target, double rows,
						Cost startup_cost, Cost total_cost, List *pathkeys, Relids required_outer,
						Path *fdw_outerpath, List *private)
{
	ServerScanPath *scanpath = palloc0(sizeof(ServerScanPath));

	if (rel->lateral_relids && !bms_is_subset(rel->lateral_relids, required_outer))
		required_outer = bms_union(required_outer, rel->lateral_relids);

	if (!bms_is_empty(required_outer) && !IS_SIMPLE_REL(rel))
		elog(ERROR, "parameterized foreign joins are not supported yet");

	scanpath->cpath.path.type = T_CustomPath;
	scanpath->cpath.path.pathtype = T_CustomScan;
	scanpath->cpath.custom_paths = fdw_outerpath == NULL ? NIL : list_make1(fdw_outerpath);
	scanpath->cpath.methods = &server_scan_path_methods;
	scanpath->cpath.path.parent = rel;
	scanpath->cpath.path.pathtarget = target ? target : rel->reltarget;
	scanpath->cpath.path.param_info = get_baserel_parampathinfo(root, rel, required_outer);
	scanpath->cpath.path.parallel_aware = false;
	scanpath->cpath.path.parallel_safe = rel->consider_parallel;
	scanpath->cpath.path.parallel_workers = 0;
	scanpath->cpath.path.rows = rows;
	scanpath->cpath.path.startup_cost = startup_cost;
	scanpath->cpath.path.total_cost = total_cost;
	scanpath->cpath.path.pathkeys = pathkeys;

	return &scanpath->cpath.path;
}
