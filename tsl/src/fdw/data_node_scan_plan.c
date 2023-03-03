/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/sysattr.h>
#include <foreign/fdwapi.h>
#include <nodes/extensible.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/pathnodes.h>
#include <nodes/plannodes.h>
#include <optimizer/appendinfo.h>
#include <optimizer/clauses.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/prep.h>
#include <optimizer/restrictinfo.h>
#include <optimizer/tlist.h>
#include <parser/parsetree.h>
#include <utils/memutils.h>

#include <math.h>

#include <compat/compat.h>
#include <debug.h>
#include <debug_guc.h>
#include <dimension.h>
#include <export.h>
#include <func_cache.h>
#include <hypertable_cache.h>
#include <import/allpaths.h>
#include <import/planner.h>
#include <planner.h>

#include "data_node_scan_plan.h"

#include "data_node_chunk_assignment.h"
#include "data_node_scan_exec.h"
#include "deparse.h"
#include "fdw_utils.h"
#include "relinfo.h"
#include "scan_plan.h"
#include "estimate.h"
#include "planner/planner.h"
#include "chunk.h"
#include "debug_assert.h"

/*
 * DataNodeScan is a custom scan implementation for scanning hypertables on
 * remote data nodes instead of scanning individual remote chunks.
 *
 * A DataNodeScan plan is created by taking a regular per-chunk scan plan and
 * then assigning each chunk to a data node, and treating each data node as a
 * "partition" of the distributed hypertable. For each resulting data node, we
 * create a data node rel which is essentially a base rel representing a remote
 * hypertable partition. Since we treat a data node rel as a base rel, although
 * it has no corresponding data node table, we point each data node rel to the root
 * hypertable. This is conceptually the right thing to do, since each data node
 * rel is a partition of the same distributed hypertable.
 *
 * For each data node rel, we plan a DataNodeScan instead of a ForeignScan since a
 * data node rel does not correspond to a real foreign table. A ForeignScan of a
 * data node rel would fail when trying to lookup the ForeignServer via the
 * data node rel's RTE relid. The only other option to get around the
 * ForeignTable lookup is to make a data node rel an upper rel instead of a base
 * rel (see nodeForeignscan.c). However, that leads to other issues in
 * setrefs.c that messes up our target lists for some queries.
 */

static Path *data_node_scan_path_create(PlannerInfo *root, RelOptInfo *rel, PathTarget *target,
										double rows, Cost startup_cost, Cost total_cost,
										List *pathkeys, Relids required_outer, Path *fdw_outerpath,
										List *private);

static Path *data_node_join_path_create(PlannerInfo *root, RelOptInfo *rel, PathTarget *target,
										double rows, Cost startup_cost, Cost total_cost,
										List *pathkeys, Relids required_outer, Path *fdw_outerpath,
										List *private);

static Path *data_node_scan_upper_path_create(PlannerInfo *root, RelOptInfo *rel,
											  PathTarget *target, double rows, Cost startup_cost,
											  Cost total_cost, List *pathkeys, Path *fdw_outerpath,
											  List *private);

static bool fdw_pushdown_foreign_join(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
									  RelOptInfo *outerrel, RelOptInfo *innerrel,
									  JoinPathExtraData *extra);

static AppendRelInfo *
create_append_rel_info(PlannerInfo *root, Index childrelid, Index parentrelid)
{
	RangeTblEntry *parent_rte = planner_rt_fetch(parentrelid, root);
	Relation relation = table_open(parent_rte->relid, NoLock);
	AppendRelInfo *appinfo;

	appinfo = makeNode(AppendRelInfo);
	appinfo->parent_relid = parentrelid;
	appinfo->child_relid = childrelid;
	appinfo->parent_reltype = relation->rd_rel->reltype;
	appinfo->child_reltype = relation->rd_rel->reltype;
	ts_make_inh_translation_list(relation, relation, childrelid, &appinfo->translated_vars);
	appinfo->parent_reloid = parent_rte->relid;
	table_close(relation, NoLock);

	return appinfo;
}

/*
 * Build a new RelOptInfo representing a data node.
 *
 * Note that the relid index should point to the corresponding range table
 * entry (RTE) we created for the data node rel when expanding the
 * hypertable. Each such RTE's relid (OID) refers to the hypertable's root
 * table. This has the upside that the planner can use the hypertable's
 * indexes to plan remote queries more efficiently. In contrast, chunks are
 * foreign tables and they cannot have indexes.
 */
static RelOptInfo *
build_data_node_rel(PlannerInfo *root, Index relid, Oid serverid, RelOptInfo *parent)
{
	RelOptInfo *rel = build_simple_rel(root, relid, parent);

	/*
	 * Use relevant exprs and restrictinfos from the parent rel. These will be
	 * adjusted to match the data node rel's relid later.
	 */
	rel->reltarget->exprs = copyObject(parent->reltarget->exprs);
	rel->baserestrictinfo = parent->baserestrictinfo;
	rel->baserestrictcost = parent->baserestrictcost;
	rel->baserestrict_min_security = parent->baserestrict_min_security;
	rel->lateral_vars = parent->lateral_vars;
	rel->lateral_referencers = parent->lateral_referencers;
	rel->lateral_relids = parent->lateral_relids;
	rel->serverid = serverid;

	/*
	 * We need to use the FDW interface to get called by the planner for
	 * partial aggs. For some reason, the standard upper_paths_hook is never
	 * called for upper rels of type UPPERREL_PARTIAL_GROUP_AGG, which is odd
	 * (see end of PostgreSQL planner.c:create_partial_grouping_paths). Until
	 * this gets fixed in the PostgreSQL planner, we're forced to set
	 * fdwroutine here although we will scan this rel with a DataNodeScan and
	 * not a ForeignScan.
	 */
	rel->fdwroutine = GetFdwRoutineByServerId(serverid);

	return rel;
}

/*
 * Adjust the attributes of data node rel quals.
 *
 * Code adapted from allpaths.c: set_append_rel_size.
 *
 * For each data node child rel, copy the quals/restrictions from the parent
 * (hypertable) rel and adjust the attributes (e.g., Vars) to point to the
 * child rel instead of the parent.
 *
 * Normally, this happens as part of estimating the rel size of an append
 * relation in standard planning, where constraint exclusion and partition
 * pruning also happens for each child. Here, however, we don't prune any
 * data node rels since they are created based on assignment of already pruned
 * chunk child rels at an earlier stage. Data node rels that aren't assigned any
 * chunks will never be created in the first place.
 */
static void
adjust_data_node_rel_attrs(PlannerInfo *root, RelOptInfo *data_node_rel, RelOptInfo *hyper_rel,
						   AppendRelInfo *appinfo)
{
	List *nodequals = NIL;
	ListCell *lc;

	foreach (lc, hyper_rel->baserestrictinfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Node *nodequal;
		ListCell *lc2;

		nodequal = adjust_appendrel_attrs(root, (Node *) rinfo->clause, 1, &appinfo);

		nodequal = eval_const_expressions(root, nodequal);

		/* might have gotten an AND clause, if so flatten it */
		foreach (lc2, make_ands_implicit((Expr *) nodequal))
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
			nodequals = lappend(nodequals,
								make_restrictinfo_compat(root,
														 (Expr *) onecq,
														 rinfo->is_pushed_down,
														 rinfo->outerjoin_delayed,
														 pseudoconstant,
														 rinfo->security_level,
														 NULL,
														 NULL,
														 NULL));
		}
	}

	data_node_rel->baserestrictinfo = nodequals;
	data_node_rel->joininfo =
		castNode(List, adjust_appendrel_attrs(root, (Node *) hyper_rel->joininfo, 1, &appinfo));

	data_node_rel->reltarget->exprs =
		castNode(List,
				 adjust_appendrel_attrs(root, (Node *) hyper_rel->reltarget->exprs, 1, &appinfo));

	/* Add equivalence class for rel to push down joins and sorts */
	if (hyper_rel->has_eclass_joins || has_useful_pathkeys(root, hyper_rel))
		add_child_rel_equivalences(root, appinfo, hyper_rel, data_node_rel);

	data_node_rel->has_eclass_joins = hyper_rel->has_eclass_joins;
}

/*
 * Build RelOptInfos for each data node.
 *
 * Each data node rel will point to the root hypertable table, which is
 * conceptually correct since we query the identical (partial) hypertables on
 * the data nodes.
 */
static RelOptInfo **
build_data_node_part_rels(PlannerInfo *root, RelOptInfo *hyper_rel, int *nparts)
{
	TimescaleDBPrivate *priv = hyper_rel->fdw_private;
	/* Update the partitioning to reflect the new per-data node plan */
	RelOptInfo **part_rels = palloc(sizeof(RelOptInfo *) * list_length(priv->serverids));
	ListCell *lc;
	int n = 0;
	int i;

	Assert(list_length(priv->serverids) == bms_num_members(priv->server_relids));
	i = -1;

	foreach (lc, priv->serverids)
	{
		Oid data_node_id = lfirst_oid(lc);
		RelOptInfo *data_node_rel;
		AppendRelInfo *appinfo;

		i = bms_next_member(priv->server_relids, i);

		Assert(i > 0);

		/*
		 * The planner expects an AppendRelInfo for any part_rels. Needs to be
		 * added prior to creating the rel because build_simple_rel will
		 * invoke our planner hooks that classify relations using this
		 * information.
		 */
		appinfo = create_append_rel_info(root, i, hyper_rel->relid);
		root->append_rel_array[i] = appinfo;
		data_node_rel = build_data_node_rel(root, i, data_node_id, hyper_rel);
		part_rels[n++] = data_node_rel;
		adjust_data_node_rel_attrs(root, data_node_rel, hyper_rel, appinfo);
	}

	if (nparts != NULL)
		*nparts = n;

	return part_rels;
}

/* Callback argument for ts_ec_member_matches_foreign */
typedef struct
{
	Expr *current;		/* current expr, or NULL if not yet found */
	List *already_used; /* expressions already dealt with */
} ts_ec_member_foreign_arg;

/*
 * Detect whether we want to process an EquivalenceClass member.
 *
 * This is a callback for use by generate_implied_equalities_for_column.
 */
static bool
ts_ec_member_matches_foreign(PlannerInfo *root, RelOptInfo *rel, EquivalenceClass *ec,
							 EquivalenceMember *em, void *arg)
{
	ts_ec_member_foreign_arg *state = (ts_ec_member_foreign_arg *) arg;
	Expr *expr = em->em_expr;

	/*
	 * If we've identified what we're processing in the current scan, we only
	 * want to match that expression.
	 */
	if (state->current != NULL)
		return equal(expr, state->current);

	/*
	 * Otherwise, ignore anything we've already processed.
	 */
	if (list_member(state->already_used, expr))
		return false;

	/* This is the new target to process. */
	state->current = expr;
	return true;
}

/*
 * Build parameterizations that are useful for performing joins with the given
 * hypertable relation. We will use them to generate the parameterized data node
 * scan paths. The code is mostly copied from postgres_fdw,
 * postgresGetForeignPaths().
 */
static List *
build_parameterizations(PlannerInfo *root, RelOptInfo *hyper_rel)
{
	/*
	 * Thumb through all join clauses for the rel to identify which outer
	 * relations could supply one or more safe-to-send-to-remote join clauses.
	 * We'll build a parameterized path for each such outer relation.
	 *
	 * Note that in case we have multiple local tables, this outer relation
	 * here may be the result of joining the local tables together. For an
	 * example, see the multiple join in the dist_param test.
	 *
	 * It's convenient to represent each candidate outer relation by the
	 * ParamPathInfo node for it.  We can then use the ppi_clauses list in the
	 * ParamPathInfo node directly as a list of the interesting join clauses for
	 * that rel.  This takes care of the possibility that there are multiple
	 * safe join clauses for such a rel, and also ensures that we account for
	 * unsafe join clauses that we'll still have to enforce locally (since the
	 * parameterized-path machinery insists that we handle all movable clauses).
	 */
	List *ppi_list = NIL;
	ListCell *lc;
	foreach (lc, hyper_rel->joininfo)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
		Relids required_outer;
		ParamPathInfo *param_info;

		/* Check if clause can be moved to this rel */
		if (!join_clause_is_movable_to(rinfo, hyper_rel))
		{
			continue;
		}

		/* See if it is safe to send to remote */
		if (!ts_is_foreign_expr(root, hyper_rel, rinfo->clause))
		{
			continue;
		}

		/* Calculate required outer rels for the resulting path */
		required_outer = bms_union(rinfo->clause_relids, hyper_rel->lateral_relids);
		/* We do not want the data node rel itself listed in required_outer */
		required_outer = bms_del_member(required_outer, hyper_rel->relid);

		/*
		 * required_outer probably can't be empty here, but if it were, we
		 * couldn't make a parameterized path.
		 */
		if (bms_is_empty(required_outer))
		{
			continue;
		}

		/* Get the ParamPathInfo */
		param_info = get_baserel_parampathinfo(root, hyper_rel, required_outer);
		Assert(param_info != NULL);

		/*
		 * Add it to list unless we already have it.  Testing pointer equality
		 * is OK since get_baserel_parampathinfo won't make duplicates.
		 */
		ppi_list = list_append_unique_ptr(ppi_list, param_info);
	}

	/*
	 * The above scan examined only "generic" join clauses, not those that
	 * were absorbed into EquivalenceClauses.  See if we can make anything out
	 * of EquivalenceClauses.
	 */
	if (hyper_rel->has_eclass_joins)
	{
		/*
		 * We repeatedly scan the eclass list looking for column references
		 * (or expressions) belonging to the data node rel.  Each time we find
		 * one, we generate a list of equivalence joinclauses for it, and then
		 * see if any are safe to send to the remote.  Repeat till there are
		 * no more candidate EC members.
		 */
		ts_ec_member_foreign_arg arg;

		arg.already_used = NIL;
		for (;;)
		{
			List *clauses;

			/* Make clauses, skipping any that join to lateral_referencers */
			arg.current = NULL;
			clauses = generate_implied_equalities_for_column(root,
															 hyper_rel,
															 ts_ec_member_matches_foreign,
															 (void *) &arg,
															 hyper_rel->lateral_referencers);

			/* Done if there are no more expressions in the data node rel */
			if (arg.current == NULL)
			{
				Assert(clauses == NIL);
				break;
			}

			/* Scan the extracted join clauses */
			foreach (lc, clauses)
			{
				RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);
				Relids required_outer;
				ParamPathInfo *param_info;

				/* Check if clause can be moved to this rel */
				if (!join_clause_is_movable_to(rinfo, hyper_rel))
				{
					continue;
				}

				/* See if it is safe to send to remote */
				if (!ts_is_foreign_expr(root, hyper_rel, rinfo->clause))
				{
					continue;
				}

				/* Calculate required outer rels for the resulting path */
				required_outer = bms_union(rinfo->clause_relids, hyper_rel->lateral_relids);
				required_outer = bms_del_member(required_outer, hyper_rel->relid);
				if (bms_is_empty(required_outer))
				{
					continue;
				}

				/* Get the ParamPathInfo */
				param_info = get_baserel_parampathinfo(root, hyper_rel, required_outer);
				Assert(param_info != NULL);

				/* Add it to list unless we already have it */
				ppi_list = list_append_unique_ptr(ppi_list, param_info);
			}

			/* Try again, now ignoring the expression we found this time */
			arg.already_used = lappend(arg.already_used, arg.current);
		}
	}

	return ppi_list;
}

static void
add_data_node_scan_paths(PlannerInfo *root, RelOptInfo *data_node_rel, RelOptInfo *hyper_rel,
						 List *ppi_list)
{
	TsFdwRelInfo *fpinfo = fdw_relinfo_get(data_node_rel);
	Path *path;

	if (data_node_rel->reloptkind == RELOPT_JOINREL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("foreign joins are not supported")));

	path = data_node_scan_path_create(root,
									  data_node_rel,
									  NULL, /* default pathtarget */
									  fpinfo->rows,
									  fpinfo->startup_cost,
									  fpinfo->total_cost,
									  NIL, /* no pathkeys */
									  NULL,
									  NULL /* no extra plan */,
									  NIL);

	fdw_utils_add_path(data_node_rel, path);

	/* Add paths with pathkeys */
	fdw_add_paths_with_pathkeys_for_rel(root, data_node_rel, NULL, data_node_scan_path_create);

	/*
	 * Now build a path for each useful outer relation, if the parameterized
	 * data node scans are not disabled.
	 */
	if (!ts_guc_enable_parameterized_data_node_scan)
	{
		return;
	}

	ListCell *ppi_cell;
	foreach (ppi_cell, ppi_list)
	{
		ParamPathInfo *param_info = (ParamPathInfo *) lfirst(ppi_cell);

		/*
		 * Check if we have an index path locally that matches the
		 * parameterization. If so, we're going to have the same index path on
		 * the data node, and it's going to be significantly cheaper that a seq
		 * scan. We don't know precise values, but we have to discount it later
		 * so that the remote index paths are preferred.
		 */
		bool index_matches_parameterization = false;
		ListCell *path_cell;
		foreach (path_cell, hyper_rel->pathlist)
		{
			Path *path = (Path *) lfirst(path_cell);
			if (path->param_info == param_info)
			{
				/*
				 * We shouldn't have parameterized seq scans. Can be an
				 * IndexPath (includes index-only scans) or a BitmapHeapPath.
				 */
				Assert(path->type == T_BitmapHeapPath || path->type == T_IndexPath);

				index_matches_parameterization = true;
				break;
			}
		}

		/*
		 * As a baseline, cost the data node scan as a seq scan.
		 */
		Cost startup_cost = 0;
		Cost run_cost = 0;
		double rows = data_node_rel->tuples > 1 ? data_node_rel->tuples : 123456;

		/* Run remote non-join clauses. */
		const double remote_sel_sane =
			(fpinfo->remote_conds_sel > 0 && fpinfo->remote_conds_sel <= 1) ?
				fpinfo->remote_conds_sel :
				0.1;

		startup_cost += data_node_rel->reltarget->cost.startup;
		startup_cost += fpinfo->remote_conds_cost.startup;
		run_cost += fpinfo->remote_conds_cost.per_tuple * rows;
		run_cost += cpu_tuple_cost * rows;
		run_cost += seq_page_cost * data_node_rel->pages;
		rows *= remote_sel_sane;

		/*
		 * For this parameterization, we're going to have an index scan on the
		 * remote. We don't have a way to calculate the precise cost for it, so
		 * at least discount it by a constant factor compared to the seq scan.
		 */
		if (index_matches_parameterization)
		{
			run_cost *= 0.1;
		}

		/* Run remote join clauses. */
		QualCost remote_join_cost;
		cost_qual_eval(&remote_join_cost, param_info->ppi_clauses, root);

		/*
		 * We don't have up to date per-column statistics for the root
		 * distributed hypertable currently, so the join estimates are going to
		 * be way off. The worst is when they are too low and we end up
		 * transferring much more rows from the data node that we expected. Just
		 * hardcode it at 0.1 per clause for now.
		 * In the future, we could make use of per-chunk per-column statistics
		 * that we do have, by injecting them into the Postgres cost functions
		 * through the get_relation_stats_hook. For a data node scan, we would
		 * combine statistics for all participating chunks on the given data
		 * node.
		 */
		const double remote_join_sel = pow(0.1, list_length(param_info->ppi_clauses));

		startup_cost += remote_join_cost.startup;
		run_cost += remote_join_cost.per_tuple * rows;
		rows *= remote_join_sel;

		/* Transfer the resulting tuples over the network. */
		startup_cost += fpinfo->fdw_startup_cost;
		run_cost += fpinfo->fdw_tuple_cost * rows;

		/* Run local filters. */
		const double local_sel_sane =
			(fpinfo->local_conds_sel > 0 && fpinfo->local_conds_sel <= 1) ?
				fpinfo->local_conds_sel :
				0.5;

		startup_cost += fpinfo->local_conds_cost.startup;
		run_cost += fpinfo->local_conds_cost.per_tuple * rows;
		run_cost += cpu_tuple_cost * rows;
		rows *= local_sel_sane;

		/* Compute the output targetlist. */
		run_cost += data_node_rel->reltarget->cost.per_tuple * rows;

		rows = clamp_row_est(rows);

		/*
		 * ppi_rows currently won't get looked at by anything, but still we
		 * may as well ensure that it matches our idea of the rowcount.
		 */
		param_info->ppi_rows = rows;

		/* Make the path */
		path = data_node_scan_path_create(root,
										  data_node_rel,
										  NULL, /* default pathtarget */
										  rows,
										  startup_cost,
										  startup_cost + run_cost,
										  NIL, /* no pathkeys */
										  param_info->ppi_req_outer,
										  NULL,
										  NIL); /* no fdw_private list */

		add_path(data_node_rel, (Path *) path);
	}
}

/*
 * Force GROUP BY aggregates to be pushed down.
 *
 * Push downs are forced by making the GROUP BY expression in the query become
 * the partitioning keys, even if this is not compatible with
 * partitioning. This makes the planner believe partitioning and GROUP BYs
 * line up perfectly. Forcing a push down is useful because the PostgreSQL
 * planner is not smart enough to realize it can always push things down if
 * there's, e.g., only one partition (or data node) involved in the query.
 */
static void
force_group_by_push_down(PlannerInfo *root, RelOptInfo *hyper_rel)
{
	PartitionScheme partscheme = hyper_rel->part_scheme;
	List *groupexprs;
	List **nullable_partexprs;
	int16 new_partnatts;
	Oid *partopfamily;
	Oid *partopcintype;
	Oid *partcollation;
	ListCell *lc;
	int i = 0;

	Assert(partscheme != NULL);

	groupexprs = get_sortgrouplist_exprs(root->parse->groupClause, root->parse->targetList);
	new_partnatts = list_length(groupexprs);

	/*
	 * Only reallocate the partitioning attributes arrays if it is smaller than
	 * the new size. palloc0 is needed to zero out the extra space.
	 */
	if (partscheme->partnatts < new_partnatts)
	{
		partopfamily = palloc0(new_partnatts * sizeof(Oid));
		partopcintype = palloc0(new_partnatts * sizeof(Oid));
		partcollation = palloc0(new_partnatts * sizeof(Oid));
		nullable_partexprs = palloc0(new_partnatts * sizeof(List *));

		memcpy(partopfamily, partscheme->partopfamily, partscheme->partnatts * sizeof(Oid));
		memcpy(partopcintype, partscheme->partopcintype, partscheme->partnatts * sizeof(Oid));
		memcpy(partcollation, partscheme->partcollation, partscheme->partnatts * sizeof(Oid));
		memcpy(nullable_partexprs,
			   hyper_rel->nullable_partexprs,
			   partscheme->partnatts * sizeof(List *));

		partscheme->partopfamily = partopfamily;
		partscheme->partopcintype = partopcintype;
		partscheme->partcollation = partcollation;
		hyper_rel->nullable_partexprs = nullable_partexprs;

		hyper_rel->partexprs = (List **) palloc0(sizeof(List *) * new_partnatts);
	}

	partscheme->partnatts = new_partnatts;

	foreach (lc, groupexprs)
	{
		List *expr = lfirst(lc);

		hyper_rel->partexprs[i++] = list_make1(expr);
	}

	Assert(i == partscheme->partnatts);
}

/*
 * Check if it is safe to push down GROUP BYs to remote nodes. A push down is
 * safe if the chunks that are part of the query are disjointedly partitioned
 * on data nodes along the first closed "space" dimension, or all dimensions are
 * covered in the GROUP BY expresssion.
 *
 * If we knew that the GROUP BY covers all partitioning keys, we would not
 * need to check overlaps. Such a check is done in
 * planner.c:group_by_has_partkey(), but this function is not public. We
 * could copy it here to avoid some unnecessary work.
 *
 * There are other "base" cases when we can always safely push down--even if
 * the GROUP BY does NOT cover the partitioning keys--for instance, when only
 * one data node is involved in the query. We try to account for such cases too
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
	const Dimension *dim;
	bool overlaps;

	Assert(hs->num_dimensions >= 1);
	Assert(hyper_rel->part_scheme->partnatts == hs->num_dimensions);

	/*
	 * Check for special case when there is only one data node with chunks. This
	 * can always be safely pushed down irrespective of partitioning
	 */
	if (scas->num_nodes_with_chunks == 1)
	{
		force_group_by_push_down(root, hyper_rel);
		return;
	}

	/*
	 * Get first closed dimension that we use for assigning chunks to
	 * data nodes. If there is no closed dimension, we are done.
	 */
	dim = hyperspace_get_closed_dimension(hs, 0);

	if (NULL == dim)
		return;

	overlaps = data_node_chunk_assignments_are_overlapping(scas, dim->fd.id);

	if (!overlaps)
	{
		/*
		 * If data node chunk assignments are non-overlapping along the
		 * "space" dimension, we can treat this as a one-dimensional
		 * partitioned table since any aggregate GROUP BY that includes the
		 * data node assignment dimension is safe to execute independently on
		 * each data node.
		 */
		Assert(NULL != dim);
		hyper_rel->partexprs[0] = ts_dimension_get_partexprs(dim, hyper_rel->relid);
		hyper_rel->part_scheme->partnatts = 1;
	}
}

/*
 * Check if the query performs a join between a hypertable (outer) and a reference
 * table (inner) and the join type is a LEFT JOIN, an INNER JOIN, or an implicit
 * join.
 */
static bool
is_safe_to_pushdown_reftable_join(PlannerInfo *root, List *join_reference_tables,
								  RangeTblEntry *innertableref, JoinType jointype)
{
	Assert(root != NULL);
	Assert(innertableref != NULL);

	/*
	 * We support pushing down of INNER and LEFT joins only.
	 *
	 * Constructing queries representing partitioned FULL, SEMI, and ANTI
	 * joins is hard, hence not considered right now.
	 */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT)
		return false;

	/* Check that at least one reference table is defined. */
	if (join_reference_tables == NIL)
		return false;

	/* Only queries with two tables are supported. */
	if (bms_num_members(root->all_baserels) != 2)
		return false;

	/* Right table has to be a distributed hypertable */
	if (!list_member_oid(join_reference_tables, innertableref->relid))
		return false;

	/* Join can be pushed down */
	return true;
}

/*
 * Assess whether the join between inner and outer relations can be pushed down
 * to the foreign server. As a side effect, save information we obtain in this
 * function to TsFdwRelInfo passed in.
 *
 * The code is based on PostgreSQL's foreign_join_ok function (version 15.1).
 */
static bool
fdw_pushdown_foreign_join(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
						  RelOptInfo *outerrel, RelOptInfo *innerrel, JoinPathExtraData *extra)
{
	TsFdwRelInfo *fpinfo;
	TsFdwRelInfo *fpinfo_o;
	TsFdwRelInfo *fpinfo_i;
	ListCell *lc;
	List *joinclauses;

	/*
	 * If either of the joining relations is marked as unsafe to pushdown, the
	 * join can not be pushed down.
	 */
	fpinfo = fdw_relinfo_get(joinrel);
	fpinfo_o = fdw_relinfo_get(outerrel);
	fpinfo_i = fdw_relinfo_get(innerrel);

	Assert(fpinfo_o != NULL);
	Assert(fpinfo_o->pushdown_safe);
	Assert(fpinfo_i != NULL);
	Assert(fpinfo_i->pushdown_safe);

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations. Hence the join can
	 * not be pushed down (shouldn't happen in the current implementation).
	 */
	Assert(fpinfo_o->local_conds == NULL);
	Assert(fpinfo_i->local_conds == NULL);

	fpinfo->server = fpinfo_o->server;

	/*
	 * Separate restrict list into join quals and pushed-down (other) quals.
	 *
	 * Join quals belonging to an outer join must all be shippable, else we
	 * cannot execute the join remotely.  Add such quals to 'joinclauses'.
	 *
	 * Add other quals to fpinfo->remote_conds if they are shippable, else to
	 * fpinfo->local_conds.  In an inner join it's okay to execute conditions
	 * either locally or remotely; the same is true for pushed-down conditions
	 * at an outer join.
	 *
	 * Note we might return failure after having already scribbled on
	 * fpinfo->remote_conds and fpinfo->local_conds.  That's okay because we
	 * won't consult those lists again if we deem the join unshippable.
	 */
	joinclauses = NIL;
	foreach (lc, extra->restrictlist)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
		bool is_remote_clause = ts_is_foreign_expr(root, joinrel, rinfo->clause);

		if (IS_OUTER_JOIN(jointype) && !RINFO_IS_PUSHED_DOWN(rinfo, joinrel->relids))
		{
			if (!is_remote_clause)
				return false;
			joinclauses = lappend(joinclauses, rinfo);
		}
		else
		{
			if (is_remote_clause)
				fpinfo->remote_conds = lappend(fpinfo->remote_conds, rinfo);
			else
				fpinfo->local_conds = lappend(fpinfo->local_conds, rinfo);
		}
	}

	if (fpinfo->local_conds != NIL)
		return false;

	/* Save the join clauses, for later use. */
	fpinfo->joinclauses = joinclauses;

	/*
	 * deparseExplicitTargetList() isn't smart enough to handle anything other
	 * than a Var.  In particular, if there's some PlaceHolderVar that would
	 * need to be evaluated within this join tree (because there's an upper
	 * reference to a quantity that may go to NULL as a result of an outer
	 * join), then we can't try to push the join down because we'll fail when
	 * we get to deparseExplicitTargetList().  However, a PlaceHolderVar that
	 * needs to be evaluated *at the top* of this join tree is OK, because we
	 * can do that locally after fetching the results from the remote side.
	 *
	 * Note: At the moment, the placeholder code is not used in our current join
	 * pushdown implementation.
	 */
#ifdef ENABLE_DEAD_CODE
	foreach (lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = lfirst(lc);
		Relids relids;

		/* PlaceHolderInfo refers to parent relids, not child relids. */
		relids = IS_OTHER_REL(joinrel) ? joinrel->top_parent_relids : joinrel->relids;

		if (bms_is_subset(phinfo->ph_eval_at, relids) &&
			bms_nonempty_difference(relids, phinfo->ph_eval_at))
			return false;
	}
#endif

	fpinfo->outerrel = outerrel;
	fpinfo->innerrel = innerrel;
	fpinfo->jointype = jointype;

	/*
	 * By default, both the input relations are not required to be deparsed as
	 * subqueries, but there might be some relations covered by the input
	 * relations that are required to be deparsed as subqueries, so save the
	 * relids of those relations for later use by the deparser.
	 */
	fpinfo->make_outerrel_subquery = false;
	fpinfo->make_innerrel_subquery = false;
	Assert(bms_is_subset(fpinfo_o->lower_subquery_rels, outerrel->relids));
	Assert(bms_is_subset(fpinfo_i->lower_subquery_rels, innerrel->relids));
	fpinfo->lower_subquery_rels =
		bms_union(fpinfo_o->lower_subquery_rels, fpinfo_i->lower_subquery_rels);

	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation
	 * wherever possible. This avoids building subqueries at every join step.
	 *
	 * For an inner join, clauses from both the relations are added to the
	 * other remote clauses. For LEFT and RIGHT OUTER join, the clauses from
	 * the outer side are added to remote_conds since those can be evaluated
	 * after the join is evaluated. The clauses from inner side are added to
	 * the joinclauses, since they need to be evaluated while constructing the
	 * join.
	 *
	 * For a FULL OUTER JOIN, the other clauses from either relation can not
	 * be added to the joinclauses or remote_conds, since each relation acts
	 * as an outer relation for the other.
	 *
	 * The joining sides can not have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
		case JOIN_INNER:
#if PG14_GE
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds, fpinfo_i->remote_conds);
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds, fpinfo_o->remote_conds);
#else
			fpinfo->remote_conds =
				list_concat(fpinfo->remote_conds, list_copy(fpinfo_i->remote_conds));
			fpinfo->remote_conds =
				list_concat(fpinfo->remote_conds, list_copy(fpinfo_o->remote_conds));
#endif
			break;

		case JOIN_LEFT:
#if PG14_GE
			fpinfo->joinclauses = list_concat(fpinfo->joinclauses, fpinfo_i->remote_conds);
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds, fpinfo_o->remote_conds);
#else
			fpinfo->joinclauses =
				list_concat(fpinfo->joinclauses, list_copy(fpinfo_i->remote_conds));
			fpinfo->remote_conds =
				list_concat(fpinfo->remote_conds, list_copy(fpinfo_o->remote_conds));
#endif
			break;

/* Right and full joins are not supported at the moment */
#ifdef ENABLE_DEAD_CODE
		case JOIN_RIGHT:
#if PG14_GE
			fpinfo->joinclauses = list_concat(fpinfo->joinclauses, fpinfo_o->remote_conds);
			fpinfo->remote_conds = list_concat(fpinfo->remote_conds, fpinfo_i->remote_conds);
#else
			fpinfo->joinclauses =
				list_concat(fpinfo->joinclauses, list_copy(fpinfo_o->remote_conds));
			fpinfo->remote_conds =
				list_concat(fpinfo->remote_conds, list_copy(fpinfo_i->remote_conds));
#endif
			break;

		case JOIN_FULL:

			/*
			 * In this case, if any of the input relations has conditions, we
			 * need to deparse that relation as a subquery so that the
			 * conditions can be evaluated before the join.  Remember it in
			 * the fpinfo of this relation so that the deparser can take
			 * appropriate action.  Also, save the relids of base relations
			 * covered by that relation for later use by the deparser.
			 */
			if (fpinfo_o->remote_conds)
			{
				fpinfo->make_outerrel_subquery = true;
				fpinfo->lower_subquery_rels =
					bms_add_members(fpinfo->lower_subquery_rels, outerrel->relids);
			}
			if (fpinfo_i->remote_conds)
			{
				fpinfo->make_innerrel_subquery = true;
				fpinfo->lower_subquery_rels =
					bms_add_members(fpinfo->lower_subquery_rels, innerrel->relids);
			}
			break;
#endif

		default:
			/* Should not happen, we have just checked this above */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/*
	 * For an inner join, all restrictions can be treated alike. Treating the
	 * pushed down conditions as join conditions allows a top level full outer
	 * join to be deparsed without requiring subqueries.
	 */
	if (jointype == JOIN_INNER)
	{
		Assert(!fpinfo->joinclauses);
		fpinfo->joinclauses = fpinfo->remote_conds;
		fpinfo->remote_conds = NIL;
	}
	/* Mark that this join can be pushed down safely */
	fpinfo->pushdown_safe = true;

	/*
	 * Set the string describing this join relation to be used in EXPLAIN
	 * output of corresponding ForeignScan.  Note that the decoration we add
	 * to the base relation names mustn't include any digits, or it'll confuse
	 * postgresExplainForeignScan.
	 */
	fpinfo->relation_name = makeStringInfo();
	appendStringInfo(fpinfo->relation_name,
					 "(%s) %s JOIN (%s)",
					 fpinfo_o->relation_name->data,
					 get_jointype_name(fpinfo->jointype),
					 fpinfo_i->relation_name->data);

	/*
	 * Set the relation index.  This is defined as the position of this
	 * joinrel in the join_rel_list list plus the length of the rtable list.
	 * Note that since this joinrel is at the end of the join_rel_list list
	 * when we are called, we can get the position by list_length.
	 */
	fpinfo->relation_index = list_length(root->parse->rtable) + list_length(root->join_rel_list);

	return true;
}

/*
 * Check if the given hypertable is a distributed hypertable.
 */
static bool
is_distributed_hypertable(Oid hypertable_reloid)
{
	Cache *hcache;

	Hypertable *ht =
		ts_hypertable_cache_get_cache_and_entry(hypertable_reloid, CACHE_FLAG_MISSING_OK, &hcache);

	/* perform check before cache is released */
	bool ht_is_distributed = (ht != NULL && hypertable_is_distributed(ht));
	ts_cache_release(hcache);

	return ht_is_distributed;
}

/*
 * Create a new join partition RelOptInfo data structure for a partition. The data
 * structure is based on the parameter joinrel. The paramater is taken as template
 * and adjusted for the partition provided by the parameter data_node_rel.
 */
static RelOptInfo *
create_data_node_joinrel(PlannerInfo *root, RelOptInfo *innerrel, RelOptInfo *joinrel,
						 RelOptInfo *data_node_rel, AppendRelInfo *appinfo)
{
	RelOptInfo *join_partition = palloc(sizeof(RelOptInfo));
	memcpy(join_partition, joinrel, sizeof(RelOptInfo));

	/* Create a new relinfo for the join partition */
	join_partition->fdw_private = NULL;
	TsFdwRelInfo *join_part_fpinfo = fdw_relinfo_create(root,
														join_partition,
														data_node_rel->serverid,
														InvalidOid,
														TS_FDW_RELINFO_REFERENCE_JOIN_PARTITION);

	Assert(join_part_fpinfo != NULL);

	TsFdwRelInfo *data_node_rel_fpinfo = fdw_relinfo_get(data_node_rel);
	Assert(data_node_rel_fpinfo != NULL);

	/* Copy chunk assignment from hypertable */
	join_part_fpinfo->sca = data_node_rel_fpinfo->sca;

	/* Set parameters of the join partition */
	join_partition->relid = data_node_rel->relid;
	join_partition->relids = bms_copy(data_node_rel->relids);
	join_partition->relids = bms_add_members(join_partition->relids, innerrel->relids);
	join_partition->pathlist = NIL;
	join_partition->partial_pathlist = NIL;

	/* Set the reltarget expressions of the partition based on the reltarget expressions
	 * of the join and adjust them for the partition */
	join_partition->reltarget = create_empty_pathtarget();
	join_partition->reltarget->sortgrouprefs = joinrel->reltarget->sortgrouprefs;
	join_partition->reltarget->cost = joinrel->reltarget->cost;
	join_partition->reltarget->width = joinrel->reltarget->width;
#if PG14_GE
	join_partition->reltarget->has_volatile_expr = joinrel->reltarget->has_volatile_expr;
#endif
	join_partition->reltarget->exprs =
		castNode(List,
				 adjust_appendrel_attrs(root, (Node *) joinrel->reltarget->exprs, 1, &appinfo));

	return join_partition;
}

/*
 * Create a JoinPathExtraData data structure for a partition. The new struct is based on the
 * original JoinPathExtraData of the join and the AppendRelInfo of the partition.
 */
static JoinPathExtraData *
create_data_node_joinrel_extra(PlannerInfo *root, JoinPathExtraData *extra, AppendRelInfo *appinfo)
{
	JoinPathExtraData *partition_extra = palloc(sizeof(JoinPathExtraData));
	partition_extra->inner_unique = extra->inner_unique;
	partition_extra->sjinfo = extra->sjinfo;
	partition_extra->semifactors = extra->semifactors;
	partition_extra->param_source_rels = extra->param_source_rels;
	partition_extra->mergeclause_list =
		castNode(List, adjust_appendrel_attrs(root, (Node *) extra->mergeclause_list, 1, &appinfo));
	partition_extra->restrictlist =
		castNode(List, adjust_appendrel_attrs(root, (Node *) extra->restrictlist, 1, &appinfo));

	return partition_extra;
}

/*
 * Generate the paths for a pushed down join. Each data node will be considered as a partition
 * of the join. The join can be pushed down if:
 *
 * (1) The setting "ts_guc_enable_per_data_node_queries" is enabled
 * (2) The outer relation is a distributed hypertable
 * (3) The inner relation is marked as a reference table
 * (4) The join is a left join or an inner join
 *
 * The join will be performed between the multiple DataNodeRels (see function
 * build_data_node_part_rels) and the original innerrel of the join (the reftable).
 *
 * The code is based on PostgreSQL's postgresGetForeignJoinPaths function
 * (version 15.1).
 */
void
data_node_generate_pushdown_join_paths(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel,
									   RelOptInfo *innerrel, JoinType jointype,
									   JoinPathExtraData *extra)
{
	TsFdwRelInfo *fpinfo;
	Path *joinpath;
	double rows = 0;
	int width = 0;
	Cost startup_cost = 0;
	Cost total_cost = 0;
	Path *epq_path = NULL;
	RelOptInfo **hyper_table_rels;
	RelOptInfo **join_partition_rels;
	int nhyper_table_rels;
	List *join_part_rels_list = NIL;
#if PG15_GE
	Bitmapset *data_node_live_rels = NULL;
#endif

	/*
	 * Skip check if the join result has been considered already.
	 */
	if (joinrel->fdw_private)
		return;

	/* Distributed hypertables are not supported by MERGE at the moment. Ensure that
	 * we perform our planning only on SELECTs.
	 */
	if (root->parse->commandType != CMD_SELECT)
		return;

#ifdef ENABLE_DEAD_CODE
	/*
	 * This code does not work for joins with lateral references, since those
	 * must have parameterized paths, which we don't generate yet.
	 */
	if (!bms_is_empty(joinrel->lateral_relids))
		return;
#endif

	/* Get the hypertable from the outer relation. */
	RangeTblEntry *rte_outer = planner_rt_fetch(outerrel->relid, root);

	/* Test that the fetched outer relation is an actual RTE and a
	 * distributed hypertable. */
	if (rte_outer == NULL || !is_distributed_hypertable(rte_outer->relid))
		return;

#ifdef USE_ASSERT_CHECKING
	/* The outerrel has to be distributed. This condition should be always hold
	 * because otherwise we should not start the planning for distributed tables
	 * (see timescaledb_set_join_pathlist_hook).
	 */
	TimescaleDBPrivate *outerrel_private = outerrel->fdw_private;
	Assert(outerrel_private != NULL);
	Assert(outerrel_private->fdw_relation_info != NULL);
#endif

	/* We know at this point that outerrel is a distributed hypertable.
	 * So, outerrel has to be partitioned. */
	Assert(outerrel->nparts > 0);

	/* Test if inner table has a range table. */
	RangeTblEntry *rte_inner = planner_rt_fetch(innerrel->relid, root);
	if (rte_inner == NULL)
		return;

	/* Get current partitioning of the outerrel. */
	hyper_table_rels = outerrel->part_rels;
	nhyper_table_rels = outerrel->nparts;

	Assert(nhyper_table_rels > 0);
	Assert(hyper_table_rels != NULL);

	/*
	 * Create an PgFdwRelationInfo entry that is used to indicate
	 * that the join relation is already considered, so that we won't waste
	 * time in judging safety of join pushdown and adding the same paths again
	 * if found safe. Once we know that this join can be pushed down, we fill
	 * the entry.
	 */
	fpinfo = fdw_relinfo_create(root, joinrel, InvalidOid, InvalidOid, TS_FDW_RELINFO_JOIN);
	Assert(fpinfo->type == TS_FDW_RELINFO_JOIN);

	/* attrs_used is only for base relations. */
	fpinfo->attrs_used = NULL;
	fpinfo->pushdown_safe = false;

	/*
	 * We need the FDW information to get retrieve the information about the
	 * configured reference join tables. So, create the data structure for
	 * the first server. The reference tables are the same for all servers.
	 */
	Oid server_oid = hyper_table_rels[0]->serverid;
	fpinfo->server = GetForeignServer(server_oid);
	apply_fdw_and_server_options(fpinfo);

	if (!is_safe_to_pushdown_reftable_join(root,
										   fpinfo->join_reference_tables,
										   rte_inner,
										   jointype))
	{
		/*
		 * Reset fdw_private to allow further planner calls with different arguments
		 * (e.g., swapped inner and outer relation) to replan the pushdown.
		 */
		pfree(joinrel->fdw_private);
		joinrel->fdw_private = NULL;
		return;
	}

	/*
	 * Join pushdown only works if the data node rels are created in
	 * data_node_scan_add_node_paths during scan planning.
	 */
	if (!ts_guc_enable_per_data_node_queries)
	{
		ereport(DEBUG1,
				(errmsg("join on reference table is not considered to be pushed down because "
						"'enable_per_data_node_queries' GUC is disabled")));

		return;
	}

	/* The inner table can be a distributed hypertable or a plain table. Plain tables don't have
	 * a TsFdwRelInfo at this point. So, it needs to be created.
	 */
	if (innerrel->fdw_private == NULL)
		fdw_relinfo_create(root, innerrel, InvalidOid, InvalidOid, TS_FDW_RELINFO_REFERENCE_TABLE);

	/* Allow pushdown of the inner rel (the reference table) */
	TsFdwRelInfo *fpinfo_i = fdw_relinfo_get(innerrel);
	fpinfo_i->pushdown_safe = true;

	ereport(DEBUG1, (errmsg("try to push down a join on a reference table")));

	join_partition_rels = palloc(sizeof(RelOptInfo *) * nhyper_table_rels);

	/* Create join paths and cost estimations per data node / join relation. */
	for (int i = 0; i < nhyper_table_rels; i++)
	{
		RelOptInfo *data_node_rel = hyper_table_rels[i];
		Assert(data_node_rel);

		/* Adjust join target expression list */
		AppendRelInfo *appinfo = root->append_rel_array[data_node_rel->relid];
		Assert(appinfo != NULL);

		RelOptInfo *join_partition =
			create_data_node_joinrel(root, innerrel, joinrel, data_node_rel, appinfo);
		join_partition_rels[i] = join_partition;
		fpinfo = fdw_relinfo_get(join_partition);

		/* Create a new join path extra for this join partition */
		JoinPathExtraData *partition_extra = create_data_node_joinrel_extra(root, extra, appinfo);

		/* Pushdown the join expressions */
		bool join_pushdown_ok = fdw_pushdown_foreign_join(root,
														  join_partition,
														  jointype,
														  data_node_rel,
														  innerrel,
														  partition_extra);

		/* Join cannot be pushed down */
		if (!join_pushdown_ok)
		{
			ereport(DEBUG1,
					(errmsg(
						"join pushdown on reference table is not supported for the used query")));
			return;
		}

		/*
		 * Compute the selectivity and cost of the local_conds, so we don't have
		 * to do it over again for each path. The best we can do for these
		 * conditions is to estimate selectivity on the basis of local statistics.
		 * The local conditions are applied after the join has been computed on
		 * the remote side like quals in WHERE clause, so pass jointype as
		 * JOIN_INNER.
		 */
		fpinfo->local_conds_sel =
			clauselist_selectivity(root, fpinfo->local_conds, 0, JOIN_INNER, NULL);
		cost_qual_eval(&fpinfo->local_conds_cost, fpinfo->local_conds, root);

		/*
		 * If we are going to estimate costs locally, estimate the join clause
		 * selectivity here while we have special join info.
		 */
		fpinfo->joinclause_sel =
			clauselist_selectivity(root, fpinfo->joinclauses, 0, fpinfo->jointype, extra->sjinfo);

		/* Estimate costs for bare join relation */
		fdw_estimate_path_cost_size(root,
									join_partition,
									NIL,
									&rows,
									&width,
									&startup_cost,
									&total_cost);

		/* Now update this information in the joinrel */
		join_partition->rows = rows;
		join_partition->reltarget->width = width;
		fpinfo->rows = rows;
		fpinfo->width = width;
		fpinfo->startup_cost = startup_cost;
		fpinfo->total_cost = total_cost;

		/*
		 * Create a new join path and add it to the joinrel which represents a
		 * join between foreign tables.
		 */
		joinpath = data_node_join_path_create(root,
											  join_partition,
											  NULL, /* default pathtarget */
											  rows,
											  startup_cost,
											  total_cost,
											  NIL, /* no pathkeys */
											  join_partition->lateral_relids,
											  epq_path,
											  NIL); /* no fdw_private */

		Assert(joinpath != NULL);

		if (!bms_is_empty(fpinfo->sca->chunk_relids))
		{
			/* Add generated path into joinrel by add_path(). */
			fdw_utils_add_path(join_partition, (Path *) joinpath);
			join_part_rels_list = lappend(join_part_rels_list, join_partition);

#if PG15_GE
			data_node_live_rels = bms_add_member(data_node_live_rels, i);
#endif

			/* Consider pathkeys for the join relation */
			fdw_add_paths_with_pathkeys_for_rel(root,
												join_partition,
												epq_path,
												data_node_join_path_create);
		}
		else
			ts_set_dummy_rel_pathlist(join_partition);

		set_cheapest(join_partition);
	}

	Assert(list_length(join_part_rels_list) > 0);

	/* Must keep partitioning info consistent with the join partition paths we have created */
	joinrel->part_rels = join_partition_rels;
	joinrel->nparts = nhyper_table_rels;
#if PG15_GE
	joinrel->live_parts = data_node_live_rels;
#endif

	add_paths_to_append_rel(root, joinrel, join_part_rels_list);

	/* XXX Consider parameterized paths for the join relation */
}

/*
 * Turn chunk append paths into data node append paths.
 *
 * By default, a hypertable produces append plans where each child is a chunk
 * to be scanned. This function computes alternative append plans where each
 * child corresponds to a data node.
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
	Hypertable *ht = ts_hypertable_cache_get_entry(hcache, hyper_rte->relid, CACHE_FLAG_NONE);
	List *data_node_rels_list = NIL;
	RelOptInfo **data_node_rels;
#if PG15_GE
	Bitmapset *data_node_live_rels = NULL;
#endif
	int ndata_node_rels;
	DataNodeChunkAssignments scas;
	int i;

	Assert(NULL != ht);

	if (nchunk_rels <= 0)
	{
		ts_cache_release(hcache);
		return;
	}

	/* Create the RelOptInfo for each data node */
	data_node_rels = build_data_node_part_rels(root, hyper_rel, &ndata_node_rels);

	Assert(ndata_node_rels > 0);

	data_node_chunk_assignments_init(&scas, SCA_STRATEGY_ATTACHED_DATA_NODE, root, ndata_node_rels);

	/* Assign chunks to data nodes */
	data_node_chunk_assignment_assign_chunks(&scas, chunk_rels, nchunk_rels);

	/* Try to push down GROUP BY expressions and bucketing, if possible */
	push_down_group_bys(root, hyper_rel, ht->space, &scas);

	/*
	 * Index path for this relation are not useful by themselves, but we are
	 * going to use them to guess whether the remote scan can use an index for a
	 * given parameterization. This is needed to estimate the cost for
	 * parameterized data node scans. We will reset the pathlist below so these
	 * path are not going to be used.
	 */
	create_index_paths(root, hyper_rel);

	/*
	 * Not sure what parameterizations there could be except the ones used for
	 * join. Still, it's hard to verify from the code because
	 * get_baserel_parampathinfo() is called all over the place w/o checking if
	 * a join would be valid for the given required_outer. So for generating
	 * the parameterized data node scan paths we'll use the explicit list of
	 * ppis valid for joins that we just built, and not the entire
	 * hyper_rel->ppilist.
	 */
	List *ppi_list = build_parameterizations(root, hyper_rel);

	/*
	 * Create estimates and paths for each data node rel based on data node chunk
	 * assignments.
	 */
	for (i = 0; i < ndata_node_rels; i++)
	{
		RelOptInfo *data_node_rel = data_node_rels[i];
		DataNodeChunkAssignment *sca =
			data_node_chunk_assignment_get_or_create(&scas, data_node_rel);
		TsFdwRelInfo *fpinfo;

		/*
		 * Basic stats for data node rels come from the assigned chunks since
		 * data node rels don't correspond to real tables in the system.
		 */
		data_node_rel->pages = sca->pages;
		data_node_rel->tuples = sca->tuples;
		data_node_rel->rows = sca->rows;
		/* The width should be the same as any chunk */
		data_node_rel->reltarget->width = hyper_rel->part_rels[0]->reltarget->width;

		fpinfo = fdw_relinfo_create(root,
									data_node_rel,
									data_node_rel->serverid,
									hyper_rte->relid,
									TS_FDW_RELINFO_HYPERTABLE_DATA_NODE);

		fpinfo->sca = sca;

		if (!bms_is_empty(sca->chunk_relids))
		{
			add_data_node_scan_paths(root, data_node_rel, hyper_rel, ppi_list);
			data_node_rels_list = lappend(data_node_rels_list, data_node_rel);
#if PG15_GE
			data_node_live_rels = bms_add_member(data_node_live_rels, i);
#endif
		}
		else
			ts_set_dummy_rel_pathlist(data_node_rel);

		set_cheapest(data_node_rel);

#ifdef TS_DEBUG
		if (ts_debug_optimizer_flags.show_rel)
			tsl_debug_log_rel_with_paths(root, data_node_rel, (UpperRelationKind *) NULL);
#endif
	}

	Assert(list_length(data_node_rels_list) > 0);

	/* Reset the pathlist since data node scans are preferred */
	hyper_rel->pathlist = NIL;

	/* Must keep partitioning info consistent with the append paths we create */
	hyper_rel->part_rels = data_node_rels;
	hyper_rel->nparts = ndata_node_rels;
#if PG15_GE
	hyper_rel->live_parts = data_node_live_rels;
#endif

	add_paths_to_append_rel(root, hyper_rel, data_node_rels_list);
	ts_cache_release(hcache);
}

/*
 * Creates CustomScanPath for the data node and adds to output_rel. No custom_path is added,
 * i.e., it is encapsulated by the CustomScanPath, so it doesn't inflate continuation of the
 * planning and will be planned locally on the data node.
 */
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

	/* Verify that this is a data node rel */
	if (NULL == fpinfo || fpinfo->type != TS_FDW_RELINFO_HYPERTABLE_DATA_NODE)
		return;

	fdw_create_upper_paths(fpinfo,
						   root,
						   stage,
						   input_rel,
						   output_rel,
						   extra,
						   data_node_scan_upper_path_create);
}

static CustomScanMethods data_node_scan_plan_methods = {
	.CustomName = "DataNodeScan",
	.CreateCustomScanState = data_node_scan_state_create,
};

typedef struct DataNodeScanPath
{
	CustomPath cpath;
} DataNodeScanPath;

static Plan *
data_node_scan_plan_create(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path, List *tlist,
						   List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	ScanInfo scaninfo;

	memset(&scaninfo, 0, sizeof(ScanInfo));

	fdw_scan_info_init(&scaninfo, root, rel, &best_path->path, clauses, NULL);

	cscan->methods = &data_node_scan_plan_methods;
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

	/* Raise an error when system column is requsted, eg. tableoid */
	if (scaninfo.systemcol)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("system columns are not accessible on distributed hypertables with current "
						"settings"),
				 errhint("Set timescaledb.enable_per_data_node_queries=false to query system "
						 "columns.")));

	/* Should have determined the fetcher type by now. */
	Assert(ts_data_node_fetcher_scan_type != AutoFetcherType);

	cscan->custom_private = list_make3(scaninfo.fdw_private,
									   list_make1_int(scaninfo.systemcol),
									   makeInteger(ts_data_node_fetcher_scan_type));

	return &cscan->scan.plan;
}

static CustomPathMethods data_node_scan_path_methods = {
	.CustomName = DATA_NODE_SCAN_PATH_NAME,
	.PlanCustomPath = data_node_scan_plan_create,
};

static Path *
data_node_scan_path_create(PlannerInfo *root, RelOptInfo *rel, PathTarget *target, double rows,
						   Cost startup_cost, Cost total_cost, List *pathkeys,
						   Relids required_outer, Path *fdw_outerpath, List *private)
{
	DataNodeScanPath *scanpath = palloc0(sizeof(DataNodeScanPath));

	if (rel->lateral_relids && !bms_is_subset(rel->lateral_relids, required_outer))
		required_outer = bms_union(required_outer, rel->lateral_relids);

	if (!bms_is_empty(required_outer) && !IS_SIMPLE_REL(rel))
		elog(ERROR, "parameterized foreign joins are not supported yet");

	scanpath->cpath.path.type = T_CustomPath;
	scanpath->cpath.path.pathtype = T_CustomScan;
	scanpath->cpath.custom_paths = fdw_outerpath == NULL ? NIL : list_make1(fdw_outerpath);
	scanpath->cpath.methods = &data_node_scan_path_methods;
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

/*
 * data_node_join_path_create
 *	  Creates a path corresponding to a scan of a foreign join,
 *	  returning the pathnode.
 *
 * There is a usually-sane default for the pathtarget (rel->reltarget),
 * so we let a NULL for "target" select that.
 *
 * The code is based on PostgreSQL's create_foreign_join_path function
 * (version 15.1).
 */
static Path *
data_node_join_path_create(PlannerInfo *root, RelOptInfo *rel, PathTarget *target, double rows,
						   Cost startup_cost, Cost total_cost, List *pathkeys,
						   Relids required_outer, Path *fdw_outerpath, List *private)
{
	DataNodeScanPath *scanpath = palloc0(sizeof(DataNodeScanPath));

#ifdef ENABLE_DEAD_CODE
	if (rel->lateral_relids && !bms_is_subset(rel->lateral_relids, required_outer))
		required_outer = bms_union(required_outer, rel->lateral_relids);

	/*
	 * We should use get_joinrel_parampathinfo to handle parameterized paths,
	 * but the API of this function doesn't support it, and existing
	 * extensions aren't yet trying to build such paths anyway.  For the
	 * moment just throw an error if someone tries it; eventually we should
	 * revisit this.
	 */
	if (!bms_is_empty(required_outer) || !bms_is_empty(rel->lateral_relids))
		elog(ERROR, "parameterized foreign joins are not supported yet");
#endif

	scanpath->cpath.path.type = T_CustomPath;
	scanpath->cpath.path.pathtype = T_CustomScan;
	scanpath->cpath.custom_paths = fdw_outerpath == NULL ? NIL : list_make1(fdw_outerpath);
	scanpath->cpath.methods = &data_node_scan_path_methods;
	scanpath->cpath.path.parent = rel;
	scanpath->cpath.path.pathtarget = target ? target : rel->reltarget;
	scanpath->cpath.path.param_info = NULL; /* XXX see above */
	scanpath->cpath.path.parallel_aware = false;
	scanpath->cpath.path.parallel_safe = rel->consider_parallel;
	scanpath->cpath.path.parallel_workers = 0;
	scanpath->cpath.path.rows = rows;
	scanpath->cpath.path.startup_cost = startup_cost;
	scanpath->cpath.path.total_cost = total_cost;
	scanpath->cpath.path.pathkeys = pathkeys;

	return &scanpath->cpath.path;
}

static Path *
data_node_scan_upper_path_create(PlannerInfo *root, RelOptInfo *rel, PathTarget *target,
								 double rows, Cost startup_cost, Cost total_cost, List *pathkeys,
								 Path *fdw_outerpath, List *private)
{
	DataNodeScanPath *scanpath = palloc0(sizeof(DataNodeScanPath));

	/*
	 * Upper relations should never have any lateral references, since joining
	 * is complete.
	 */
	Assert(bms_is_empty(rel->lateral_relids));

	scanpath->cpath.path.type = T_CustomPath;
	scanpath->cpath.path.pathtype = T_CustomScan;
	scanpath->cpath.custom_paths = fdw_outerpath == NULL ? NIL : list_make1(fdw_outerpath);
	scanpath->cpath.methods = &data_node_scan_path_methods;
	scanpath->cpath.path.parent = rel;
	scanpath->cpath.path.pathtarget = target ? target : rel->reltarget;
	scanpath->cpath.path.param_info = NULL;
	scanpath->cpath.path.parallel_aware = false;
	scanpath->cpath.path.parallel_safe = rel->consider_parallel;
	scanpath->cpath.path.parallel_workers = 0;
	scanpath->cpath.path.rows = rows;
	scanpath->cpath.path.startup_cost = startup_cost;
	scanpath->cpath.path.total_cost = total_cost;
	scanpath->cpath.path.pathkeys = pathkeys;

	return &scanpath->cpath.path;
}
