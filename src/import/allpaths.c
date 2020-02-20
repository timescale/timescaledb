/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#include <postgres.h>
#include <nodes/parsenodes.h>
#include <nodes/plannodes.h>
#include <nodes/nodeFuncs.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/cost.h>
#include <optimizer/clauses.h>
#include <optimizer/planner.h>
#include <optimizer/plancat.h>
#include <optimizer/prep.h>
#include <foreign/fdwapi.h>
#include <utils/rel.h>
#include <access/tsmapi.h>
#include <catalog/pg_proc.h>
#include <miscadmin.h>
#include <math.h>

#include "allpaths.h"

#if PG12_GE
#include <optimizer/appendinfo.h>
#include <optimizer/optimizer.h>
#endif

static void set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte);

/* copied from allpaths.c */
static void
set_foreign_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Call the FDW's GetForeignPaths function to generate path(s) */
	rel->fdwroutine->GetForeignPaths(root, rel, rte->relid);
}

/* copied from allpaths.c */
static void
set_tablesample_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids required_outer;
	Path *path;

	/*
	 * We don't support pushing join clauses into the quals of a samplescan,
	 * but it could still have required parameterization due to LATERAL refs
	 * in its tlist or TABLESAMPLE arguments.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sampled scan */
	path = create_samplescan_path(root, rel, required_outer);

	/*
	 * If the sampling method does not support repeatable scans, we must avoid
	 * plans that would scan the rel multiple times.  Ideally, we'd simply
	 * avoid putting the rel on the inside of a nestloop join; but adding such
	 * a consideration to the planner seems like a great deal of complication
	 * to support an uncommon usage of second-rate sampling methods.  Instead,
	 * if there is a risk that the query might perform an unsafe join, just
	 * wrap the SampleScan in a Materialize node.  We can check for joins by
	 * counting the membership of all_baserels (note that this correctly
	 * counts inheritance trees as single rels).  If we're inside a subquery,
	 * we can't easily check whether a join might occur in the outer query, so
	 * just assume one is possible.
	 *
	 * GetTsmRoutine is relatively expensive compared to the other tests here,
	 * so check repeatable_across_scans last, even though that's a bit odd.
	 */
	if ((root->query_level > 1 || bms_membership(root->all_baserels) != BMS_SINGLETON) &&
		!(GetTsmRoutine(rte->tablesample->tsmhandler)->repeatable_across_scans))
	{
		path = (Path *) create_material_path(rel, path);
	}

	add_path(rel, path);

	/* For the moment, at least, there are no other paths to consider */
}

/* copied from allpaths.c */
static void
create_plain_partial_paths(PlannerInfo *root, RelOptInfo *rel)
{
	int parallel_workers;

	parallel_workers =
		compute_parallel_worker(rel, rel->pages, -1, max_parallel_workers_per_gather);

	/* If any limit was set to zero, the user doesn't want a parallel scan. */
	if (parallel_workers <= 0)
		return;

	/* Add an unordered partial path based on a parallel sequential scan. */
	add_partial_path(rel, create_seqscan_path(root, rel, NULL, parallel_workers));
}

/* copied from allpaths.c */
static void
set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	Relids required_outer;

	/*
	 * We don't support pushing join clauses into the quals of a seqscan, but
	 * it could still have required parameterization due to LATERAL refs in
	 * its tlist.
	 */
	required_outer = rel->lateral_relids;

	/* Consider sequential scan */
	add_path(rel, create_seqscan_path(root, rel, required_outer, 0));

	/* If appropriate, consider parallel sequential scan */
	if (rel->consider_parallel && required_outer == NULL)
		create_plain_partial_paths(root, rel);

	/* Consider index scans */
	create_index_paths(root, rel);

	/* Consider TID scans */
	create_tidscan_paths(root, rel);
}

/* copied from allpaths.c */
void
ts_set_append_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	int parentRTindex = rti;
	List *live_childrels = NIL;
	ListCell *l;

	/*
	 * Generate access paths for each member relation, and remember the
	 * non-dummy children.
	 */
	foreach (l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		/* Re-locate the child RTE and RelOptInfo */
		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];
		childrel = root->simple_rel_array[childRTindex];

		/*
		 * If set_append_rel_size() decided the parent appendrel was
		 * parallel-unsafe at some point after visiting this child rel, we
		 * need to propagate the unsafety marking down to the child, so that
		 * we don't generate useless partial paths for it.
		 */
		if (!rel->consider_parallel)
			childrel->consider_parallel = false;

		/*
		 * Compute the child's access paths.
		 */
		set_rel_pathlist(root, childrel, childRTindex, childRTE);

		/*
		 * If child is dummy, ignore it.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		/* Bubble up childrel's partitioned children. */
		if (rel->part_scheme)
			rel->partitioned_child_rels = list_concat(rel->partitioned_child_rels,
													  list_copy(childrel->partitioned_child_rels));

		/*
		 * Child is live, so add it to the live_childrels list for use below.
		 */
		live_childrels = lappend(live_childrels, childrel);
	}

	/* Add paths to the append relation. */
	add_paths_to_append_rel(root, rel, live_childrels);
}

/* based on the function in allpaths.c, with the irrelevant branches removed */
static void
set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	if (IS_DUMMY_REL(rel))
	{
		/* We already proved the relation empty, so nothing more to do */
	}
	else
	{
		Assert(!rte->inh);
		switch (rel->rtekind)
		{
			case RTE_RELATION:
				if (rte->relkind == RELKIND_FOREIGN_TABLE)
				{
					/* Foreign table */
					set_foreign_pathlist(root, rel, rte);
				}
				else if (rte->tablesample != NULL)
				{
					/* Sampled relation */
					set_tablesample_rel_pathlist(root, rel, rte);
				}
				else
				{
					/* Plain relation */
					set_plain_rel_pathlist(root, rel, rte);
				}
				break;
			case RTE_SUBQUERY:
			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
			case RTE_VALUES:
			case RTE_CTE:
			case RTE_NAMEDTUPLESTORE:
#if PG12_GE
			case RTE_RESULT:
#endif
			default:
				elog(ERROR, "unexpected rtekind: %d", (int) rel->rtekind);
				break;
		}
	}

	/*
	 * Allow a plugin to editorialize on the set of Paths for this base
	 * relation.  It could add new paths (such as CustomPaths) by calling
	 * add_path(), or add_partial_path() if parallel aware.  It could also
	 * delete or modify paths added by the core code.
	 */
	if (set_rel_pathlist_hook)
		(*set_rel_pathlist_hook)(root, rel, rti, rte);

	/*
	 * If this is a baserel, we should normally consider gathering any partial
	 * paths we may have created for it.  We have to do this after calling the
	 * set_rel_pathlist_hook, else it cannot add partial paths to be included
	 * here.
	 *
	 * However, if this is an inheritance child, skip it.  Otherwise, we could
	 * end up with a very large number of gather nodes, each trying to grab
	 * its own pool of workers.  Instead, we'll consider gathering partial
	 * paths for the parent appendrel.
	 *
	 * Also, if this is the topmost scan/join rel (that is, the only baserel),
	 * we postpone gathering until the final scan/join targetlist is available
	 * (see grouping_planner).
	 */
	if (rel->reloptkind == RELOPT_BASEREL && bms_membership(root->all_baserels) != BMS_SINGLETON)
		generate_gather_paths(root, rel, false);

	/* Now find the cheapest of the paths for this rel */
	set_cheapest(rel);

#ifdef OPTIMIZER_DEBUG
	debug_print_rel(root, rel);
#endif
}

#if PG12_GE
/* copied from allpaths.c */
static void
set_dummy_rel_pathlist(RelOptInfo *rel)
{
	/* Set dummy size estimates --- we leave attr_widths[] as zeroes */
	rel->rows = 0;
	rel->reltarget->width = 0;

	/* Discard any pre-existing paths; no further need for them */
	rel->pathlist = NIL;
	rel->partial_pathlist = NIL;

	/* Set up the dummy path */
	add_path(rel,
			 (Path *) create_append_path(NULL,
										 rel,
										 NIL,
										 NIL,
										 NIL,
										 rel->lateral_relids,
										 0,
										 false,
										 NIL,
										 -1));

	/*
	 * We set the cheapest-path fields immediately, just in case they were
	 * pointing at some discarded path.  This is redundant when we're called
	 * from set_rel_size(), but not when called from elsewhere, and doing it
	 * twice is harmless anyway.
	 */
	set_cheapest(rel);
}
#endif /* PG12_GE */

/* copied from allpaths.c */
static void
set_rel_consider_parallel(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/*
	 * The flag has previously been initialized to false, so we can just
	 * return if it becomes clear that we can't safely set it.
	 */
	Assert(!rel->consider_parallel);

	/* Don't call this if parallelism is disallowed for the entire query. */
	Assert(root->glob->parallelModeOK);

	/* This should only be called for baserels and appendrel children. */
	Assert(IS_SIMPLE_REL(rel));

	/* Assorted checks based on rtekind. */
	switch (rte->rtekind)
	{
		case RTE_RELATION:

			/*
			 * Currently, parallel workers can't access the leader's temporary
			 * tables.  We could possibly relax this if the wrote all of its
			 * local buffers at the start of the query and made no changes
			 * thereafter (maybe we could allow hint bit changes), and if we
			 * taught the workers to read them.  Writing a large number of
			 * temporary buffers could be expensive, though, and we don't have
			 * the rest of the necessary infrastructure right now anyway.  So
			 * for now, bail out if we see a temporary table.
			 */
			if (get_rel_persistence(rte->relid) == RELPERSISTENCE_TEMP)
				return;

			/*
			 * Table sampling can be pushed down to workers if the sample
			 * function and its arguments are safe.
			 */
			if (rte->tablesample != NULL)
			{
				char proparallel = func_parallel(rte->tablesample->tsmhandler);

				if (proparallel != PROPARALLEL_SAFE)
					return;
				if (!is_parallel_safe(root, (Node *) rte->tablesample->args))
					return;
			}

			/*
			 * Ask FDWs whether they can support performing a ForeignScan
			 * within a worker.  Most often, the answer will be no.  For
			 * example, if the nature of the FDW is such that it opens a TCP
			 * connection with a remote server, each parallel worker would end
			 * up with a separate connection, and these connections might not
			 * be appropriately coordinated between workers and the leader.
			 */
			if (rte->relkind == RELKIND_FOREIGN_TABLE)
			{
				Assert(rel->fdwroutine);
				if (!rel->fdwroutine->IsForeignScanParallelSafe)
					return;
				if (!rel->fdwroutine->IsForeignScanParallelSafe(root, rel, rte))
					return;
			}

			/*
			 * There are additional considerations for appendrels, which we'll
			 * deal with in set_append_rel_size and set_append_rel_pathlist.
			 * For now, just set consider_parallel based on the rel's own
			 * quals and targetlist.
			 */
			break;

		case RTE_SUBQUERY:

			/*
			 * There's no intrinsic problem with scanning a subquery-in-FROM
			 * (as distinct from a SubPlan or InitPlan) in a parallel worker.
			 * If the subquery doesn't happen to have any parallel-safe paths,
			 * then flagging it as consider_parallel won't change anything,
			 * but that's true for plain tables, too.  We must set
			 * consider_parallel based on the rel's own quals and targetlist,
			 * so that if a subquery path is parallel-safe but the quals and
			 * projection we're sticking onto it are not, we correctly mark
			 * the SubqueryScanPath as not parallel-safe.  (Note that
			 * set_subquery_pathlist() might push some of these quals down
			 * into the subquery itself, but that doesn't change anything.)
			 *
			 * We can't push sub-select containing LIMIT/OFFSET to workers as
			 * there is no guarantee that the row order will be fully
			 * deterministic, and applying LIMIT/OFFSET will lead to
			 * inconsistent results at the top-level.  (In some cases, where
			 * the result is ordered, we could relax this restriction.  But it
			 * doesn't currently seem worth expending extra effort to do so.)
			 */
			{
				Query *subquery = castNode(Query, rte->subquery);

				if (limit_needed(subquery))
					return;
			}
			break;

		case RTE_JOIN:
			/* Shouldn't happen; we're only considering baserels here. */
			Assert(false);
			return;

		case RTE_FUNCTION:
			/* Check for parallel-restricted functions. */
			if (!is_parallel_safe(root, (Node *) rte->functions))
				return;
			break;

		case RTE_TABLEFUNC:
			/* not parallel safe */
			return;

		case RTE_VALUES:
			/* Check for parallel-restricted functions. */
			if (!is_parallel_safe(root, (Node *) rte->values_lists))
				return;
			break;

		case RTE_CTE:

			/*
			 * CTE tuplestores aren't shared among parallel workers, so we
			 * force all CTE scans to happen in the leader.  Also, populating
			 * the CTE would require executing a subplan that's not available
			 * in the worker, might be parallel-restricted, and must get
			 * executed only once.
			 */
			return;

		case RTE_NAMEDTUPLESTORE:

			/*
			 * tuplestore cannot be shared, at least without more
			 * infrastructure to support that.
			 */
			return;
#if PG12_GE
		case RTE_RESULT:
			/* RESULT RTEs, in themselves, are no problem. */
			break;
#endif
	}

	/*
	 * If there's anything in baserestrictinfo that's parallel-restricted, we
	 * give up on parallelizing access to this relation.  We could consider
	 * instead postponing application of the restricted quals until we're
	 * above all the parallelism in the plan tree, but it's not clear that
	 * that would be a win in very many cases, and it might be tricky to make
	 * outer join clauses work correctly.  It would likely break equivalence
	 * classes, too.
	 */
	if (!is_parallel_safe(root, (Node *) rel->baserestrictinfo))
		return;

	/*
	 * Likewise, if the relation's outputs are not parallel-safe, give up.
	 * (Usually, they're just Vars, but sometimes they're not.)
	 */
	if (!is_parallel_safe(root, (Node *) rel->reltarget->exprs))
		return;

	/* We have a winner. */
	rel->consider_parallel = true;
}

/* copied from allpaths.c */
static void
set_append_rel_size(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	int parentRTindex = rti;
	bool has_live_children;
	double parent_rows;
	double parent_size;
	double *parent_attrsizes;
	int nattrs;
	ListCell *l;

	/* Guard against stack overflow due to overly deep inheritance tree. */
	check_stack_depth();

	Assert(IS_SIMPLE_REL(rel));

	/*
	 * Initialize partitioned_child_rels to contain this RT index.
	 *
	 * Note that during the set_append_rel_pathlist() phase, we will bubble up
	 * the indexes of partitioned relations that appear down in the tree, so
	 * that when we've created Paths for all the children, the root
	 * partitioned table's list will contain all such indexes.
	 */
	if (rte->relkind == RELKIND_PARTITIONED_TABLE)
		rel->partitioned_child_rels = list_make1_int(rti);

	/*
	 * If this is a partitioned baserel, set the consider_partitionwise_join
	 * flag; currently, we only consider partitionwise joins with the baserel
	 * if its targetlist doesn't contain a whole-row Var.
	 */
	if (enable_partitionwise_join && rel->reloptkind == RELOPT_BASEREL &&
		rte->relkind == RELKIND_PARTITIONED_TABLE &&
		rel->attr_needed[InvalidAttrNumber - rel->min_attr] == NULL)
		rel->consider_partitionwise_join = true;

	/*
	 * Initialize to compute size estimates for whole append relation.
	 *
	 * We handle width estimates by weighting the widths of different child
	 * rels proportionally to their number of rows.  This is sensible because
	 * the use of width estimates is mainly to compute the total relation
	 * "footprint" if we have to sort or hash it.  To do this, we sum the
	 * total equivalent size (in "double" arithmetic) and then divide by the
	 * total rowcount estimate.  This is done separately for the total rel
	 * width and each attribute.
	 *
	 * Note: if you consider changing this logic, beware that child rels could
	 * have zero rows and/or width, if they were excluded by constraints.
	 */
	has_live_children = false;
	parent_rows = 0;
	parent_size = 0;
	nattrs = rel->max_attr - rel->min_attr + 1;
	parent_attrsizes = (double *) palloc0(nattrs * sizeof(double));

	foreach (l, root->append_rel_list)
	{
		AppendRelInfo *appinfo = (AppendRelInfo *) lfirst(l);
		int childRTindex;
		RangeTblEntry *childRTE;
		RelOptInfo *childrel;
		ListCell *parentvars;
		ListCell *childvars;

		/* append_rel_list contains all append rels; ignore others */
		if (appinfo->parent_relid != parentRTindex)
			continue;

		childRTindex = appinfo->child_relid;
		childRTE = root->simple_rte_array[childRTindex];

		/*
		 * The child rel's RelOptInfo was already created during
		 * add_other_rels_to_query.
		 */
		childrel = find_base_rel(root, childRTindex);
		Assert(childrel->reloptkind == RELOPT_OTHER_MEMBER_REL);

		/* We may have already proven the child to be dummy. */
		if (IS_DUMMY_REL(childrel))
			continue;

		/*
		 * We have to copy the parent's targetlist and quals to the child,
		 * with appropriate substitution of variables.  However, the
		 * baserestrictinfo quals were already copied/substituted when the
		 * child RelOptInfo was built.  So we don't need any additional setup
		 * before applying constraint exclusion.
		 */
		if (relation_excluded_by_constraints(root, childrel, childRTE))
		{
			/*
			 * This child need not be scanned, so we can omit it from the
			 * appendrel.
			 */
			set_dummy_rel_pathlist(childrel);
			continue;
		}

		/*
		 * Constraint exclusion failed, so copy the parent's join quals and
		 * targetlist to the child, with appropriate variable substitutions.
		 *
		 * NB: the resulting childrel->reltarget->exprs may contain arbitrary
		 * expressions, which otherwise would not occur in a rel's targetlist.
		 * Code that might be looking at an appendrel child must cope with
		 * such.  (Normally, a rel's targetlist would only include Vars and
		 * PlaceHolderVars.)  XXX we do not bother to update the cost or width
		 * fields of childrel->reltarget; not clear if that would be useful.
		 */
		childrel->joininfo =
			(List *) adjust_appendrel_attrs(root, (Node *) rel->joininfo, 1, &appinfo);
		childrel->reltarget->exprs =
			(List *) adjust_appendrel_attrs(root, (Node *) rel->reltarget->exprs, 1, &appinfo);

		/*
		 * We have to make child entries in the EquivalenceClass data
		 * structures as well.  This is needed either if the parent
		 * participates in some eclass joins (because we will want to consider
		 * inner-indexscan joins on the individual children) or if the parent
		 * has useful pathkeys (because we should try to build MergeAppend
		 * paths that produce those sort orderings).
		 */
		if (rel->has_eclass_joins || has_useful_pathkeys(root, rel))
			add_child_rel_equivalences(root, appinfo, rel, childrel);
		childrel->has_eclass_joins = rel->has_eclass_joins;

		/*
		 * Note: we could compute appropriate attr_needed data for the child's
		 * variables, by transforming the parent's attr_needed through the
		 * translated_vars mapping.  However, currently there's no need
		 * because attr_needed is only examined for base relations not
		 * otherrels.  So we just leave the child's attr_needed empty.
		 */

		/*
		 * If we consider partitionwise joins with the parent rel, do the same
		 * for partitioned child rels.
		 *
		 * Note: here we abuse the consider_partitionwise_join flag by setting
		 * it for child rels that are not themselves partitioned.  We do so to
		 * tell try_partitionwise_join() that the child rel is sufficiently
		 * valid to be used as a per-partition input, even if it later gets
		 * proven to be dummy.  (It's not usable until we've set up the
		 * reltarget and EC entries, which we just did.)
		 */
		if (rel->consider_partitionwise_join)
			childrel->consider_partitionwise_join = true;

		/*
		 * If parallelism is allowable for this query in general, see whether
		 * it's allowable for this childrel in particular.  But if we've
		 * already decided the appendrel is not parallel-safe as a whole,
		 * there's no point in considering parallelism for this child.  For
		 * consistency, do this before calling set_rel_size() for the child.
		 */
		if (root->glob->parallelModeOK && rel->consider_parallel)
			set_rel_consider_parallel(root, childrel, childRTE);

		/*
		 * Compute the child's size.
		 */
		ts_set_rel_size(root, childrel, childRTindex, childRTE);

		/*
		 * It is possible that constraint exclusion detected a contradiction
		 * within a child subquery, even though we didn't prove one above. If
		 * so, we can skip this child.
		 */
		if (IS_DUMMY_REL(childrel))
			continue;

		/* We have at least one live child. */
		has_live_children = true;

		/*
		 * If any live child is not parallel-safe, treat the whole appendrel
		 * as not parallel-safe.  In future we might be able to generate plans
		 * in which some children are farmed out to workers while others are
		 * not; but we don't have that today, so it's a waste to consider
		 * partial paths anywhere in the appendrel unless it's all safe.
		 * (Child rels visited before this one will be unmarked in
		 * set_append_rel_pathlist().)
		 */
		if (!childrel->consider_parallel)
			rel->consider_parallel = false;

		/*
		 * Accumulate size information from each live child.
		 */
		Assert(childrel->rows > 0);

		parent_rows += childrel->rows;
		parent_size += childrel->reltarget->width * childrel->rows;

		/*
		 * Accumulate per-column estimates too.  We need not do anything for
		 * PlaceHolderVars in the parent list.  If child expression isn't a
		 * Var, or we didn't record a width estimate for it, we have to fall
		 * back on a datatype-based estimate.
		 *
		 * By construction, child's targetlist is 1-to-1 with parent's.
		 */
		forboth (parentvars, rel->reltarget->exprs, childvars, childrel->reltarget->exprs)
		{
			Var *parentvar = (Var *) lfirst(parentvars);
			Node *childvar = (Node *) lfirst(childvars);

			if (IsA(parentvar, Var))
			{
				int pndx = parentvar->varattno - rel->min_attr;
				int32 child_width = 0;

				if (IsA(childvar, Var) && ((Var *) childvar)->varno == childrel->relid)
				{
					int cndx = ((Var *) childvar)->varattno - childrel->min_attr;

					child_width = childrel->attr_widths[cndx];
				}
				if (child_width <= 0)
					child_width = get_typavgwidth(exprType(childvar), exprTypmod(childvar));
				Assert(child_width > 0);
				parent_attrsizes[pndx] += child_width * childrel->rows;
			}
		}
	}

	if (has_live_children)
	{
		/*
		 * Save the finished size estimates.
		 */
		int i;

		Assert(parent_rows > 0);
		rel->rows = parent_rows;
		rel->reltarget->width = rint(parent_size / parent_rows);
		for (i = 0; i < nattrs; i++)
			rel->attr_widths[i] = rint(parent_attrsizes[i] / parent_rows);

		/*
		 * Set "raw tuples" count equal to "rows" for the appendrel; needed
		 * because some places assume rel->tuples is valid for any baserel.
		 */
		rel->tuples = parent_rows;

		/*
		 * Note that we leave rel->pages as zero; this is important to avoid
		 * double-counting the appendrel tree in total_table_pages.
		 */
	}
	else
	{
		/*
		 * All children were excluded by constraints, so mark the whole
		 * appendrel dummy.  We must do this in this phase so that the rel's
		 * dummy-ness is visible when we generate paths for other rels.
		 */
		set_dummy_rel_pathlist(rel);
	}

	pfree(parent_attrsizes);
}

/* copied from allpaths.c */
static void
set_foreign_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/* Mark rel with estimated output rows, width, etc */
	set_foreign_size_estimates(root, rel);

	/* Let FDW adjust the size estimates, if it can */
	rel->fdwroutine->GetForeignRelSize(root, rel, rte->relid);

	/* ... but do not let it set the rows estimate to zero */
	rel->rows = clamp_row_est(rel->rows);
}

/* copied from allpaths.c */
static void
set_tablesample_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	TableSampleClause *tsc = rte->tablesample;
	TsmRoutine *tsm;
	BlockNumber pages;
	double tuples;

	/*
	 * Test any partial indexes of rel for applicability.  We must do this
	 * first since partial unique indexes can affect size estimates.
	 */
	check_index_predicates(root, rel);

	/*
	 * Call the sampling method's estimation function to estimate the number
	 * of pages it will read and the number of tuples it will return.  (Note:
	 * we assume the function returns sane values.)
	 */
	tsm = GetTsmRoutine(tsc->tsmhandler);
	tsm->SampleScanGetSampleSize(root, rel, tsc->args, &pages, &tuples);

	/*
	 * For the moment, because we will only consider a SampleScan path for the
	 * rel, it's okay to just overwrite the pages and tuples estimates for the
	 * whole relation.  If we ever consider multiple path types for sampled
	 * rels, we'll need more complication.
	 */
	rel->pages = pages;
	rel->tuples = tuples;

	/* Mark rel with estimated output rows, width, etc */
	set_baserel_size_estimates(root, rel);
}

/* copied from allpaths.c */
static void
set_plain_rel_size(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
{
	/*
	 * Test any partial indexes of rel for applicability.  We must do this
	 * first since partial unique indexes can affect size estimates.
	 */
	check_index_predicates(root, rel);

	/* Mark rel with estimated output rows, width, etc */
	set_baserel_size_estimates(root, rel);
}

/* extracted from the same function in allpaths.c
 * assumes that the root table is either excluded by constraints, or is an
 * inheritance base table, and that chunks are regular tables
 */
void
ts_set_rel_size(PlannerInfo *root, RelOptInfo *rel, Index rti, RangeTblEntry *rte)
{
	if (rel->reloptkind == RELOPT_BASEREL && relation_excluded_by_constraints(root, rel, rte))
	{
		/*
		 * We proved we don't need to scan the rel via constraint exclusion,
		 * so set up a single dummy path for it.  Here we only check this for
		 * regular baserels; if it's an otherrel, CE was already checked in
		 * set_append_rel_size().
		 *
		 * In this case, we go ahead and set up the relation's path right away
		 * instead of leaving it for set_rel_pathlist to do.  This is because
		 * we don't have a convention for marking a rel as dummy except by
		 * assigning a dummy path to it.
		 */
		set_dummy_rel_pathlist(rel);
	}
	else if (rte->inh)
	{
		/* It's an "append relation", process accordingly */
		set_append_rel_size(root, rel, rti, rte);
	}
	else
	{
		switch (rel->rtekind)
		{
			case RTE_RELATION:
				if (rte->relkind == RELKIND_FOREIGN_TABLE)
				{
					/* Foreign table */
					set_foreign_size(root, rel, rte);
				}
				else if (rte->relkind == RELKIND_PARTITIONED_TABLE)
				{
					/*
					 * We could get here if asked to scan a partitioned table
					 * with ONLY.  In that case we shouldn't scan any of the
					 * partitions, so mark it as a dummy rel.
					 */
					set_dummy_rel_pathlist(rel);
				}
				else if (rte->tablesample != NULL)
				{
					/* Sampled relation */
					set_tablesample_rel_size(root, rel, rte);
				}
				else
				{
					/* Plain relation */
					set_plain_rel_size(root, rel, rte);
				}
				break;
			case RTE_SUBQUERY:
			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
			case RTE_VALUES:
			case RTE_CTE:
			case RTE_NAMEDTUPLESTORE:
#if PG12_GE
			case RTE_RESULT:
#endif
			default:
				elog(ERROR, "unexpected rtekind: %d", (int) rel->rtekind);
				break;
		}
	}

	/*
	 * We insist that all non-dummy rels have a nonzero rowcount estimate.
	 */
	Assert(rel->rows > 0 || IS_DUMMY_REL(rel));
}
