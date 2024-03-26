/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/pathnodes.h>
#include <nodes/pg_list.h>
#include <optimizer/appendinfo.h>
#include <optimizer/clauses.h>
#include <optimizer/cost.h>
#include <optimizer/optimizer.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/planner.h>
#include <optimizer/prep.h>
#include <optimizer/tlist.h>
#include <parser/parse_func.h>
#include <utils/lsyscache.h>

#include "cross_module_fn.h"
#include "debug_assert.h"
#include "partialize.h"
#include "planner.h"
#include "gapfill.h"
#include "nodes/print.h"
#include "extension_constants.h"
#include "utils.h"
#include "estimate.h"
#include "nodes/chunk_append/chunk_append.h"
#include "import/planner.h"

#define TS_PARTIALFN "partialize_agg"

typedef struct PartializeWalkerState
{
	bool found_partialize;
	bool found_non_partial_agg;
	bool looking_for_agg;
	Oid fnoid;
	PartializeAggFixAggref fix_aggref;
} PartializeWalkerState;

/*
 * Look for the partialize function in a target list and mark the wrapped
 * aggregate as a partial aggregate.
 *
 * The partialize function is an expression of the form:
 *
 * _timescaledb_functions.partialize_agg(avg(temp))
 *
 * where avg(temp) can be replaced by any aggregate that can be partialized.
 *
 * When such an expression is found, this function will mark the Aggref node
 * for the aggregate as partial.
 */
static bool
check_for_partialize_function_call(Node *node, PartializeWalkerState *state)
{
	if (node == NULL)
		return false;

	/*
	 * If the last node we saw was partialize, the next one must be aggregate
	 * we're partializing
	 */
	if (state->looking_for_agg && !IsA(node, Aggref))
		elog(ERROR, "the input to partialize must be an aggregate");

	if (IsA(node, Aggref))
	{
		Aggref *aggref = castNode(Aggref, node);

		if (state->looking_for_agg)
		{
			state->looking_for_agg = false;

			if (state->fix_aggref != TS_DO_NOT_FIX_AGGSPLIT)
			{
				if (state->fix_aggref == TS_FIX_AGGSPLIT_SIMPLE &&
					aggref->aggsplit == AGGSPLIT_SIMPLE)
				{
					aggref->aggsplit = AGGSPLIT_INITIAL_SERIAL;
				}
				else if (state->fix_aggref == TS_FIX_AGGSPLIT_FINAL &&
						 aggref->aggsplit == AGGSPLIT_FINAL_DESERIAL)
				{
					aggref->aggsplit = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE |
									   AGGSPLITOP_SERIALIZE | AGGSPLITOP_SKIPFINAL;
				}

				if (aggref->aggtranstype == INTERNALOID)
					aggref->aggtype = BYTEAOID;
				else
					aggref->aggtype = aggref->aggtranstype;
			}
		}

		/* We currently cannot handle cases like
		 *     SELECT sum(i), partialize(sum(i)) ...
		 *
		 * We check for non-partial aggs to ensure that if any of the aggregates
		 * in a statement are partialized, all of them have to be.
		 */
		else if (aggref->aggsplit != AGGSPLIT_INITIAL_SERIAL)
			state->found_non_partial_agg = true;
	}
	else if (IsA(node, FuncExpr) && ((FuncExpr *) node)->funcid == state->fnoid)
	{
		state->found_partialize = true;
		state->looking_for_agg = true;
	}

	return expression_tree_walker(node, check_for_partialize_function_call, state);
}

bool
has_partialize_function(Node *node, PartializeAggFixAggref fix_aggref)
{
	Oid partialfnoid = InvalidOid;
	Oid argtyp[] = { ANYELEMENTOID };

	PartializeWalkerState state = { .found_partialize = false,
									.found_non_partial_agg = false,
									.looking_for_agg = false,
									.fix_aggref = fix_aggref,
									.fnoid = InvalidOid };
	List *name = list_make2(makeString(FUNCTIONS_SCHEMA_NAME), makeString(TS_PARTIALFN));

	partialfnoid = LookupFuncName(name, lengthof(argtyp), argtyp, false);
	Assert(OidIsValid(partialfnoid));
	state.fnoid = partialfnoid;
	check_for_partialize_function_call(node, &state);

	if (state.found_partialize && state.found_non_partial_agg)
		elog(ERROR, "cannot mix partialized and non-partialized aggregates in the same statement");

	return state.found_partialize;
}

/*
 * Modify all AggPaths in relation to use partial aggregation.
 *
 * Note that there can be both parallel (split) paths and non-parallel
 * (non-split) paths suggested at this stage, but all of them refer to the
 * same Aggrefs. Depending on the Path picked, the Aggrefs are "fixed up" by
 * the PostgreSQL planner at a later stage in planner (in setrefs.c) to match
 * the choice of Path. For this reason, it is not possible to modify Aggrefs
 * at this stage AND keep both type of Paths. Therefore, if a split Path is
 * found, then prune the non-split path.
 */
static bool
partialize_agg_paths(RelOptInfo *rel)
{
	ListCell *lc;
	bool has_combine = false;
	List *aggsplit_simple_paths = NIL;
	List *aggsplit_final_paths = NIL;
	List *other_paths = NIL;

	foreach (lc, rel->pathlist)
	{
		Path *path = lfirst(lc);

		if (IsA(path, AggPath))
		{
			AggPath *agg = castNode(AggPath, path);

			if (agg->aggsplit == AGGSPLIT_SIMPLE)
			{
				agg->aggsplit = AGGSPLIT_INITIAL_SERIAL;
				aggsplit_simple_paths = lappend(aggsplit_simple_paths, path);
			}
			else if (agg->aggsplit == AGGSPLIT_FINAL_DESERIAL)
			{
				has_combine = true;
				aggsplit_final_paths = lappend(aggsplit_final_paths, path);
			}
			else
			{
				other_paths = lappend(other_paths, path);
			}
		}
		else
		{
			other_paths = lappend(other_paths, path);
		}
	}

	if (aggsplit_final_paths != NIL)
		rel->pathlist = list_concat(other_paths, aggsplit_final_paths);
	else
		rel->pathlist = list_concat(other_paths, aggsplit_simple_paths);

	return has_combine;
}

/* Helper function to find the first node of the provided type in the pathlist of the relation */
static Node *
find_node(const RelOptInfo *relation, NodeTag type)
{
	ListCell *lc;
	foreach (lc, relation->pathlist)
	{
		Node *node = lfirst(lc);
		if (nodeTag(node) == type)
			return node;
	}

	return NULL;
}

/* Check if the relation already has a min/max path */
static bool
has_min_max_agg_path(const RelOptInfo *relation)
{
	return find_node(relation, T_MinMaxAggPath) != NULL;
}

/*
 * Get an an existing aggregation path for the given relation or NULL if no aggregation path exists.
 */
static AggPath *
get_existing_agg_path(const RelOptInfo *relation)
{
	Node *node = find_node(relation, T_AggPath);
	return node ? castNode(AggPath, node) : NULL;
}

/*
 * Get all subpaths from a Append, MergeAppend, or ChunkAppend path
 */
static List *
get_subpaths_from_append_path(Path *path, bool handle_gather_path)
{
	if (IsA(path, AppendPath))
	{
		AppendPath *append_path = castNode(AppendPath, path);
		return append_path->subpaths;
	}
	else if (IsA(path, MergeAppendPath))
	{
		MergeAppendPath *merge_append_path = castNode(MergeAppendPath, path);
		return merge_append_path->subpaths;
	}
	else if (ts_is_chunk_append_path(path))
	{
		CustomPath *custom_path = castNode(CustomPath, path);
		return custom_path->custom_paths;
	}
	else if (handle_gather_path && IsA(path, GatherPath))
	{
		return get_subpaths_from_append_path(castNode(GatherPath, path)->subpath, false);
	}

	/* Aggregation push-down is not supported for other path types so far */
	return NIL;
}

/*
 * Copy an AppendPath and set new subpaths.
 */
static AppendPath *
copy_append_path(AppendPath *path, List *subpaths, PathTarget *pathtarget)
{
	AppendPath *newPath = makeNode(AppendPath);
	memcpy(newPath, path, sizeof(AppendPath));

	Assert(list_length(newPath->subpaths) == list_length(subpaths));
	newPath->subpaths = subpaths;
	newPath->path.pathtarget = copy_pathtarget(pathtarget);

	/*
	 * Note that we can't just call cost_append here, because when there is only
	 * one child path, postgres inherits its pathkeys for append, but doesn't
	 * reset append's parallel-aware flags, which leads to an assertion failure
	 * in cost_append(). Below is a copy of how the create_append_path() handles
	 * this case:
	 *
	 * If there's exactly one child path, the Append is a no-op and will be
	 * discarded later (in setrefs.c); therefore, we can inherit the child's
	 * size and cost, as well as its pathkeys if any (overriding whatever the
	 * caller might've said).  Otherwise, we must do the normal costsize
	 * calculation.
	 */
	if (list_length(newPath->subpaths) == 1)
	{
		Path	   *child = (Path *) linitial(newPath->subpaths);

		newPath->path.rows = child->rows;
		newPath->path.startup_cost = child->startup_cost;
		newPath->path.total_cost = child->total_cost;
		newPath->path.pathkeys = child->pathkeys;
	}
	else
		cost_append(newPath);

	return newPath;
}

/*
 * Copy a MergeAppendPath and set new subpaths.
 */
static MergeAppendPath *
copy_merge_append_path(PlannerInfo *root, MergeAppendPath *path, List *subpaths,
					   PathTarget *pathtarget)
{
	MergeAppendPath *newPath = create_merge_append_path_compat(root,
															   path->path.parent,
															   subpaths,
															   path->path.pathkeys,
															   NULL,
															   path->partitioned_rels);

#if PG14_LT
	newPath->partitioned_rels = list_copy(path->partitioned_rels);
#endif

	newPath->path.param_info = path->path.param_info;
	newPath->path.pathtarget = copy_pathtarget(pathtarget);

	return newPath;
}

/*
 * Copy an append-like path and set new subpaths
 */
static Path *
copy_append_like_path(PlannerInfo *root, Path *path, List *new_subpaths, PathTarget *pathtarget)
{
	if (IsA(path, AppendPath))
	{
		AppendPath *append_path = castNode(AppendPath, path);
		AppendPath *new_append_path = copy_append_path(append_path, new_subpaths, pathtarget);
		return &new_append_path->path;
	}
	else if (IsA(path, MergeAppendPath))
	{
		MergeAppendPath *merge_append_path = castNode(MergeAppendPath, path);
		MergeAppendPath *new_merge_append_path =
			copy_merge_append_path(root, merge_append_path, new_subpaths, pathtarget);
		return &new_merge_append_path->path;
	}
	else if (ts_is_chunk_append_path(path))
	{
		CustomPath *custom_path = castNode(CustomPath, path);
		ChunkAppendPath *chunk_append_path = (ChunkAppendPath *) custom_path;
		ChunkAppendPath *new_chunk_append_path =
			ts_chunk_append_path_copy(chunk_append_path, new_subpaths, pathtarget);
		return &new_chunk_append_path->cpath.path;
	}

	/* Should never happen, already checked by caller */
	Ensure(false, "unknown path type");
	pg_unreachable();
}

/*
 * Generate a partially sorted aggregated agg path on top of a path
 */
static AggPath *
create_sorted_partial_agg_path(PlannerInfo *root, Path *path, PathTarget *target,
							   double d_num_groups, GroupPathExtraData *extra_data)
{
	Query *parse = root->parse;

	/* Determine costs for aggregations */
	AggClauseCosts *agg_partial_costs = &extra_data->agg_partial_costs;

	bool is_sorted = pathkeys_contained_in(root->group_pathkeys, path->pathkeys);

	if (!is_sorted)
	{
		path = (Path *) create_sort_path(root, path->parent, path, root->group_pathkeys, -1.0);
	}

	AggPath *sorted_agg_path = create_agg_path(root,
											   path->parent,
											   path,
											   target,
											   parse->groupClause ? AGG_SORTED : AGG_PLAIN,
											   AGGSPLIT_INITIAL_SERIAL,
#if PG16_LT
											   parse->groupClause,
#else
											   root->processed_groupClause,
#endif
											   NIL,
											   agg_partial_costs,
											   d_num_groups);

	return sorted_agg_path;
}

/*
 * Generate a partially hashed aggregated add path on top of a path
 */
static AggPath *
create_hashed_partial_agg_path(PlannerInfo *root, Path *path, PathTarget *target,
							   double d_num_groups, GroupPathExtraData *extra_data)
{
	/* Determine costs for aggregations */
	AggClauseCosts *agg_partial_costs = &extra_data->agg_partial_costs;

	AggPath *hash_path = create_agg_path(root,
										 path->parent,
										 path,
										 target,
										 AGG_HASHED,
										 AGGSPLIT_INITIAL_SERIAL,
#if PG16_LT
										 root->parse->groupClause,
#else
										 root->processed_groupClause,
#endif
										 NIL,
										 agg_partial_costs,
										 d_num_groups);
	return hash_path;
}

/*
 * Add partially aggregated subpath
 */
static void
add_partially_aggregated_subpaths(PlannerInfo *root, PathTarget *pathtarget_before_agg,
								  PathTarget *partial_grouping_target, double d_num_groups,
								  GroupPathExtraData *extra_data, bool can_sort, bool can_hash,
								  Path *subpath, List **sorted_paths, List **hashed_paths)
{
	/* Translate targetlist for partition */
	AppendRelInfo *appinfo = ts_get_appendrelinfo(root, subpath->parent->relid, false);
	PathTarget *chunk_grouping_target = copy_pathtarget(partial_grouping_target);
	chunk_grouping_target->exprs =
		castNode(List, adjust_appendrel_attrs(root, (Node *) chunk_grouping_target->exprs, 1, &appinfo));

//	fprintf(stderr, "original subpath target:\n");
//	my_print(subpath->pathtarget);

	/* In declarative partitioning planning, this is done by appy_scanjoin_target_to_path */
//	Assert(list_length(subpath->pathtarget->exprs) == list_length(parent_path->pathtarget->exprs));
//	subpath->pathtarget->sortgrouprefs = parent_path->pathtarget->sortgrouprefs;
//	subpath->pathtarget = pathtarget_before_agg;

	PathTarget *chunk_target_before_agg = copy_pathtarget(pathtarget_before_agg);
	chunk_target_before_agg->exprs =
		castNode(List, adjust_appendrel_attrs(root, (Node *) chunk_target_before_agg->exprs, 1, &appinfo));

	Path *projected = apply_projection_to_path(root, subpath->parent, subpath,
		chunk_target_before_agg);


	if (can_sort)
	{
		AggPath *agg_path =
			create_sorted_partial_agg_path(root, projected, chunk_grouping_target, d_num_groups, extra_data);

		Path *pushed_down = ts_cm_functions->push_down_aggregation(root, agg_path, projected);
		if (pushed_down)
		{
			*sorted_paths = lappend(*sorted_paths, projected);
		}
		else
		{
			*sorted_paths = lappend(*sorted_paths, (Path *) agg_path);
		}
	}

	if (can_hash)
	{
		AggPath *agg_path =
			create_hashed_partial_agg_path(root, projected, chunk_grouping_target, d_num_groups, extra_data);

		Path *pushed_down = ts_cm_functions->push_down_aggregation(root, agg_path, projected);
		if (pushed_down)
		{
			*hashed_paths = lappend(*hashed_paths, projected);
		}
		else
		{
			*hashed_paths = lappend(*hashed_paths, (Path *) agg_path);
		}
	}
}

/*
 * Generate a total aggregation path for partial aggregations.
 *
 * The generated paths contain partial aggregations (created by using AGGSPLIT_INITIAL_SERIAL).
 * These aggregations need to be finished by the caller by adding a node that performs the
 * AGGSPLIT_FINAL_DESERIAL step.
 */
static void
generate_agg_pushdown_path(PlannerInfo *root, Path *cheapest_total_path, RelOptInfo *output_rel,
						   RelOptInfo *partially_grouped_rel, PathTarget *grouping_target,
						   PathTarget *partial_grouping_target, bool can_sort, bool can_hash,
						   double d_num_groups, GroupPathExtraData *extra_data)
{
	/* Get subpaths */
	List *subpaths = get_subpaths_from_append_path(cheapest_total_path, false);

	/* No subpaths available or unsupported append node */
	if (subpaths == NIL)
	{
		return;
	}

	if (list_length(subpaths) < 2)
	{
		/*
		 * Doesn't make sense to add per-chunk aggregation paths if there's
		 * only one chunk.
		 */
		return;
	}

//	fprintf(stderr, "grouping target:\n");
//	my_print(grouping_target);
//	fprintf(stderr, "partial grouping target:\n");
//	my_print(partial_grouping_target);
//
	PathTarget *pathtarget_before_agg = cheapest_total_path->pathtarget;
//	fprintf(stderr, "pathtarget before agg:\n");
//	my_print(pathtarget_before_agg);

	if (list_length(subpaths) < 2)
	{
		/*
		 * Doesn't make sense to add per-chunk aggregation paths if there's
		 * only one chunk.
		 */
		return;
	}

	/* Generate agg paths on top of the append children */
	List *sorted_subpaths = NIL;
	List *hashed_subpaths = NIL;

	ListCell *lc;
	foreach (lc, subpaths)
	{
		Path *subpath = lfirst(lc);

		/* Check if we have an append path under an append path (e.g., a partially compressed
		 * chunk. The first append path merges the chunk results. The second append path merges the
		 * uncompressed and the compressed part of the chunk).
		 *
		 * In this case, the partial aggregation needs to be pushed down below the lower
		 * append path.
		 */
		List *subsubpaths = get_subpaths_from_append_path(subpath, false);

		if (subsubpaths != NIL)
		{
			List *sorted_subsubpaths = NIL;
			List *hashed_subsubpaths = NIL;

			ListCell *lc2;
			foreach (lc2, subsubpaths)
			{
				Path *subsubpath = lfirst(lc2);

				add_partially_aggregated_subpaths(root,
												  pathtarget_before_agg,
												  partial_grouping_target,
												  d_num_groups,
												  extra_data,
												  can_sort,
												  can_hash,
												  subsubpath,
												  &sorted_subsubpaths /* Result path */,
												  &hashed_subsubpaths /* Result path */);
			}

			if (can_sort)
			{
				sorted_subpaths = lappend(sorted_subpaths,
										  copy_append_like_path(root,
																subpath,
																sorted_subsubpaths,
																subpath->pathtarget));
			}

			if (can_hash)
			{
				hashed_subpaths = lappend(hashed_subpaths,
										  copy_append_like_path(root,
																subpath,
																hashed_subsubpaths,
																subpath->pathtarget));
			}
		}
		else
		{
			add_partially_aggregated_subpaths(root,
											  pathtarget_before_agg,
											  partial_grouping_target,
											  d_num_groups,
											  extra_data,
											  can_sort,
											  can_hash,
											  subpath,
											  &sorted_subpaths /* Result paths */,
											  &hashed_subpaths /* Result paths */);
		}
	}

	/* Create new append paths */
	if (sorted_subpaths != NIL)
	{
		add_path(partially_grouped_rel,
				 copy_append_like_path(root,
									   cheapest_total_path,
									   sorted_subpaths,
									   partial_grouping_target));
	}

	if (hashed_subpaths != NIL)
	{
		add_path(partially_grouped_rel,
				 copy_append_like_path(root,
									   cheapest_total_path,
									   hashed_subpaths,
									   partial_grouping_target));
	}
}

/*
 * Generate a partial aggregation path for chunk-wise partial aggregations.

 * This function does almost the same as generate_agg_pushdown_path(). In contrast, it processes a
 * partial_path (paths that are usually used in parallel plans) of the input relation, pushes down
 * the aggregation in this path and adds a gather node on top of the partial plan. Therefore, the
 * push-down of the partial aggregates also works in parallel plans.
 *
 * Note: The PostgreSQL terminology can cause some confusion here. Partial paths are usually used by
 * PostgreSQL to distribute work between parallel workers. This has nothing to do with the partial
 * aggregation we are creating in the function.
 */
static void
generate_partial_agg_pushdown_path(PlannerInfo *root, Path *cheapest_partial_path,
								   RelOptInfo *output_rel, RelOptInfo *partially_grouped_rel,
								   PathTarget *grouping_target, PathTarget *partial_grouping_target,
								   bool can_sort, bool can_hash, double d_num_groups,
								   GroupPathExtraData *extra_data)
{
	/* Get subpaths */
	List *subpaths = get_subpaths_from_append_path(cheapest_partial_path, false);

	/* No subpaths available or unsupported append node */
	if (subpaths == NIL)
		return;

	PathTarget *pathtarget_before_agg = cheapest_partial_path->pathtarget;

	if (list_length(subpaths) < 2)
	{
		/*
		 * Doesn't make sense to add per-chunk aggregation paths if there's
		 * only one chunk.
		 */
		return;
	}
	/* Generate agg paths on top of the append children */
	ListCell *lc;
	List *sorted_subpaths = NIL;
	List *hashed_subpaths = NIL;

	foreach (lc, subpaths)
	{
		Path *subpath = lfirst(lc);

		Assert(subpath->parallel_safe);

		/* There should be no nested append paths in the partial paths to construct the upper
		 * relation */
		Assert(get_subpaths_from_append_path(subpath, false) == NIL);

		add_partially_aggregated_subpaths(root,
										  pathtarget_before_agg,
										  partial_grouping_target,
										  d_num_groups,
										  extra_data,
										  can_sort,
										  can_hash,
										  subpath,
										  &sorted_subpaths /* Result paths */,
										  &hashed_subpaths /* Result paths */);
	}

	/* Create new append paths */
	if (sorted_subpaths != NIL)
	{
		add_partial_path(partially_grouped_rel,
						 copy_append_like_path(root,
											   cheapest_partial_path,
											   sorted_subpaths,
											   partial_grouping_target));
	}

	if (hashed_subpaths != NIL)
	{
		add_partial_path(partially_grouped_rel,
						 copy_append_like_path(root,
											   cheapest_partial_path,
											   hashed_subpaths,
											   partial_grouping_target));
	}

	/* Finish the partial paths (just added by add_partial_path to partially_grouped_rel in this
	 * function) by adding a gather node and add this path to the partially_grouped_rel using
	 * add_path). */
	foreach (lc, partially_grouped_rel->partial_pathlist)
	{
		Path *append_path = lfirst(lc);
		double total_groups = append_path->rows * append_path->parallel_workers;

		Path *gather_path = (Path *) create_gather_path(root,
														partially_grouped_rel,
														append_path,
														partially_grouped_rel->reltarget,
														NULL,
														&total_groups);
		add_path(partially_grouped_rel, (Path *) gather_path);
	}
}

/*
 * Get the best total path for aggregation. Prefer chunk append paths if we have one, otherwise
 * return the cheapest_total_path;
 */
static Path *
get_best_total_path(RelOptInfo *output_rel)
{
	ListCell *lc;
	foreach (lc, output_rel->pathlist)
	{
		Path *path = lfirst(lc);

		if (ts_is_chunk_append_path(path))
			return path;
	}

	return output_rel->cheapest_total_path;
}

/*
 Is the provided path a agg path that uses a sorted or plain agg strategy?
*/
static bool pg_nodiscard
is_path_sorted_or_plain_agg_path(Path *path)
{
	AggPath *agg_path = castNode(AggPath, path);
	Assert(agg_path->aggstrategy == AGG_SORTED || agg_path->aggstrategy == AGG_PLAIN ||
		   agg_path->aggstrategy == AGG_HASHED);
	return agg_path->aggstrategy == AGG_SORTED || agg_path->aggstrategy == AGG_PLAIN;
}

/*
 * Check if this path belongs to a plain or sorted aggregation
 */
static bool
contains_path_plain_or_sorted_agg(Path *path)
{
	List *subpaths = get_subpaths_from_append_path(path, true);

	Ensure(subpaths != NIL, "Unable to determine aggregation type");

	ListCell *lc;
	foreach (lc, subpaths)
	{
		Path *subpath = lfirst(lc);

		if (IsA(subpath, AggPath))
			return is_path_sorted_or_plain_agg_path(subpath);
	}

	/*
	 * No dedicated aggregation nodes found directly underneath the append node. This could be
	 * due to two reasons.
	 *
	 * (1) Only vectorized aggregation is used and we don't have dedicated Aggregation nods.
	 * (2) The query plan uses multi-level appends to keep a certain sorting
	 *     - ChunkAppend
	 *          - Merge Append
	 *             - Agg Chunk 1
	 *             - Agg Chunk 2
	 *          - Merge Append
	 *             - Agg Chunk 3
	 *             - Agg Chunk 4
	 *
	 * in both cases, we use a sorted aggregation node to finalize the partial aggregation and
	 * produce a proper sorting.
	 */
	return true;
}

/*
 * Replan the aggregation and create a partial aggregation at chunk level and finalize the
 * aggregation on top of an append node.
 *
 * The functionality is inspired by PostgreSQL's create_partitionwise_grouping_paths() function
 *
 * Generated aggregation paths:
 *
 * Finalize Aggregate
 *   -> Append
 *      -> Partial Aggregation
 *        - Chunk 1
 *      ...
 *      -> Append of partially compressed chunk 2
 *         -> Partial Aggregation
 *             -> Scan on uncompressed part of chunk 2
 *         -> Partial Aggregation
 *             -> Scan on compressed part of chunk 2
 *      ...
 *      -> Partial Aggregation N
 *        - Chunk N
 */
void
ts_pushdown_partial_agg(PlannerInfo *root, Hypertable *ht, RelOptInfo *input_rel,
						RelOptInfo *output_rel, void *extra)
{
	Query *parse = root->parse;

	/* We are only interested in hypertables */
	if (!ht)
		return;

	/* Perform partial aggregation planning only if there is an aggregation is requested */
	if (!parse->hasAggs)
		return;

	/* Grouping sets are not supported by the partial aggregation pushdown */
	if (parse->groupingSets)
		return;

	/* Don't replan aggregation if we already have a MinMaxAggPath (e.g., created by
	 * ts_preprocess_first_last_aggregates) */
	if (has_min_max_agg_path(output_rel))
		return;

	/* Is sorting possible ? */
	bool can_sort = grouping_is_sortable(parse->groupClause) && ts_guc_enable_chunkwise_aggregation;

	/* Is hashing possible ? */
	bool can_hash = grouping_is_hashable(parse->groupClause) &&
					!ts_is_gapfill_path(linitial(output_rel->pathlist)) && enable_hashagg;

	Assert(extra != NULL);
	GroupPathExtraData *extra_data = (GroupPathExtraData *) extra;

	/* Determine the number of groups from the already planned aggregation */
	AggPath *existing_agg_path = get_existing_agg_path(output_rel);
	if (existing_agg_path == NULL)
		return;

	/* Skip partial aggregations already created by _timescaledb_functions.partialize_agg */
	if (existing_agg_path->aggsplit == AGGSPLIT_INITIAL_SERIAL)
		return;

/* Don't replan aggregation if it contains already partials or non-serializable aggregates */
#if PG14_LT
	AggClauseCosts agg_costs;
	MemSet(&agg_costs, 0, sizeof(AggClauseCosts));
	get_agg_clause_costs_compat(root, (Node *) root->processed_tlist, AGGSPLIT_SIMPLE, &agg_costs);
	get_agg_clause_costs_compat(root, parse->havingQual, AGGSPLIT_SIMPLE, &agg_costs);

	if (agg_costs.hasNonPartial || agg_costs.hasNonSerial)
#else
	if (root->hasNonPartialAggs || root->hasNonSerialAggs)
#endif
		return;

	double d_num_groups = existing_agg_path->numGroups;
	Assert(d_num_groups > 0);

	/* Construct partial group agg upper relation */
	RelOptInfo *partially_grouped_rel =
		fetch_upper_rel(root, UPPERREL_PARTIAL_GROUP_AGG, input_rel->relids);
	partially_grouped_rel->consider_parallel = input_rel->consider_parallel;
	partially_grouped_rel->reloptkind = input_rel->reloptkind;
	partially_grouped_rel->serverid = input_rel->serverid;
	partially_grouped_rel->userid = input_rel->userid;
	partially_grouped_rel->useridiscurrent = input_rel->useridiscurrent;
	partially_grouped_rel->fdwroutine = input_rel->fdwroutine;

	/* Build target list for partial aggregate paths */
	PathTarget *grouping_target = output_rel->reltarget;
	PathTarget *partial_grouping_target = ts_make_partial_grouping_target(root, grouping_target);
	partially_grouped_rel->reltarget = partial_grouping_target;

	/* Calculate aggregation costs */
	if (!extra_data->partial_costs_set)
	{
		/* Init costs */
		MemSet(&extra_data->agg_partial_costs, 0, sizeof(AggClauseCosts));
		MemSet(&extra_data->agg_final_costs, 0, sizeof(AggClauseCosts));

		/* partial phase */
		get_agg_clause_costs_compat(root,
									(Node *) partial_grouping_target->exprs,
									AGGSPLIT_INITIAL_SERIAL,
									&extra_data->agg_partial_costs);

		/* final phase */
		get_agg_clause_costs_compat(root,
									(Node *) root->upper_targets[UPPERREL_GROUP_AGG]->exprs,
									AGGSPLIT_FINAL_DESERIAL,
									&extra_data->agg_final_costs);

		extra_data->partial_costs_set = true;
	}

	/* Generate the aggregation pushdown path */
	Path *cheapest_total_path = get_best_total_path(input_rel);
	Assert(cheapest_total_path != NULL);
	generate_agg_pushdown_path(root,
							   cheapest_total_path,
							   output_rel,
							   partially_grouped_rel,
							   grouping_target,
							   partial_grouping_target,
							   can_sort,
							   can_hash,
							   d_num_groups,
							   extra_data);

	/* The same as above but for partial paths */
	if (input_rel->partial_pathlist != NIL && input_rel->consider_parallel)
	{
		Path *cheapest_partial_path = linitial(input_rel->partial_pathlist);
		generate_partial_agg_pushdown_path(root,
										   cheapest_partial_path,
										   output_rel,
										   partially_grouped_rel,
										   grouping_target,
										   partial_grouping_target,
										   can_sort,
										   can_hash,
										   d_num_groups,
										   extra_data);
	}

	/* Replan aggregation if we were able to generate partially grouped rel paths */
	if (partially_grouped_rel->pathlist == NIL)
		return;

	/* Prefer our paths */
	output_rel->pathlist = NIL;
	output_rel->partial_pathlist = NIL;

	/* Finalize the created partially aggregated paths by adding a 'Finalize Aggregate' node on top
	 * of them. */
	AggClauseCosts *agg_final_costs = &extra_data->agg_final_costs;
	ListCell *lc;
	foreach (lc, partially_grouped_rel->pathlist)
	{
		Path *append_path = lfirst(lc);

		if (contains_path_plain_or_sorted_agg(append_path))
		{
			bool is_sorted;

			is_sorted = pathkeys_contained_in(root->group_pathkeys, append_path->pathkeys);

			if (!is_sorted)
			{
				append_path = (Path *)
					create_sort_path(root, output_rel, append_path, root->group_pathkeys, -1.0);
			}

			add_path(output_rel,
					 (Path *) create_agg_path(root,
											  output_rel,
											  append_path,
											  grouping_target,
											  parse->groupClause ? AGG_SORTED : AGG_PLAIN,
											  AGGSPLIT_FINAL_DESERIAL,
#if PG16_LT
											  parse->groupClause,
#else
											  root->processed_groupClause,
#endif
											  (List *) parse->havingQual,
											  agg_final_costs,
											  d_num_groups));
		}
		else
		{
			add_path(output_rel,
					 (Path *) create_agg_path(root,
											  output_rel,
											  append_path,
											  grouping_target,
											  AGG_HASHED,
											  AGGSPLIT_FINAL_DESERIAL,
#if PG16_LT
											  parse->groupClause,
#else
											  root->processed_groupClause,
#endif
											  (List *) parse->havingQual,
											  agg_final_costs,
											  d_num_groups));
		}
	}
}

/*
 * Turn an aggregate relation into a partial aggregate relation if aggregates
 * are enclosed by the partialize_agg function.
 *
 * The partialize_agg function can "manually" partialize an aggregate. For
 * instance:
 *
 *  SELECT time_bucket('1 day', time), device,
 *  _timescaledb_functions.partialize_agg(avg(temp))
 *  GROUP BY 1, 2;
 *
 * Would compute the partial aggregate of avg(temp).
 *
 * The plan to compute the relation must be either entirely non-partial or
 * entirely partial, so it is not possible to mix partials and
 * non-partials. Note that aggregates can appear in both the target list and the
 * HAVING clause, for instance:
 *
 *  SELECT time_bucket('1 day', time), device, avg(temp)
 *  GROUP BY 1, 2
 *  HAVING avg(temp) > 3;
 *
 * Regular partial aggregations executed by the planner (i.e., those not induced
 * by the partialize_agg function) have their HAVING aggregates transparently
 * moved to the target list during planning so that the finalize node can use it
 * when applying the final filter on the resulting groups, obviously omitting
 * the extra columns in the final output/projection. However, it doesn't make
 * much sense to transparently do that when partializing with partialize_agg
 * since it would be odd to return more columns than requested by the
 * user. Therefore, the caller would have to do that manually. This, in fact, is
 * also done when materializing continuous aggregates.
 *
 * For this reason, HAVING clauses with partialize_agg are blocked, except in
 * cases where the planner transparently reduces the having expression to a
 * simple filter (e.g., HAVING device > 3). In such cases, the HAVING clause is
 * removed and replaced by a filter on the input.
 * Returns : true if partial aggs were found, false otherwise.
 * Modifies : output_rel if partials aggs were found.
 */
bool
ts_plan_process_partialize_agg(PlannerInfo *root, RelOptInfo *output_rel)
{
	Query *parse = root->parse;
	bool found_partialize_agg_func;

	Assert(IS_UPPER_REL(output_rel));

	if (CMD_SELECT != parse->commandType || !parse->hasAggs)
		return false;

	found_partialize_agg_func =
		has_partialize_function((Node *) parse->targetList, TS_DO_NOT_FIX_AGGSPLIT);

	if (!found_partialize_agg_func)
		return false;

	/* partialize_agg() function found. Now turn simple (non-partial) aggs
	 * (AGGSPLIT_SIMPLE) into partials. If the Agg is a combine/final we want
	 * to do the combine but not the final step. However, it is not possible
	 * to change that here at the Path stage because the PostgreSQL planner
	 * will hit an assertion, so we defer that to the plan stage in planner.c.
	 */
	bool is_combine = partialize_agg_paths(output_rel);

	if (!is_combine)
		has_partialize_function((Node *) parse->targetList, TS_FIX_AGGSPLIT_SIMPLE);

	/* We cannot check root->hasHavingqual here because sometimes the
	 * planner can replace the HAVING clause with a simple filter. But
	 * root->hashavingqual stays true to remember that the query had a
	 * HAVING clause initially. */
	if (NULL != parse->havingQual)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot partialize aggregate with HAVING clause"),
				 errhint("Any aggregates in a HAVING clause need to be partialized in the output "
						 "target list.")));

	return true;
}
