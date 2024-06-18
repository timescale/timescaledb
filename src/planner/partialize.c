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
#include "estimate.h"
#include "extension_constants.h"
#include "gapfill.h"
#include "import/planner.h"
#include "nodes/chunk_append/chunk_append.h"
#include "nodes/print.h"
#include "partialize.h"
#include "planner.h"
#include "utils.h"

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
static void
get_subpaths_from_append_path(Path *path, List **subpaths, Path **append, Path **gather)
{
	if (IsA(path, AppendPath))
	{
		AppendPath *append_path = castNode(AppendPath, path);
		*subpaths = append_path->subpaths;
		*append = path;
		return;
	}

	if (IsA(path, MergeAppendPath))
	{
		MergeAppendPath *merge_append_path = castNode(MergeAppendPath, path);
		*subpaths = merge_append_path->subpaths;
		*append = path;
		return;
	}

	if (ts_is_chunk_append_path(path))
	{
		CustomPath *custom_path = castNode(CustomPath, path);
		*subpaths = custom_path->custom_paths;
		*append = path;
		return;
	}

	if (IsA(path, GatherPath))
	{
		*gather = path;
		get_subpaths_from_append_path(castNode(GatherPath, path)->subpath,
									  subpaths,
									  append,
									  /* gather = */ NULL);
		return;
	}

	if (IsA(path, GatherMergePath))
	{
		*gather = path;
		get_subpaths_from_append_path(castNode(GatherMergePath, path)->subpath,
									  subpaths,
									  append,
									  /* gather = */ NULL);
		return;
	}

	if (IsA(path, SortPath))
	{
		/* Can see GatherMerge -> Sort -> Partial HashAggregate in parallel plans. */
		get_subpaths_from_append_path(castNode(SortPath, path)->subpath, subpaths, append, gather);
		return;
	}

	if (IsA(path, AggPath))
	{
		/* Can see GatherMerge -> Sort -> Partial HashAggregate in parallel plans. */
		get_subpaths_from_append_path(castNode(AggPath, path)->subpath, subpaths, append, gather);
		return;
	}

	if (IsA(path, ProjectionPath))
	{
		ProjectionPath *projection = castNode(ProjectionPath, path);
		get_subpaths_from_append_path(projection->subpath, subpaths, append, gather);
		return;
	}

	/* Aggregation push-down is not supported for other path types so far */
}

/*
 * Copy an AppendPath and set new subpaths.
 */
static AppendPath *
copy_append_path(AppendPath *path, List *subpaths, PathTarget *pathtarget)
{
	AppendPath *newPath = makeNode(AppendPath);
	memcpy(newPath, path, sizeof(AppendPath));
	newPath->subpaths = subpaths;
	newPath->path.pathtarget = copy_pathtarget(pathtarget);

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
	MergeAppendPath *newPath =
		create_merge_append_path(root, path->path.parent, subpaths, path->path.pathkeys, NULL);

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
add_partially_aggregated_subpaths(PlannerInfo *root, PathTarget *input_target,
								  PathTarget *partial_grouping_target, double d_num_groups,
								  GroupPathExtraData *extra_data, bool can_sort, bool can_hash,
								  Path *subpath, List **sorted_paths, List **hashed_paths)
{
	/* Translate targetlist for partition */
	AppendRelInfo *appinfo = ts_get_appendrelinfo(root, subpath->parent->relid, false);
	PathTarget *chunktarget = copy_pathtarget(partial_grouping_target);
	chunktarget->exprs =
		castNode(List, adjust_appendrel_attrs(root, (Node *) chunktarget->exprs, 1, &appinfo));

	/* In declarative partitioning planning, this is done by apply_scanjoin_target_to_path */
	Assert(list_length(subpath->pathtarget->exprs) == list_length(input_target->exprs));
	subpath->pathtarget->sortgrouprefs = input_target->sortgrouprefs;

	if (can_sort)
	{
		AggPath *agg_path =
			create_sorted_partial_agg_path(root, subpath, chunktarget, d_num_groups, extra_data);

		*sorted_paths = lappend(*sorted_paths, (Path *) agg_path);
	}

	if (can_hash)
	{
		AggPath *agg_path =
			create_hashed_partial_agg_path(root, subpath, chunktarget, d_num_groups, extra_data);

		*hashed_paths = lappend(*hashed_paths, (Path *) agg_path);
	}
}

/*
 * Generate a total aggregation path for partial aggregations.
 *
 * The generated paths contain partial aggregations (created by using AGGSPLIT_INITIAL_SERIAL).
 * These aggregations need to be finished by the caller by adding a node that performs the
 * AGGSPLIT_FINAL_DESERIAL step.
 *
 * The original path can be either parallel or non-parallel aggregation, and the
 * resulting path will be parallel accordingly.
 */
static void
generate_agg_pushdown_path(PlannerInfo *root, Path *cheapest_total_path, RelOptInfo *input_rel,
						   RelOptInfo *output_rel, RelOptInfo *partially_grouped_rel,
						   PathTarget *grouping_target, PathTarget *partial_grouping_target,
						   bool can_sort, bool can_hash, double d_num_groups,
						   GroupPathExtraData *extra_data)
{
	/* Get subpaths */
	List *subpaths = NIL;
	Path *top_gather = NULL;
	Path *top_append = NULL;
	get_subpaths_from_append_path(cheapest_total_path, &subpaths, &top_append, &top_gather);

	/* No subpaths available or unsupported append node */
	if (subpaths == NIL)
	{
		return;
	}

	Assert(top_append != NULL);

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
		List *partially_compressed_paths = NIL;
		Path *partially_compressed_append = NULL;
		Path *partially_compressed_gather = NULL;
		get_subpaths_from_append_path(subpath,
									  &partially_compressed_paths,
									  &partially_compressed_append,
									  &partially_compressed_gather);
		Assert(partially_compressed_gather == NULL);

		if (partially_compressed_append != NULL)
		{
			List *partially_compressed_sorted = NIL;
			List *partially_compressed_hashed = NIL;

			ListCell *lc2;
			foreach (lc2, partially_compressed_paths)
			{
				Path *partially_compressed_path = lfirst(lc2);

				add_partially_aggregated_subpaths(root,
												  input_rel->reltarget,
												  partial_grouping_target,
												  d_num_groups,
												  extra_data,
												  can_sort,
												  can_hash,
												  partially_compressed_path,
												  &partially_compressed_sorted /* Result path */,
												  &partially_compressed_hashed /* Result path */);
			}

			if (can_sort)
			{
				sorted_subpaths = lappend(sorted_subpaths,
										  copy_append_like_path(root,
																partially_compressed_append,
																partially_compressed_sorted,
																subpath->pathtarget));
			}

			if (can_hash)
			{
				hashed_subpaths = lappend(hashed_subpaths,
										  copy_append_like_path(root,
																partially_compressed_append,
																partially_compressed_hashed,
																subpath->pathtarget));
			}
		}
		else
		{
			add_partially_aggregated_subpaths(root,
											  input_rel->reltarget,
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
	if (top_gather == NULL)
	{
		/*
		 * The original aggregation plan was non-parallel, so we're creating a
		 * non-parallel plan as well.
		 */
		if (sorted_subpaths != NIL)
		{
			add_path(partially_grouped_rel,
					 copy_append_like_path(root,
										   top_append,
										   sorted_subpaths,
										   partial_grouping_target));
		}

		if (hashed_subpaths != NIL)
		{
			add_path(partially_grouped_rel,
					 copy_append_like_path(root,
										   top_append,
										   hashed_subpaths,
										   partial_grouping_target));
		}
	}
	else
	{
		/*
		 * The cheapest aggregation plan was parallel, so we're creating a
		 * parallel plan as well.
		 */
		if (sorted_subpaths != NIL)
		{
			add_partial_path(partially_grouped_rel,
							 copy_append_like_path(root,
												   top_append,
												   sorted_subpaths,
												   partial_grouping_target));
		}

		if (hashed_subpaths != NIL)
		{
			add_partial_path(partially_grouped_rel,
							 copy_append_like_path(root,
												   top_append,
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
	List *subpaths = NIL;
	Path *append = NULL;
	Path *gather = NULL;
	get_subpaths_from_append_path(path, &subpaths, &append, &gather);

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
	{
		return;
	}

	/* Skip partial aggregations already created by _timescaledb_functions.partialize_agg */
	if (existing_agg_path->aggsplit == AGGSPLIT_INITIAL_SERIAL)
		return;

	/* Don't replan aggregation if it contains already partials or non-serializable aggregates */
	if (root->hasNonPartialAggs || root->hasNonSerialAggs)
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
		get_agg_clause_costs(root, AGGSPLIT_INITIAL_SERIAL, &extra_data->agg_partial_costs);

		/* final phase */
		get_agg_clause_costs(root, AGGSPLIT_FINAL_DESERIAL, &extra_data->agg_final_costs);

		extra_data->partial_costs_set = true;
	}

	/* Generate the aggregation pushdown path */
	generate_agg_pushdown_path(root,
							   &existing_agg_path->path,
							   input_rel,
							   output_rel,
							   partially_grouped_rel,
							   grouping_target,
							   partial_grouping_target,
							   can_sort,
							   can_hash,
							   d_num_groups,
							   extra_data);

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
