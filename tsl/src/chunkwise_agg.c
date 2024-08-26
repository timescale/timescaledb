/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include <optimizer/appendinfo.h>
#include <optimizer/cost.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/prep.h>
#include <optimizer/tlist.h>

#include "chunkwise_agg.h"

#include "gapfill.h"
#include "guc.h"
#include "import/planner.h"
#include "nodes/chunk_append/chunk_append.h"
#include "nodes/decompress_chunk/decompress_chunk.h"
#include "planner.h"

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
	PathTarget *chunk_grouped_target = copy_pathtarget(partial_grouping_target);
	chunk_grouped_target->exprs =
		castNode(List,
				 adjust_appendrel_attrs(root,
										(Node *) chunk_grouped_target->exprs,
										/* nappinfos = */ 1,
										&appinfo));

	/*
	 * We might have to project before aggregation. In declarative partitioning
	 * planning, the projection is applied by apply_scanjoin_target_to_path().
	 */
	PathTarget *chunk_target_before_grouping = copy_pathtarget(input_target);
	chunk_target_before_grouping->exprs =
		castNode(List,
				 adjust_appendrel_attrs(root,
										(Node *) chunk_target_before_grouping->exprs,
										/* nappinfos = */ 1,
										&appinfo));
	subpath =
		apply_projection_to_path(root, subpath->parent, subpath, chunk_target_before_grouping);

	if (can_sort)
	{
		AggPath *agg_path = create_sorted_partial_agg_path(root,
														   subpath,
														   chunk_grouped_target,
														   d_num_groups,
														   extra_data);

		*sorted_paths = lappend(*sorted_paths, (Path *) agg_path);
	}

	if (can_hash)
	{
		AggPath *agg_path = create_hashed_partial_agg_path(root,
														   subpath,
														   chunk_grouped_target,
														   d_num_groups,
														   extra_data);

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
tsl_pushdown_partial_agg(PlannerInfo *root, Hypertable *ht, RelOptInfo *input_rel,
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
