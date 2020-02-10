/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <nodes/plannodes.h>
#include <parser/parsetree.h>
#include <optimizer/clauses.h>
#include <optimizer/pathnode.h>
#include <optimizer/paths.h>
#include <optimizer/tlist.h>
#include <catalog/namespace.h>
#include <miscadmin.h>

#include "compat-msvc-enter.h"
#include <optimizer/cost.h>
#include "compat-msvc-exit.h"

#include "plan_add_hashagg.h"
#include "utils.h"
#include "guc.h"
#include "estimate.h"

/* Add a parallel HashAggregate plan.
 * This code is similar to parts of create_grouping_paths */
static void
plan_add_parallel_hashagg(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel,
						  double d_num_groups)
{
	Query *parse = root->parse;
	Path *cheapest_partial_path = linitial(input_rel->partial_pathlist);
	PathTarget *target = root->upper_targets[UPPERREL_GROUP_AGG];
	PathTarget *partial_grouping_target = ts_make_partial_grouping_target(root, target);
	AggClauseCosts agg_partial_costs;
	AggClauseCosts agg_final_costs;
	Size hashagg_table_size;
	double total_groups;
	Path *partial_path;
	double d_num_partial_groups = ts_estimate_group(root, cheapest_partial_path->rows);

	/* don't have any special estimate */
	if (!IS_VALID_ESTIMATE(d_num_partial_groups))
		return;

	MemSet(&agg_partial_costs, 0, sizeof(AggClauseCosts));
	MemSet(&agg_final_costs, 0, sizeof(AggClauseCosts));

	if (parse->hasAggs)
	{
		/* partial phase */
		get_agg_clause_costs(root,
							 (Node *) partial_grouping_target->exprs,
							 AGGSPLIT_INITIAL_SERIAL,
							 &agg_partial_costs);

		/* final phase */
		get_agg_clause_costs(root,
							 (Node *) target->exprs,
							 AGGSPLIT_FINAL_DESERIAL,
							 &agg_final_costs);
		get_agg_clause_costs(root, parse->havingQual, AGGSPLIT_FINAL_DESERIAL, &agg_final_costs);
	}

	hashagg_table_size = ts_estimate_hashagg_tablesize(cheapest_partial_path,
													   &agg_partial_costs,
													   d_num_partial_groups);

	/*
	 * Tentatively produce a partial HashAgg Path, depending on if it looks as
	 * if the hash table will fit in work_mem.
	 */
	if (hashagg_table_size >= work_mem * UINT64CONST(1024))
		return;

	add_partial_path(output_rel,
					 (Path *) create_agg_path(root,
											  output_rel,
											  cheapest_partial_path,
											  partial_grouping_target,
											  AGG_HASHED,
											  AGGSPLIT_INITIAL_SERIAL,
											  parse->groupClause,
											  NIL,
											  &agg_partial_costs,
											  d_num_partial_groups));

	if (!output_rel->partial_pathlist)
		return;

	partial_path = (Path *) linitial(output_rel->partial_pathlist);

	total_groups = partial_path->rows * partial_path->parallel_workers;

	partial_path = (Path *) create_gather_path(root,
											   output_rel,
											   partial_path,
											   partial_grouping_target,
											   NULL,
											   &total_groups);
	add_path(output_rel,
			 (Path *) create_agg_path(root,
									  output_rel,
									  partial_path,
									  target,
									  AGG_HASHED,
									  AGGSPLIT_FINAL_DESERIAL,
									  parse->groupClause,
									  (List *) parse->havingQual,
									  &agg_final_costs,
									  d_num_groups));
}

/* This function add a HashAggregate path, if appropriate
 * it looks like a highly modified create_grouping_paths function
 * in the postgres planner. */
void
ts_plan_add_hashagg(PlannerInfo *root, RelOptInfo *input_rel, RelOptInfo *output_rel)
{
	Query *parse = root->parse;
	Path *cheapest_path = input_rel->cheapest_total_path;
	AggClauseCosts agg_costs;
	bool can_hash;
	double d_num_groups;
	Size hashaggtablesize;
	PathTarget *target = root->upper_targets[UPPERREL_GROUP_AGG];
	bool try_parallel_aggregation;

	if (parse->groupingSets || !parse->hasAggs || parse->groupClause == NIL)
		return;

	MemSet(&agg_costs, 0, sizeof(AggClauseCosts));
	get_agg_clause_costs(root, (Node *) root->processed_tlist, AGGSPLIT_SIMPLE, &agg_costs);
	get_agg_clause_costs(root, parse->havingQual, AGGSPLIT_SIMPLE, &agg_costs);

	can_hash = (parse->groupClause != NIL && agg_costs.numOrderedAggs == 0 &&
				grouping_is_hashable(parse->groupClause));

	if (!can_hash)
		return;

	d_num_groups = ts_estimate_group(root, cheapest_path->rows);

	/* don't have any special estimate */
	if (!IS_VALID_ESTIMATE(d_num_groups))
		return;

	hashaggtablesize = ts_estimate_hashagg_tablesize(cheapest_path, &agg_costs, d_num_groups);

	if (hashaggtablesize >= work_mem * UINT64CONST(1024))
		return;

	if (!output_rel->consider_parallel)
	{
		/* Not even parallel-safe. */
		try_parallel_aggregation = false;
	}
	else if (output_rel->partial_pathlist == NIL)
	{
		/* Nothing to use as input for partial aggregate. */
		try_parallel_aggregation = false;
	}
	else if (agg_costs.hasNonPartial || agg_costs.hasNonSerial)
	{
		/* Insufficient support for partial mode. */
		try_parallel_aggregation = false;
	}
	else
	{
		/* Everything looks good. */
		try_parallel_aggregation = true;
	}

	if (try_parallel_aggregation)
		plan_add_parallel_hashagg(root, input_rel, output_rel, d_num_groups);

	/*
	 * We just need an Agg over the cheapest-total input path, since input
	 * order won't matter.
	 */
	add_path(output_rel,
			 (Path *) create_agg_path(root,
									  output_rel,
									  cheapest_path,
									  target,
									  AGG_HASHED,
									  AGGSPLIT_SIMPLE,
									  parse->groupClause,
									  (List *) parse->havingQual,
									  &agg_costs,
									  d_num_groups));
}
