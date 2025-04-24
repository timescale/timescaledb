/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <executor/tuptable.h>

typedef struct GroupingPolicy GroupingPolicy;

typedef struct TupleTableSlot TupleTableSlot;

typedef struct VectorAggDef VectorAggDef;

typedef struct GroupingColumn GroupingColumn;

/*
 * This is a common interface for grouping policies which define how the rows
 * are grouped for aggregation -- e.g. there can be an implementation for no
 * grouping, grouping by compression segmentby columns, grouping over sorted
 * input (GroupAggregate), grouping using a hash table, and so on.
 */
typedef struct GroupingPolicy
{
	/*
	 * Used for rescans in the Postgres sense.
	 */
	void (*gp_reset)(GroupingPolicy *gp);

	/*
	 * Aggregate a single compressed batch.
	 */
	void (*gp_add_batch)(GroupingPolicy *gp, TupleTableSlot *vector_slot);

	/*
	 * Is a partial aggregation result ready?
	 */
	bool (*gp_should_emit)(GroupingPolicy *gp);

	/*
	 * Emit a partial aggregation result into the result slot.
	 */
	bool (*gp_do_emit)(GroupingPolicy *gp, TupleTableSlot *aggregated_slot);

	/*
	 * Destroy the grouping policy.
	 */
	void (*gp_destroy)(GroupingPolicy *gp);

	/*
	 * Description of this grouping policy for the EXPLAIN output.
	 */
	char *(*gp_explain)(GroupingPolicy *gp);
} GroupingPolicy;

/*
 * The various types of grouping we might use, as determined at planning time.
 * The hashed subtypes are all implemented by hash grouping policy.
 */
typedef enum
{
	VAGT_Invalid,
	VAGT_Batch,
	VAGT_HashSingleFixed2,
	VAGT_HashSingleFixed4,
	VAGT_HashSingleFixed8,
	VAGT_HashSingleText,
	VAGT_HashSerialized,
} VectorAggGroupingType;

extern GroupingPolicy *create_grouping_policy_batch(int num_agg_defs, VectorAggDef *agg_defs,
													int num_grouping_columns,
													GroupingColumn *grouping_columns);

extern GroupingPolicy *create_grouping_policy_hash(int num_agg_defs, VectorAggDef *agg_defs,
												   int num_grouping_columns,
												   GroupingColumn *grouping_columns,
												   VectorAggGroupingType grouping_type);
