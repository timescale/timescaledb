/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <postgres.h>

#include "nodes/decompress_chunk/compressed_batch.h"
#include <nodes/execnodes.h>

#include "function/functions.h"
#include "grouping_policy.h"

typedef struct VectorAggDef
{
	VectorAggFunctions func;
	int input_offset;
	int output_offset;
	List *filter_clauses;
	uint64 *filter_result;
} VectorAggDef;

typedef struct GroupingColumn
{
	int input_offset;
	int output_offset;
	int value_bytes;
} GroupingColumn;

typedef struct VectorAggState
{
	CustomScanState custom;

	int num_agg_defs;
	VectorAggDef *agg_defs;

	int num_grouping_columns;
	GroupingColumn *grouping_columns;

	/*
	 * We can't call the underlying scan after it has ended, or it will be
	 * restarted. This is the behavior of Postgres heap scans. So we have to
	 * track whether it has ended to avoid this.
	 */
	bool input_ended;

	GroupingPolicy *grouping;

	/*
	 * State to compute vector quals for FILTER clauses.
	 */
	CompressedBatchVectorQualState vqual_state;

	/*
	 * Initialization function for vectorized quals depending on slot type.
	 */
	VectorQualState *(*init_vector_quals)(struct VectorAggState *agg_state, VectorAggDef *agg_def,
										  TupleTableSlot *slot);

	/*
	 * Function for getting the next slot from the child node depending on
	 * child node type.
	 */
	TupleTableSlot *(*get_next_slot)(struct VectorAggState *vector_agg_state);
} VectorAggState;

extern Node *vector_agg_state_create(CustomScan *cscan);
