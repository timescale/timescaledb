/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <compression/arrow_c_data_interface.h>

/*
 * Function table for a vectorized implementation of an aggregate function.
 *
 * One state of aggregate function corresponds to one set of rows that are
 * supposed to be grouped together (e.g. for one grouping key).
 *
 * There are functions for adding a compressed batch to one aggregate function
 * state (no grouping keys), and to multiple aggregate function states laid out
 * contiguously in memory.
 */
typedef struct
{
	/* Size of the aggregate function state. */
	size_t state_bytes;

	/*
	 * Initialize the n aggregate function states stored contiguously at the
	 * given pointer.
	 */
	void (*agg_init)(void *restrict agg_states, int n);

	/* Aggregate a given arrow array. */
	void (*agg_vector)(void *restrict agg_state, const ArrowArray *vector, const uint64 *filter,
					   MemoryContext agg_extra_mctx);

	/* Aggregate a scalar value, like segmentby or column with default value. */
	void (*agg_scalar)(void *restrict agg_state, Datum constvalue, bool constisnull, int n,
					   MemoryContext agg_extra_mctx);

	/* Emit a partial aggregation result. */
	void (*agg_emit)(void *restrict agg_state, Datum *out_result, bool *out_isnull);
} VectorAggFunctions;

VectorAggFunctions *get_vector_aggregate(Oid aggfnoid);
