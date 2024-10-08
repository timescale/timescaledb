/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <compression/arrow_c_data_interface.h>

/*
 * Function table for a vectorized implementation of an aggregate function.
 */
typedef struct
{
	/* Size of the aggregate function state. */
	size_t state_bytes;

	/* Initialize the aggregate function states. */
	void (*agg_init)(void *restrict agg_states, int n);

	/* Aggregate a given arrow array. */
	void (*agg_vector)(void *restrict agg_state, const ArrowArray *vector, const uint64 *filter,
					   MemoryContext agg_extra_mctx);

	/* Aggregate a constant (like segmentby or column with default value). */
	void (*agg_const)(void *restrict agg_state, Datum constvalue, bool constisnull, int n,
					  MemoryContext agg_extra_mctx);

	void (*agg_many)(void *restrict agg_states, uint32 *restrict offsets, int start_row,
					 int end_row, const ArrowArray *vector, MemoryContext agg_extra_mctx);

	void (*agg_many_scalar)(void *restrict agg_states, uint32 *restrict offsets, int start_row,
							int end_row, Datum constvalue, bool constisnull,
							MemoryContext agg_extra_mctx);

	/* Emit a partial result. */
	void (*agg_emit)(void *restrict agg_state, Datum *out_result, bool *out_isnull);
} VectorAggFunctions;

VectorAggFunctions *get_vector_aggregate(Oid aggfnoid);
