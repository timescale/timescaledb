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

	/* Initialize the aggregate function state pointed to by agg_value and agg_isnull. */
	void (*agg_init)(Datum *agg_value, bool *agg_isnull);

	/* Aggregate a given arrow array. */
	void (*agg_vector)(ArrowArray *vector, uint64 *filter, Datum *agg_value, bool *agg_isnull);

	/* Aggregate a constant (like segmentby or column with default value). */
	void (*agg_const)(Datum constvalue, bool constisnull, int n, Datum *agg_value,
					  bool *agg_isnull);
} VectorAggFunctions;

VectorAggFunctions *get_vector_aggregate(Oid aggfnoid);
