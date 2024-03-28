/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Functions for working with vectorized predicates.
 */
#pragma once

typedef void(VectorPredicate)(const ArrowArray *, Datum, uint64 *restrict);

VectorPredicate *get_vector_const_predicate(Oid pg_predicate);

void vector_array_predicate(VectorPredicate *vector_const_predicate, bool is_or,
							const ArrowArray *vector, Datum array, uint64 *restrict final_result);

void vector_nulltest(const ArrowArray *arrow, int test_type, uint64 *restrict result);

typedef enum VectorQualSummary
{
	AllRowsPass,
	NoRowsPass,
	SomeRowsPass
} VectorQualSummary;

static pg_attribute_always_inline VectorQualSummary
get_vector_qual_summary(uint64 *restrict qual_result, size_t n_rows)
{
	bool any_rows_pass = false;
	bool all_rows_pass = true;
	for (size_t i = 0; i < n_rows / 64; i++)
	{
		any_rows_pass |= (qual_result[i] != 0);
		all_rows_pass &= (~qual_result[i] == 0);
	}

	if (n_rows % 64 != 0)
	{
		const uint64 last_word_mask = -1ULL >> (64 - n_rows % 64);
		any_rows_pass |= (qual_result[n_rows / 64] & last_word_mask) != 0;
		all_rows_pass &= ((~qual_result[n_rows / 64]) & last_word_mask) == 0;
	}

	Assert(!(all_rows_pass && !any_rows_pass));

	if (!any_rows_pass)
	{
		return NoRowsPass;
	}

	if (all_rows_pass)
	{
		return AllRowsPass;
	}

	return SomeRowsPass;
}
