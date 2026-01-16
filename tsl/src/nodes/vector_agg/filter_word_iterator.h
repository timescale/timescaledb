/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/*
 * Iterator that skips the bitmap words that are fully zero. It helps us do less
 * work when most of the batch rows are filtered out.
 */
typedef struct
{
	const int nrows;
	const int past_the_end_word;
	uint64 const *const filter;

	int start_word;
	int end_word;

	int start_row;
	int end_row;

	int stats_matched_rows;
} FilterWordIterator;

inline static void
filter_word_iterator_advance(FilterWordIterator *iter)
{
	if (iter->filter == NULL)
	{
		iter->start_word = iter->end_word;
		iter->end_word = iter->past_the_end_word;
		iter->start_row = iter->end_row;
		iter->end_row = iter->nrows;
		return;
	}

	/*
	 * Skip the bitmap words which are zero.
	 */
	for (iter->start_word = iter->end_word;
		 iter->start_word < iter->past_the_end_word && iter->filter[iter->start_word] == 0;
		 iter->start_word++)
		;

	if (iter->start_word >= iter->past_the_end_word)
	{
		/*
		 * Finished. The start_row shouldn't be used because the iterator is
		 * invalid now, but set it to past-the-end for consistency.
		 */
		iter->start_row = iter->nrows;
		return;
	}

	/*
	 * Collect the consecutive bitmap words which are nonzero.
	 */
	for (iter->end_word = iter->start_word + 1;
		 iter->end_word < iter->past_the_end_word && iter->filter[iter->end_word] != 0;
		 iter->end_word++)
		;

	Assert(iter->end_word > iter->start_word);

	/*
	 * Now we have the [start, end] range of bitmap words that are
	 * nonzero.
	 *
	 * Determine starting and ending rows, also skipping the starting
	 * and trailing zero bits at the ends of the range.
	 */
	iter->start_row =
		iter->start_word * 64 + pg_rightmost_one_pos64(iter->filter[iter->start_word]);
	Assert(iter->start_row <= iter->nrows);

	/*
	 * The bits for past-the-end rows must be set to zero, so this
	 * calculation should yield no more than n.
	 */
	iter->end_row =
		(iter->end_word - 1) * 64 + pg_leftmost_one_pos64(iter->filter[iter->end_word - 1]) + 1;
	Assert(iter->end_row <= iter->nrows);
}

inline static FilterWordIterator
filter_word_iterator_init(int nrows, uint64 const *filter)
{
	FilterWordIterator iter = { .nrows = nrows,
								.past_the_end_word = (nrows - 1) / 64 + 1,
								.filter = filter };

	filter_word_iterator_advance(&iter);

	return iter;
}

inline static bool
filter_word_iterator_is_valid(FilterWordIterator const *iter)
{
	return iter->start_word < iter->past_the_end_word;
}
