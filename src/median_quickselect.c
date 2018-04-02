/*
 * This median function implements the 'quickselect' algorithm, aka Hoare's
 * "Algorithm 65: Find".
 *
 * https://en.wikipedia.org/wiki/Quickselect
 *
 * The typedef 'QuickSelectType' and macro 'TIMESCALE_MEDIAN_COMPARE' are in
 * place to lay the groundwork for refactoring this code to provide multiple
 * specializations for postgres datatypes: right now, only Numeric is
 * supported, which provides good generality, since all numeric types can be
 * converted into Numeric values, but poor performance, since the creation
 * and comparison of Numeric values is expensive.
 */

#include <postgres.h>
#include <utils/builtins.h>
#include <utils/numeric.h>

#include "median_quickselect.h"

typedef Numeric QuickSelectType;

/*
 * A function on two 'QuickSelectType' values 'a' and 'b': ---------- Match
 * a, b with | a is bigger than b  ->  1 | a is smaller than b -> -1 | a and
 * b are equal   ->  0 ----------
 */
#define TIMESCALE_MEDIAN_COMPARE(a, b) \
    DatumGetInt32(DirectFunctionCall2(numeric_cmp, \
                                      NumericGetDatum(a), \
                                      NumericGetDatum(b)))

static inline
void
swap(QuickSelectType * a, QuickSelectType * b)
{
	QuickSelectType	temp = *a;
	*a = *b;
	*b = temp;
}

/*
 * Group in place the sublist of the specified 'list' delimited by the
 * indices 'left' and 'right' into two parts, those less than than the item
 * at the specified 'pivot_index' and those greater than the item at the
 * specified 'pivot_index.
 */
static
size_t partition(QuickSelectType * list,
		 size_t left, size_t right, size_t pivot_index) {
	QuickSelectType	pivot_value = list[pivot_index];
	swap(&list[pivot_index], &list[right]);
	size_t		store_index = left;
	for (size_t i = left; i < right; ++i) {
		if (TIMESCALE_MEDIAN_COMPARE(list[i], pivot_value) == -1) {
			swap(&list[store_index], &list[i]);
			++store_index;
		}
	}
	swap(&list[right], &list[store_index]);
	return store_index;
}

/*
 * Iteratively partitions the 'list' in place, selecting at each point the
 * half we know the median to be in, until the partition we are in is has
 * just one item, which is the median.
 */
static
QuickSelectType select(QuickSelectType * list, size_t arr_size) {
	size_t		left = 0;
	size_t		right = arr_size - 1;
	size_t		k = ((arr_size - 1) / 2);	/* the median index */
	for (;;) {
		if (left == right) {
			return list[left];
		}
		size_t		pivot_index = right - 1;
		/* to do - randomly select value between left and right */
		pivot_index = partition(list, left, right, pivot_index);
		if (k == pivot_index) {
			return list[k];
		} else if (k < pivot_index) {
			right = pivot_index - 1;
		} else {
			left = pivot_index + 1;
		}
	}
}

/*
 * Find the median in the specified 'arr' of 'arr_size'. NOTE: Partially
 * sorts 'arr' in the process of computing the median.
 */

QuickSelectType
median_numeric_quickselect(QuickSelectType * arr, size_t arr_size) {
	Assert(arr != NULL);
	Assert(arr_size > 0);
	if (arr_size == 1) {
		return *arr;
	}
	return select(arr, arr_size);
}

#undef TIMESCALE_MEDIAN_COMPARE

