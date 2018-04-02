#include <postgres.h>

#include "median_quickselect.h"

#include <catalog/pg_type.h>
#include <utils/array.h>
#include <utils/numeric.h>
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"

#include "compat.h"

#include <stddef.h>

/*
 * The postgres aggregate takes advantage of existing functions for
 * collecting array data- since the computation of the median requires seeing
 * all rows at once, we can use the existing array_append accumulate function
 * and simply provide a 'finalfunc' that computes the median given the
 * complete dataset.
 *
 * ---------- CREATE AGGREGATE avg (float8) ( sfunc = array_append, stype =
 * anyarray, finalfunc = median_numeric_finalfunc ); ----------
 */

/* a bare bones wrapper around an array of Numerics */
typedef struct NumericArray {
	Size		size;
	Numeric	       *data;
}		NumericArray;

/*
 * Create a NumericArray allocated using the specified 'agg_context' from the
 * specified 'array', IGNORING any null values.
 *
 * This function relies on the fact that 'array' is filled with Datums that
 * contain Numeric values. To call it with any other array is undefined
 * behavior.
 *
 * On error, this function will call elog(ERROR, ...)
 */
static
NumericArray pgArrayToNumericArray(ArrayType * array,
				   MemoryContext * agg_context) {
	Assert(array);
	Assert(agg_context);

	NumericArray	result;

	int		number_of_dimensions = ARR_NDIM(array);
	int	       *array_of_dim_lengths = ARR_DIMS(array);
	if (number_of_dimensions != 1) {
		elog(ERROR, "median undefined on an array column");
	}
	size_t		length = array_of_dim_lengths[0];

	result.data = MemoryContextAllocZero(*agg_context, length * sizeof(Numeric));

	int		slice_ndim = 0;	/* iterate item by item */
	ArrayMetaState *meta_state = NULL;
	ArrayIterator	iterator
	= array_create_iterator(array, slice_ndim, meta_state);

	/* iterate through array */
	Datum		value;
	bool		is_null;
	int		i = 0;
	size_t		actual_length = 0;
	while (array_iterate(iterator, &value, &is_null)) {
		result.data[i] = DatumGetNumeric(value);
		++actual_length;
		++i;
	}
	array_free_iterator(iterator);

	result.size = actual_length;

	return result;
}

/*
 * The final function for the median aggregate, specialized for Numeric
 * values. Takes as an argument an ArrayType of Numeric values, returning the
 * median.
 *
 * Unpacks the array into a c-style array, taking O(N) extra space.
 *
 * Uses the Quickselect algorithm, taking O(N) on average, O(N^2) in the
 * worst case.
 */
TS_FUNCTION_INFO_V1(median_numeric_finalfunc);
Datum
median_numeric_finalfunc(PG_FUNCTION_ARGS)
{
	MemoryContext	agg_context;
	ArrayBuildState *state = NULL;
	Datum		array_datum;
	NumericArray	numeric_array = {0};
	ArrayType      *array;

	Numeric		result = {0};

	if (!AggCheckCallContext(fcinfo, &agg_context)) {
		elog(ERROR, "timescale median_numeric_finalfunc called "
		     "in non-aggregate context");
	}

	if (PG_ARGISNULL(0)) {
		PG_RETURN_NULL();
	}

	state = (ArrayBuildState *) PG_GETARG_POINTER(0);

	if (state == NULL) {
		PG_RETURN_NULL();
	}

	array_datum = makeArrayResult(state, agg_context);
	array = DatumGetArrayTypeP(array_datum);
	numeric_array = pgArrayToNumericArray(array, &agg_context);

	if (numeric_array.size == 0) {
		PG_RETURN_NULL();
	}

	result = median_numeric_quickselect(numeric_array.data, numeric_array.size);

	PG_RETURN_NUMERIC(result);
}
