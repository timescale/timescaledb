#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/builtins.h>
#include <utils/array.h>
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"

/* aggregate histogram:
 *	 hist(state, val, min, max, nbuckets) returns the histogram array with nbuckets
 *
 * Usage:
 *	 SELECT hist(field, min, max, nbuckets) FROM table GROUP BY parameter.
 */

PG_FUNCTION_INFO_V1(hist_sfunc);

/* Generate a histogram */
Datum
hist_sfunc(PG_FUNCTION_ARGS) 
{
	MemoryContext aggcontext; 
	ArrayType 	*state = PG_ARGISNULL(0) ? NULL : PG_GETARG_ARRAYTYPE_P(0);
	Datum 		*elems; 

	float 	val = PG_GETARG_FLOAT4(1); 
	float 	min = PG_GETARG_FLOAT4(2); 
	float 	max = PG_GETARG_FLOAT4(3); 
	int 	nbuckets = PG_GETARG_INT32(4); 

	int 	bucket = DirectFunctionCall4(width_bucket_float8, val, min, max, nbuckets);  

	int     dims[1]; // 1-D array containing number of buckets used to construct histogram
 	int     lbs[1]; // 1-D array containing the lower bound used to construct histogram
 	int 	s = 0; // Extra indexing variable for conversion between C and PostgreSQL arrays

 	/* Determine the lower bound (i.e. zero- or one-basing in PostgreSQL array) */
 	lbs[0] = (bucket == 0) ? 0 : 1;

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* Cannot be called directly because of internal-type argument */
		elog(ERROR, "hist_sfunc called in non-aggregate context");
	}


	if (min > max) {
		elog(ERROR, "lower bound cannot exceed upper bound");
	}

	/* Init the array with the correct number of 0's so the caller doesn't see NULLs (for loop) */
	if (state == NULL)  
	{
		/* Increment the number of buckets if width_bucket returned nbuckets + 1 */
		if (bucket > nbuckets) {
			nbuckets++;
		}

		/* Construct a zero-based array representation in C (+1 accounts for the zero-th element) */
		elems = (Datum *) MemoryContextAlloc(aggcontext, sizeof(Datum) * (nbuckets + 1)); 

		for (int i = 0; i <= nbuckets; i++) {
			elems[i] = (Datum) 0;
		}

		/* Specify the number of elements by accounting for buckets outside of the input range */
		dims[0] = nbuckets + 1 - lbs[0]; 
	}

	else { 
		Oid    	i_eltype;
	    int16  	i_typlen;
	    bool   	i_typbyval;
	    char   	i_typalign;
	    int 	n;
	    bool 	*nulls;
	    /* Copy of elems if needed */
	    Datum 	*elems_edit;

		/* Get input array element type */
		i_eltype = ARR_ELEMTYPE(state);
		get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);

		/* Deconstruct array */
		deconstruct_array(state, i_eltype, i_typlen, i_typbyval, i_typalign, &elems, &nulls, &n); 

		/* Specify zero-based array if the state array already contains a zero index */
		if (DirectFunctionCall2(array_lower, PointerGetDatum(state), 1) == 0) {
			lbs[0] = 0;
		}

		else if (bucket < DirectFunctionCall2(array_lower, PointerGetDatum(state), 1)) {
			n++;

			/* Copy array with an added zero index */
			elems_edit = (Datum *) MemoryContextAlloc(aggcontext, sizeof(Datum) * n);
			elems_edit[0] = (Datum) 0;

			for (int j = 1; j <= n; j++) {
				elems_edit[j] = elems[j - 1];
			}
			elems = elems_edit;
		}

		/* Otherwise specify one-based array in PostgreSQL */
		else { 
			s = 1;
		}

		/* Expand array if a value is above the bucket range */
		if (bucket > DirectFunctionCall2(array_upper, PointerGetDatum(state), 1)) {
			s = 0;
			n++;
			/* Copy array with an added index at the end */
			elems_edit = (Datum *) MemoryContextAlloc(aggcontext, sizeof(Datum) * n);

			/* Expand array based on one- or zero-indexing, respectively */
			if (lbs[0] != 0) {
				elems_edit[0] = (Datum) 0;
				for (int j = 1; j < n; j++) {
					elems_edit[j] = elems[j - 1];
				}
			}
			else {
				for (int j = 0; j < n; j++) {
					elems_edit[j] = elems[j];
				}
			}
			elems_edit[bucket] = (Datum) 0;
			elems = elems_edit;
		}

		/* Set number of buckets */
		dims[0] = n;
	}

	/* Increment the proper bucket */
	elems[bucket-s] = elems[bucket-s] + (Datum) 1; 

	/* Construct the state array */
 	state = construct_md_array(elems + lbs[0] - s, NULL, 1, dims, lbs, INT4OID, 4, true, 'i'); 

	/* Return the integer array */
	PG_RETURN_ARRAYTYPE_P(state); 
}