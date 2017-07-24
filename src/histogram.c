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
 *
 * Description:
 * Histogram generates a histogram array based off of a specified range passed into the function.
 * Values falling outside of this range are bucketed into the 0 or nbucket+1 buckets depending on 
 * if they are below or above the range, respectively. Array representations are one-based by 
 * default in PostgreSQL whereas they are zero-based in C, hence the histogram function operates 
 * on index conversion with extra variables (i.e. s) in the function code. The function assumes
 * a zero index in the elems array regardless of the bucket count. It is converted to a zero- or 
 * one-based array in PostgreSQL depending on the bucket count. If the histogram array contains
 * a zero index, it will be noted by the outputted histogram indices (e.g. [0:2]={5,1,0}).
 */

PG_FUNCTION_INFO_V1(hist_sfunc);
PG_FUNCTION_INFO_V1(hist_combinefunc);

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

		/* Deconstruct array into a default zero-based elems array*/
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

			for (int j = 1; j < n; j++) {
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

/* hist_combinefunc(ArrayType, ArrayType) => ArrayType */
Datum
hist_combinefunc(PG_FUNCTION_ARGS)
{
	MemoryContext 	aggcontext;
	ArrayType 	*state1 = PG_ARGISNULL(0) ? NULL : PG_GETARG_ARRAYTYPE_P(0);
	ArrayType 	*state2 = PG_ARGISNULL(1) ? NULL : PG_GETARG_ARRAYTYPE_P(1);

	if (!AggCheckCallContext(fcinfo, &aggcontext))
	{
		/* Cannot be called directly because of internal-type argument */
		elog(ERROR, "hist_combinefunc called in non-aggregate context");
	}

	if (state2 == NULL)
		PG_RETURN_ARRAYTYPE_P(state1);

	else if (state1 == NULL)
		PG_RETURN_ARRAYTYPE_P(state2);

	else 
	{
		Datum 	*s1, *s2, *result; // Datum array representations of state1, state2, and result

		int     dims[1]; // 1-D array containing number of buckets used to construct result
 		int     lbs[1]; // 1-D array containing the lower bound used to construct result
		int 	ubs; // upper bound used to construct histogram

		/* Lower and upper bounds for state1 and state2 */
		int 	lb1 = DirectFunctionCall2(array_lower, PointerGetDatum(state1), 1);
		int 	lb2 = DirectFunctionCall2(array_lower, PointerGetDatum(state2), 1);
		int 	ub1 = DirectFunctionCall2(array_upper, PointerGetDatum(state1), 1);
		int 	ub2 = DirectFunctionCall2(array_upper, PointerGetDatum(state2), 1); 

		/* State variables */
		Oid    	i_eltype;
	    int16  	i_typlen;
	    bool   	i_typbyval;
	    char   	i_typalign;
	    int 	n;

		/* Get bound extremities */ 
		lbs[0] = (lb1 <= lb2) ? lb1 : lb2;
		ubs = (ub1 >= ub2) ? ub1 : ub2;

		dims[0] = ubs - lbs[0] + 1;
		result = (Datum *) MemoryContextAlloc(aggcontext, sizeof(Datum) * (dims[0]));

		/* Get state1 array element type */
		i_eltype = ARR_ELEMTYPE(state1);
		get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);

		/* Deconstruct state1 into s1 */
		deconstruct_array(state1, i_eltype, i_typlen, i_typbyval, i_typalign, &s1, NULL, &n);

		/* Get state2 array element type */
		i_eltype = ARR_ELEMTYPE(state2);
		get_typlenbyvalalign(i_eltype, &i_typlen, &i_typbyval, &i_typalign);

		/* Deconstruct state2 into s2 */
		deconstruct_array(state2, i_eltype, i_typlen, i_typbyval, i_typalign, &s2, NULL, &n); 

		/* Initialize result array (which is zero-indexed in C) with zeroes */
		for (int i = 0; i < dims[0] + lbs[0]; i++) {
			result[i] = (Datum) 0;
		}

		/* Add in state1 */
		for (int i = lb1; i <= ub1; i++) {
			result[i] += (Datum) s1[i - lb1];
		}

		/* Add in state2 */
		for (int i = lb2; i <= ub2; i++) {
			result[i] += (Datum) s2[i - lb2];
		}

		PG_RETURN_ARRAYTYPE_P((construct_md_array(result + lbs[0], NULL, 1, dims, lbs, INT4OID, 4, true, 'i')));
	}
}
