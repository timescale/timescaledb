/* -*- Mode: C; tab-width: 4; indent-tabs-mode: t; c-basic-offset: 4 -*- */
#include "pgmurmur3.h"

#include <catalog/pg_type.h>
#include "utils/builtins.h"
#include <utils/array.h>
#include <utils/lsyscache.h>

/* adapted from https://github.com/markokr/pghashlib/  */

PG_FUNCTION_INFO_V1(pg_murmur3_hash_string);

Datum
pg_murmur3_hash_string(PG_FUNCTION_ARGS)
{
	struct varlena *data;
	uint64_t	io[MAX_IO_VALUES];

	memset(io, 0, sizeof(io));

	/* request aligned data on weird architectures */
#ifdef HLIB_UNALIGNED_READ_OK
	data = PG_GETARG_VARLENA_PP(0);
#else
	data = PG_GETARG_VARLENA_P(0);
#endif

	io[0] = PG_GETARG_INT32(1);

	hlib_murmur3(VARDATA_ANY(data), VARSIZE_ANY_EXHDR(data), io);
	PG_FREE_IF_COPY(data, 0);

	PG_RETURN_INT32(io[0]);
}

/* _iobeamdb_catalog.get_partition_for_key(key TEXT, mod_factor INT) RETURNS SMALLINT */
PG_FUNCTION_INFO_V1(get_partition_for_key);

Datum
get_partition_for_key(PG_FUNCTION_ARGS)
{
	struct varlena *data;
	int32		mod;
	Datum		hash_d;
	int32		hash_i;
	int16		res;
	/* request aligned data on weird architectures */
#ifdef HLIB_UNALIGNED_READ_OK
	data = PG_GETARG_VARLENA_PP(0);
#else
	data = PG_GETARG_VARLENA_P(0);
#endif
	mod = PG_GETARG_INT32(1);

	hash_d = DirectFunctionCall2(pg_murmur3_hash_string, PointerGetDatum(data), Int32GetDatum(1));
	hash_i = DatumGetInt32(hash_d);

	res = (int16) ((hash_i & 0x7fffffff) % mod);

	PG_RETURN_INT16(res);
}


/*
 * array_position_least returns the highest position in the array such that the element
 * in the array is <= searched element. If the array is of partition-keyspace-end values 
 * then this gives a unique bucket for each keyspace value.
 * 
 * Arg 0: sorted array of smallint
 * Arg 1: searched_element (smallint)
 */

PG_FUNCTION_INFO_V1(array_position_least);

Datum
array_position_least(PG_FUNCTION_ARGS)
{
	ArrayType  *array;
	Datum		searched_element,
				value;
	bool		isnull;
	int			position;
	ArrayMetaState *my_extra;
	ArrayIterator array_iterator;

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		elog(ERROR, "neither parameter should be null");
	}

	array = PG_GETARG_ARRAYTYPE_P(0);
	if (INT2OID != ARR_ELEMTYPE(array))
	{
		elog(ERROR, "only support smallint arrays");
	}

	/* should be a single dimensioned array */
	if (ARR_NDIM(array) > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("can only work with single-dimension array")));

	if (PG_ARGISNULL(1))
	{
		elog(ERROR, "does not expect null as searched-for element");
	}
	else
	{
		searched_element = PG_GETARG_DATUM(1);
	}

	position = (ARR_LBOUND(array))[0] - 1;

	/*
	 * We arrange to look up type info for array_create_iterator only once per
	 * series of calls, assuming the element type doesn't change underneath
	 * us.
	 */
	my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
	if (my_extra == NULL)
	{
		fcinfo->flinfo->fn_extra = MemoryContextAlloc(fcinfo->flinfo->fn_mcxt,
													  sizeof(ArrayMetaState));
		my_extra = (ArrayMetaState *) fcinfo->flinfo->fn_extra;
		get_typlenbyvalalign(INT2OID,
							 &my_extra->typlen,
							 &my_extra->typbyval,
							 &my_extra->typalign);
		my_extra->element_type = INT2OID;
	}

	/* Examine each array element until the element is >= searched_element. */
	array_iterator = array_create_iterator(array, 0, my_extra);
	while (array_iterate(array_iterator, &value, &isnull))
	{
		position++;

		if (isnull)
		{
			elog(ERROR, "No element in array should be null");
		}

		if (DatumGetInt16(value) >= DatumGetInt16(searched_element))
			break;
	}

	array_free_iterator(array_iterator);

	/* Avoid leaking memory when handed toasted input */
	PG_FREE_IF_COPY(array, 0);


	PG_RETURN_INT16(position);
}
