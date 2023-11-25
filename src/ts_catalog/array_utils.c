/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/pg_type.h>
#include <utils/array.h>
#include <utils/builtins.h>

#include <debug_assert.h>
#include "array_utils.h"

extern TSDLLEXPORT int
ts_array_length(ArrayType *arr)
{
	if (!arr)
		return 0;

	return ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
}

/*
 * Array helper function for internal catalog arrays.
 * These are not suitable for arbitrary dimension
 * arrays but only for 1-dimensional arrays as we use
 * them in our catalog.
 */

extern TSDLLEXPORT bool
ts_array_is_member(ArrayType *arr, const char *name)
{
	bool ret = false;
	Datum datum;
	bool null;
	if (!arr)
		return ret;

	Assert(ARR_NDIM(arr) == 1);
	Assert(arr->elemtype == TEXTOID);

	ArrayIterator it = array_create_iterator(arr, 0, NULL);
	while (array_iterate(it, &datum, &null))
	{
		Assert(!null);
		/*
		 * Our internal catalog arrays should either be NULL or
		 * have non-NULL members. During normal operation it should
		 * never have NULL members. If we have NULL members either
		 * the catalog is corrupted or some catalog tampering has
		 * happened.
		 */
		Ensure(!null, "array element was NULL");
		if (strncmp(TextDatumGetCString(datum), name, NAMEDATALEN) == 0)
		{
			ret = true;
			break;
		}
	}

	array_free_iterator(it);
	return ret;
}

extern TSDLLEXPORT int
ts_array_position(ArrayType *arr, const char *name)
{
	int pos = 0;
	Datum datum;
	bool found = false;
	bool null;
	if (!arr)
		return pos;

	Assert(ARR_NDIM(arr) == 1);
	Assert(arr->elemtype == TEXTOID);

	ArrayIterator it = array_create_iterator(arr, 0, NULL);
	while (array_iterate(it, &datum, &null))
	{
		pos++;
		/*
		 * Our internal catalog arrays should either be NULL or
		 * have non-NULL members. During normal operation it should
		 * never have NULL members. If we have NULL members either
		 * the catalog is corrupted or some catalog tampering has
		 * happened.
		 */
		Ensure(!null, "array element was NULL");
		if (strncmp(TextDatumGetCString(datum), name, NAMEDATALEN) == 0)
		{
			found = true;
			break;
		}
	}

	array_free_iterator(it);
	return found ? pos : 0;
}

extern TSDLLEXPORT bool
ts_array_get_element_bool(ArrayType *arr, int position)
{
	Assert(arr);
	Assert(ARR_NDIM(arr) == 1);
	Assert(arr->elemtype == BOOLOID);
	bool isnull;

	Datum value = array_get_element(PointerGetDatum(arr), 1, &position, -1, 1, true, 'c', &isnull);
	Ensure(!isnull, "invalid array position");

	return DatumGetBool(value);
}

extern TSDLLEXPORT const char *
ts_array_get_element_text(ArrayType *arr, int position)
{
	Assert(arr);
	Assert(ARR_NDIM(arr) == 1);
	Assert(arr->elemtype == TEXTOID);
	bool isnull;

	Datum value =
		array_get_element(PointerGetDatum(arr), 1, &position, -1, -1, false, 'i', &isnull);
	Ensure(!isnull, "invalid array position");

	return TextDatumGetCString(value);
}
