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

/*
 * Array helper function for internal catalog arrays.
 * These are not suitable for arbitrary dimension
 * arrays but only for 1-dimensional arrays as we use
 * them in our catalog.
 */

extern TSDLLEXPORT int
ts_array_length(ArrayType *arr)
{
	if (!arr)
		return 0;

	Assert(ARR_NDIM(arr) == 1);

	return ARR_DIMS(arr)[0];
}

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
	Assert(name);

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

extern TSDLLEXPORT ArrayType *
ts_array_replace_text(ArrayType *arr, const char *old, const char *new)
{
	if (!arr)
		return NULL;

	Assert(ARR_NDIM(arr) == 1);
	Assert(arr->elemtype == TEXTOID);

	Datum datum;
	bool null;
	int pos = 1;
	ArrayIterator it = array_create_iterator(arr, 0, NULL);

	while (array_iterate(it, &datum, &null))
	{
		/*
		 * Our internal catalog arrays should either be NULL or
		 * have non-NULL members. During normal operation it should
		 * never have NULL members. If we have NULL members either
		 * the catalog is corrupted or some catalog tampering has
		 * happened.
		 */
		Ensure(!null, "array element was NULL");
		if (strncmp(TextDatumGetCString(datum), old, NAMEDATALEN) == 0)
		{
			datum = array_set_element(PointerGetDatum(arr),
									  1,
									  &pos,
									  CStringGetTextDatum(new),
									  false,
									  -1,
									  -1,
									  false,
									  TYPALIGN_INT);
			arr = DatumGetArrayTypeP(datum);
		}
		pos++;
	}

	array_free_iterator(it);
	return arr;
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

extern TSDLLEXPORT ArrayType *
ts_array_add_element_text(ArrayType *arr, const char *value)
{
	Datum val = CStringGetTextDatum(value);
	if (!arr)
	{
		return construct_array(&val, 1, TEXTOID, -1, false, TYPALIGN_INT);
	}
	else
	{
		Assert(ARR_NDIM(arr) == 1);
		Assert(arr->elemtype == TEXTOID);

		Datum d = PointerGetDatum(arr);

		int position = ts_array_length(arr);
		Assert(position);
		position++;

		d = array_set_element(d, 1, &position, val, false, -1, -1, false, TYPALIGN_INT);

		return DatumGetArrayTypeP(d);
	}
}

extern TSDLLEXPORT ArrayType *
ts_array_add_element_bool(ArrayType *arr, bool value)
{
	if (!arr)
	{
		Datum val = BoolGetDatum(value);
		return construct_array(&val, 1, BOOLOID, 1, true, TYPALIGN_CHAR);
	}
	else
	{
		Assert(ARR_NDIM(arr) == 1);
		Assert(arr->elemtype == BOOLOID);

		Datum d = PointerGetDatum(arr);

		int position = ts_array_length(arr);
		Assert(position);
		position++;

		d = array_set_element(d, 1, &position, value, false, -1, 1, true, TYPALIGN_CHAR);

		return DatumGetArrayTypeP(d);
	}
}
