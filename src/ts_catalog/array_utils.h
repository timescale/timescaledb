/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/array.h>

#include "export.h"

/*
 * Array helper function for internal catalog arrays.
 * These are not suitable for arbitrary dimension
 * arrays but only for 1-dimensional arrays as we use
 * them in our catalog.
 */

extern TSDLLEXPORT int ts_array_length(ArrayType *arr);
extern TSDLLEXPORT bool ts_array_is_member(ArrayType *arr, const char *name);
extern TSDLLEXPORT int ts_array_position(ArrayType *arr, const char *name);
extern TSDLLEXPORT bool ts_array_get_element_bool(ArrayType *arr, int position);
extern TSDLLEXPORT const char *ts_array_get_element_text(ArrayType *arr, int position);
