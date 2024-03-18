/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

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
extern TSDLLEXPORT bool ts_array_equal(ArrayType *left, ArrayType *right);
extern TSDLLEXPORT bool ts_array_is_member(ArrayType *arr, const char *name);
extern TSDLLEXPORT void ts_array_append_stringinfo(ArrayType *arr, StringInfo info);
extern TSDLLEXPORT int ts_array_position(ArrayType *arr, const char *name);

extern TSDLLEXPORT bool ts_array_get_element_bool(ArrayType *arr, int position);
extern TSDLLEXPORT const char *ts_array_get_element_text(ArrayType *arr, int position);

extern TSDLLEXPORT ArrayType *ts_array_add_element_bool(ArrayType *arr, bool value);
extern TSDLLEXPORT ArrayType *ts_array_add_element_text(ArrayType *arr, const char *value);

extern TSDLLEXPORT ArrayType *ts_array_replace_text(ArrayType *arr, const char *old,
													const char *new);

extern TSDLLEXPORT ArrayType *ts_array_create_from_list_bool(List *values);
extern TSDLLEXPORT ArrayType *ts_array_create_from_list_text(List *values);
