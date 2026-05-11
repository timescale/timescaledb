/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <fmgr.h>

#include "dimension_slice.h"
#include "dimension_vector.h"
#include "export.h"
#include "test_utils.h"

TS_FUNCTION_INFO_V1(ts_test_dimension_vector);

static void
test_ts_dimension_vec_create(void)
{
	DimensionVec *vec;

	vec = ts_dimension_vec_create(0);
	TestAssertTrue(vec != NULL);
	TestAssertInt64Eq(vec->capacity, 0);
	TestAssertInt64Eq(vec->num_slices, 0);
	ts_dimension_vec_free(vec);

	vec = ts_dimension_vec_create(100);
	TestAssertTrue(vec != NULL);
	TestAssertInt64Eq(vec->capacity, 100);
	TestAssertInt64Eq(vec->num_slices, 0);
	ts_dimension_vec_free(vec);

	// Test with overflow capcity (int32)
	TestEnsureError(ts_dimension_vec_create((PG_INT32_MIN)));
}

static void
test_ts_dimension_vec_sort(void)
{
	DimensionVec *vec;
	DimensionSlice *slice_one;
	DimensionSlice *slice_two;
	DimensionSlice *slice_three;

	vec = ts_dimension_vec_create(3);
	slice_one = ts_dimension_slice_create(1, 0, 10);
	slice_two = ts_dimension_slice_create(1, 10, 20);
	slice_three = ts_dimension_slice_create(1, 20, 30);

	slice_one->fd.id = 101;
	slice_two->fd.id = 102;
	slice_three->fd.id = 103;

	vec = ts_dimension_vec_add_unique_slice(&vec, slice_three);
	vec = ts_dimension_vec_add_unique_slice(&vec, slice_one);
	vec = ts_dimension_vec_add_unique_slice(&vec, slice_two);

	TestAssertTrue(vec->num_slices == 3);
	TestAssertTrue(vec->slices[0] == slice_three);
	TestAssertTrue(vec->slices[1] == slice_one);
	TestAssertTrue(vec->slices[2] == slice_two);

	ts_dimension_vec_sort(&vec);

	TestAssertTrue(vec->slices[0] == slice_one);
	TestAssertTrue(vec->slices[1] == slice_two);
	TestAssertTrue(vec->slices[2] == slice_three);

	ts_dimension_vec_free(vec);
}

static void
test_ts_dimension_vec_add_slice(void)
{
	DimensionVec *vec;
	DimensionSlice *slice_one;
	DimensionSlice *slice_two;
	DimensionSlice *slice_three;

	vec = ts_dimension_vec_create(2);
	slice_one = ts_dimension_slice_create(1, 0, 10);
	vec = ts_dimension_vec_add_slice(&vec, slice_one);
	TestAssertTrue(vec != NULL);
	TestAssertInt64Eq(vec->capacity, 2);
	TestAssertInt64Eq(vec->num_slices, 1);
	TestAssertTrue(vec->slices[0] == slice_one);

	slice_two = ts_dimension_slice_create(1, 0, 10);
	vec = ts_dimension_vec_add_slice(&vec, slice_two);
	TestAssertTrue(vec != NULL);
	TestAssertInt64Eq(vec->capacity, 2);
	TestAssertInt64Eq(vec->num_slices, 2);
	TestAssertTrue(vec->slices[0] == slice_one);
	TestAssertTrue(vec->slices[1] == slice_two);

	slice_three = ts_dimension_slice_create(1, 0, 10);
	vec = ts_dimension_vec_add_slice(&vec, slice_three);
	TestAssertTrue(vec != NULL);
	TestAssertInt64Eq(vec->capacity, 12);
	TestAssertInt64Eq(vec->num_slices, 3);
	TestAssertTrue(vec->slices[0] == slice_one);
	TestAssertTrue(vec->slices[1] == slice_two);
	TestAssertTrue(vec->slices[2] == slice_three);

	ts_dimension_vec_free(vec);
}

static void
test_ts_dimension_vec_add_unique_slice(void)
{
	DimensionVec *vec;
	DimensionSlice *slice;

	vec = ts_dimension_vec_create(2);
	slice = ts_dimension_slice_create(10, 0, 10);

	TestAssertTrue(vec != NULL);
	TestAssertInt64Eq(vec->capacity, 2);
	TestAssertInt64Eq(vec->num_slices, 0);

	vec = ts_dimension_vec_add_unique_slice(&vec, slice);
	TestAssertTrue(vec != NULL);
	TestAssertInt64Eq(vec->capacity, 2);
	TestAssertInt64Eq(vec->num_slices, 1);
	TestAssertTrue(vec->slices[0] == slice);

	vec = ts_dimension_vec_add_unique_slice(&vec, slice);
	TestAssertTrue(vec != NULL);
	TestAssertInt64Eq(vec->capacity, 2);
	TestAssertInt64Eq(vec->num_slices, 1);
	TestAssertTrue(vec->slices[0] == slice);

	ts_dimension_vec_free(vec);
}

static void
test_ts_dimension_vec_remove_slice(void)
{
	DimensionVec *vec;
	DimensionSlice *slice_one;
	DimensionSlice *slice_two;
	DimensionSlice *slice_three;

	vec = ts_dimension_vec_create(3);
	slice_one = ts_dimension_slice_create(1, 0, 10);
	slice_two = ts_dimension_slice_create(1, 10, 20);
	slice_three = ts_dimension_slice_create(1, 20, 30);
	slice_one->fd.id = 101;
	slice_two->fd.id = 102;
	slice_three->fd.id = 103;

	TestAssertTrue(vec != NULL);
	TestAssertInt64Eq(vec->capacity, 3);
	TestAssertInt64Eq(vec->num_slices, 0);

	vec = ts_dimension_vec_add_unique_slice(&vec, slice_one);
	vec = ts_dimension_vec_add_unique_slice(&vec, slice_two);
	vec = ts_dimension_vec_add_unique_slice(&vec, slice_three);

	TestAssertTrue(vec->num_slices == 3);

	TestAssertTrue(vec->slices[0] == slice_one);
	TestAssertTrue(vec->slices[1] == slice_two);
	TestAssertTrue(vec->slices[2] == slice_three);

	ts_dimension_vec_remove_slice(&vec, 0);

	TestAssertTrue(vec->num_slices == 2);

	TestAssertTrue(vec->slices[0] == slice_two);
	TestAssertTrue(vec->slices[1] == slice_three);

	ts_dimension_vec_remove_slice(&vec, 1);

	TestAssertTrue(vec->num_slices == 1);

	TestAssertTrue(vec->slices[0] == slice_two);

	ts_dimension_vec_remove_slice(&vec, 0);

	TestAssertTrue(vec->num_slices == 0);

	ts_dimension_vec_free(vec);
}

static void
test_ts_dimension_vec_find_slice(void)
{
	DimensionVec *vec;
	DimensionSlice *slice_one;
	DimensionSlice *slice_two;
	DimensionSlice *slice_three;

	vec = ts_dimension_vec_create(3);
	slice_one = ts_dimension_slice_create(1, 0, 10);
	slice_two = ts_dimension_slice_create(1, 20, 30);
	slice_three = ts_dimension_slice_create(1, 30, 40);

	slice_one->fd.id = 101;
	slice_two->fd.id = 102;
	slice_three->fd.id = 103;

	vec = ts_dimension_vec_add_unique_slice(&vec, slice_one);
	vec = ts_dimension_vec_add_unique_slice(&vec, slice_two);
	vec = ts_dimension_vec_add_unique_slice(&vec, slice_three);

	TestAssertTrue(vec->num_slices == 3);

	TestAssertTrue(ts_dimension_vec_find_slice(vec, 5) == slice_one);
	TestAssertTrue(ts_dimension_vec_find_slice(vec, 25) == slice_two);
	TestAssertTrue(ts_dimension_vec_find_slice(vec, 35) == slice_three);

	// Inclusive start, exclusive end
	TestAssertTrue(ts_dimension_vec_find_slice(vec, 0) == slice_one);
	TestAssertTrue(ts_dimension_vec_find_slice(vec, 10) == NULL);

	TestAssertTrue(ts_dimension_vec_find_slice(vec, -5) == NULL);
	TestAssertTrue(ts_dimension_vec_find_slice(vec, 45) == NULL);

	ts_dimension_vec_free(vec);
}

static void
test_ts_dimension_vec_find_slice_index(void)
{
	DimensionVec *vec;
	DimensionSlice *slice_one;
	DimensionSlice *slice_two;
	DimensionSlice *slice_three;

	vec = ts_dimension_vec_create(3);
	slice_one = ts_dimension_slice_create(1, 0, 10);
	slice_two = ts_dimension_slice_create(1, 10, 20);
	slice_three = ts_dimension_slice_create(1, 20, 30);

	slice_one->fd.id = 101;
	slice_two->fd.id = 102;
	slice_three->fd.id = 103;

	vec = ts_dimension_vec_add_unique_slice(&vec, slice_three);
	vec = ts_dimension_vec_add_unique_slice(&vec, slice_one);
	vec = ts_dimension_vec_add_unique_slice(&vec, slice_two);

	TestAssertTrue(vec->num_slices == 3);

	TestAssertInt64Eq(ts_dimension_vec_find_slice_index(vec, 103), 0);
	TestAssertInt64Eq(ts_dimension_vec_find_slice_index(vec, 101), 1);
	TestAssertInt64Eq(ts_dimension_vec_find_slice_index(vec, 102), 2);

	TestAssertInt64Eq(ts_dimension_vec_find_slice_index(vec, 100), -1);
	TestAssertInt64Eq(ts_dimension_vec_find_slice_index(vec, 104), -1);

	ts_dimension_vec_free(vec);
}

Datum
ts_test_dimension_vector(PG_FUNCTION_ARGS)
{
	test_ts_dimension_vec_create();
	test_ts_dimension_vec_sort();
	test_ts_dimension_vec_add_slice();
	test_ts_dimension_vec_add_unique_slice();
	test_ts_dimension_vec_remove_slice();
	test_ts_dimension_vec_find_slice();
	test_ts_dimension_vec_find_slice_index();

	PG_RETURN_VOID();
}
