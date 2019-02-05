/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include "dimension_vector.h"

static int
cmp_slices(const void *left, const void *right)
{
	const DimensionSlice *left_slice = *((DimensionSlice **) left);
	const DimensionSlice *right_slice = *((DimensionSlice **) right);

	return ts_dimension_slice_cmp(left_slice, right_slice);
}

/*
 * identical to cmp_slices except for reversed arguments to ts_dimension_slice_cmp
 */
static int
cmp_slices_reverse(const void *left, const void *right)
{
	const DimensionSlice *left_slice = *((DimensionSlice **) left);
	const DimensionSlice *right_slice = *((DimensionSlice **) right);

	return ts_dimension_slice_cmp(right_slice, left_slice);
}

static int
cmp_coordinate_and_slice(const void *left, const void *right)
{
	int64 coord = *((int64 *) left);
	const DimensionSlice *slice = *((DimensionSlice **) right);

	return ts_dimension_slice_cmp_coordinate(slice, coord);
}

static DimensionVec *
dimension_vec_expand(DimensionVec *vec, int32 new_capacity)
{
	if (vec != NULL && vec->capacity >= new_capacity)
		return vec;

	if (NULL == vec)
		vec = palloc(DIMENSION_VEC_SIZE(new_capacity));
	else
		vec = repalloc(vec, DIMENSION_VEC_SIZE(new_capacity));

	vec->capacity = new_capacity;

	return vec;
}

DimensionVec *
ts_dimension_vec_create(int32 initial_num_slices)
{
	DimensionVec *vec = dimension_vec_expand(NULL, initial_num_slices);

	vec->capacity = initial_num_slices;
	vec->num_slices = 0;

	return vec;
}

DimensionVec *
ts_dimension_vec_sort(DimensionVec **vecptr)
{
	DimensionVec *vec = *vecptr;

	qsort(vec->slices, vec->num_slices, sizeof(DimensionSlice *), cmp_slices);

	return vec;
}

DimensionVec *
ts_dimension_vec_sort_reverse(DimensionVec **vecptr)
{
	DimensionVec *vec = *vecptr;

	qsort(vec->slices, vec->num_slices, sizeof(DimensionSlice *), cmp_slices_reverse);

	return vec;
}

DimensionVec *
ts_dimension_vec_add_slice(DimensionVec **vecptr, DimensionSlice *slice)
{
	DimensionVec *vec = *vecptr;

	/* Ensure consistent dimension */
	Assert(vec->num_slices == 0 || vec->slices[0]->fd.dimension_id == slice->fd.dimension_id);

	if (vec->num_slices + 1 > vec->capacity)
		*vecptr = vec = dimension_vec_expand(vec, vec->capacity + 10);

	vec->slices[vec->num_slices++] = slice;

	return vec;
}

DimensionVec *
ts_dimension_vec_add_unique_slice(DimensionVec **vecptr, DimensionSlice *slice)
{
	DimensionVec *vec = *vecptr;

	int32 existing_slice_index = ts_dimension_vec_find_slice_index(vec, slice->fd.id);

	if (existing_slice_index == -1)
	{
		return ts_dimension_vec_add_slice(vecptr, slice);
	}
	return vec;
}

DimensionVec *
ts_dimension_vec_add_slice_sort(DimensionVec **vecptr, DimensionSlice *slice)
{
	DimensionVec *vec;

	*vecptr = vec = ts_dimension_vec_add_slice(vecptr, slice);
	return ts_dimension_vec_sort(vecptr);
}

void
ts_dimension_vec_remove_slice(DimensionVec **vecptr, int32 index)
{
	DimensionVec *vec = *vecptr;

	ts_dimension_slice_free(vec->slices[index]);
	memmove(vec->slices + index,
			vec->slices + (index + 1),
			sizeof(DimensionSlice *) * (vec->num_slices - index - 1));
	vec->num_slices--;
}

#if defined(USE_ASSERT_CHECKING)
static inline bool
dimension_vec_is_sorted(DimensionVec *vec)
{
	int i;

	if (vec->num_slices < 2)
		return true;

	for (i = 1; i < vec->num_slices; i++)
		if (cmp_slices(&vec->slices[i - 1], &vec->slices[i]) > 0)
			return false;

	return true;
}
#endif

DimensionSlice *
ts_dimension_vec_find_slice(DimensionVec *vec, int64 coordinate)
{
	DimensionSlice **res;

	if (vec->num_slices == 0)
		return NULL;

	Assert(dimension_vec_is_sorted(vec));

	res = bsearch(&coordinate,
				  vec->slices,
				  vec->num_slices,
				  sizeof(DimensionSlice *),
				  cmp_coordinate_and_slice);

	if (res == NULL)
		return NULL;

	return *res;
}

int
ts_dimension_vec_find_slice_index(DimensionVec *vec, int32 dimension_slice_id)
{
	int i;

	for (i = 0; i < vec->num_slices; i++)
		if (dimension_slice_id == vec->slices[i]->fd.id)
			return i;

	return -1;
}

DimensionSlice *
ts_dimension_vec_get(DimensionVec *vec, int32 index)
{
	if (index >= vec->num_slices)
		return NULL;

	return vec->slices[index];
}

void
ts_dimension_vec_free(DimensionVec *vec)
{
	int i;

	for (i = 0; i < vec->num_slices; i++)
		ts_dimension_slice_free(vec->slices[i]);
	pfree(vec);
}
