#include "dimension_vector.h"

static int
cmp_slices(const void *left, const void *right)
{
	const DimensionSlice *left_slice = *((DimensionSlice **) left);
	const DimensionSlice *right_slice = *((DimensionSlice **) right);

	if (left_slice->fd.range_start == right_slice->fd.range_start)
	{
		if (left_slice->fd.range_end == right_slice->fd.range_end)
			return 0;

		if (left_slice->fd.range_end > right_slice->fd.range_end)
			return 1;

		return -1;
	}

	if (left_slice->fd.range_start > right_slice->fd.range_start)
		return 1;

	return -1;
}

static int
cmp_coordinate_and_slice(const void *left, const void *right)
{
	int64		coord = *((int64 *) left);
	const DimensionSlice *slice = *((DimensionSlice **) right);

	if (coord < slice->fd.range_start)
		return -1;

	if (coord >= slice->fd.range_end)
		return 1;

	return 0;
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
dimension_vec_create(int32 initial_num_slices)
{
	DimensionVec *vec = dimension_vec_expand(NULL, initial_num_slices);

	vec->capacity = initial_num_slices;
	vec->num_slices = 0;

	return vec;
}

DimensionVec *
dimension_vec_sort(DimensionVec **vecptr)
{
	DimensionVec *vec = *vecptr;

	qsort(vec->slices, vec->num_slices, sizeof(DimensionSlice *), cmp_slices);

	return vec;
}

DimensionVec *
dimension_vec_add_slice(DimensionVec **vecptr, DimensionSlice *slice)
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
dimension_vec_add_slice_sort(DimensionVec **vecptr, DimensionSlice *slice)
{
	DimensionVec *vec = *vecptr;

	*vecptr = vec = dimension_vec_add_slice(vecptr, slice);
	return dimension_vec_sort(vecptr);
}

void
dimension_vec_remove_slice(DimensionVec **vecptr, int32 index)
{
	DimensionVec *vec = *vecptr;

	dimension_slice_free(vec->slices[index]);
	memcpy(vec->slices + index, vec->slices + (index + 1), sizeof(DimensionSlice *) * (vec->num_slices - index - 1));
	vec->num_slices--;
}

#if defined(USE_ASSERT_CHECKING)
static inline bool
dimension_vec_is_sorted(DimensionVec *vec)
{
	int			i;

	if (vec->num_slices < 2)
		return true;

	for (i = 1; i < vec->num_slices; i++)
		if (cmp_slices(&vec->slices[i - 1], &vec->slices[i]) > 0)
			return false;

	return true;
}
#endif

DimensionSlice *
dimension_vec_find_slice(DimensionVec *vec, int64 coordinate)
{
	DimensionSlice **res;

	if (vec->num_slices == 0)
		return NULL;

	Assert(dimension_vec_is_sorted(vec));

	res = bsearch(&coordinate, vec->slices, vec->num_slices,
				  sizeof(DimensionSlice *), cmp_coordinate_and_slice);

	if (res == NULL)
		return NULL;

	return *res;
}

void
dimension_vec_free(DimensionVec *vec)
{
	int			i;

	for (i = 0; i < vec->num_slices; i++)
		dimension_slice_free(vec->slices[i]);
	pfree(vec);
}
