/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_DIMENSION_VECTOR_H
#define TIMESCALEDB_DIMENSION_VECTOR_H

#include <postgres.h>

#include "dimension_slice.h"

/*
 *	DimensionVec is a collection of slices (ranges) along one dimension for a
 *	time range.
 */
typedef struct DimensionVec
{
	int32 capacity;   /* The capacity of the slices array */
	int32 num_slices; /* The current number of slices in slices
					   * array */
	DimensionSlice *slices[FLEXIBLE_ARRAY_MEMBER];
} DimensionVec;

#define DIMENSION_VEC_SIZE(num_slices)                                                             \
	(sizeof(DimensionVec) + sizeof(DimensionSlice *) * num_slices)

#define DIMENSION_VEC_DEFAULT_SIZE 10

extern DimensionVec *ts_dimension_vec_create(int32 initial_num_slices);
extern DimensionVec *ts_dimension_vec_sort(DimensionVec **vec);
extern DimensionVec *ts_dimension_vec_sort_reverse(DimensionVec **vec);
extern DimensionVec *ts_dimension_vec_add_slice_sort(DimensionVec **vec, DimensionSlice *slice);
extern DimensionVec *ts_dimension_vec_add_slice(DimensionVec **vecptr, DimensionSlice *slice);
extern DimensionVec *ts_dimension_vec_add_unique_slice(DimensionVec **vecptr,
													   DimensionSlice *slice);
extern void ts_dimension_vec_remove_slice(DimensionVec **vecptr, int32 index);
extern DimensionSlice *ts_dimension_vec_find_slice(DimensionVec *vec, int64 coordinate);
extern int ts_dimension_vec_find_slice_index(DimensionVec *vec, int32 dimension_slice_id);
extern DimensionSlice *ts_dimension_vec_get(DimensionVec *vec, int32 index);
extern void ts_dimension_vec_free(DimensionVec *vec);

#endif /* TIMESCALEDB_DIMENSION_VECTOR_H */
