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
	int32		capacity;		/* The capacity of the slices array */
	int32		num_slices;		/* The current number of slices in slices
								 * array */
	DimensionSlice *slices[FLEXIBLE_ARRAY_MEMBER];
} DimensionVec;

#define DIMENSION_VEC_SIZE(num_slices)								\
	(sizeof(DimensionVec) + sizeof(DimensionSlice *) * num_slices)

#define DIMENSION_VEC_DEFAULT_SIZE 10

extern DimensionVec *dimension_vec_create(int32 initial_num_slices);
extern DimensionVec *dimension_vec_sort(DimensionVec **vec);
extern DimensionVec *dimension_vec_add_slice_sort(DimensionVec **vec, DimensionSlice *slice);
extern DimensionVec *dimension_vec_add_slice(DimensionVec **vecptr, DimensionSlice *slice);
extern void dimension_vec_remove_slice(DimensionVec **vecptr, int32 index);
extern DimensionSlice *dimension_vec_find_slice(DimensionVec *vec, int64 coordinate);
extern int	dimension_vec_find_slice_index(DimensionVec *vec, int32 dimension_slice_id);
extern DimensionSlice *dimension_vec_get(DimensionVec *vec, int32 index);
extern void dimension_vec_free(DimensionVec *vec);

#endif							/* TIMESCALEDB_DIMENSION_VECTOR_H */
