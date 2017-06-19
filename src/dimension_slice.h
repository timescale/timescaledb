#ifndef TIMESCALEDB_DIMENSION_SLICE_H
#define TIMESCALEDB_DIMENSION_SLICE_H

#include <postgres.h>
#include <nodes/pg_list.h>

#include "catalog.h"
#include "dimension.h"
#include "chunk_constraint.h"

typedef struct DimensionSlice
{
	FormData_dimension_slice fd;
	DimensionType type;
	void (*storage_free)(void *);
	void *storage; //used in the cache
} DimensionSlice;

/*
 * Hypercube is a collection of slices from N distinct dimensions, i.e., the
 * N-dimensional analogue of a square or a cube.
 */
typedef struct Hypercube
{
	int16 num_dimensions; 	/* capacity of slices[] */
	int16 num_slices; 		/* actual number of slices (should equal num_dimensions after create) */
	/* Open slices are stored before closed slices */
	DimensionSlice *slices[0];
} Hypercube;

#define HYPERCUBE_NUM_SLICES(hc)						\
	((hc)->num_open_slices + (hc)->num_closed_slices)

#define HYPERCUBE_SIZE(num_dimensions)								\
	(sizeof(Hypercube) + sizeof(DimensionSlice *) * num_dimensions)

/*
 *  DimensionVec is a collection of slices (ranges) along one dimension for a
 *  time range.
 */
typedef struct DimensionVec
{
	int32 num_slots; /* The allocated num slots in slices array */
	int32 num_slices; /* The current number of slices in slices array */
	DimensionSlice *slices[0];
} DimensionVec;

#define DIMENSION_VEC_SIZE(num_slices)								\
	(sizeof(DimensionVec) + sizeof(DimensionSlice *) * num_slices)

#define DIMENSION_VEC_DEFAULT_SIZE 10

extern DimensionVec *dimension_slice_scan(int32 dimension_id, int64 coordinate);
extern Hypercube *dimension_slice_point_scan(Hyperspace *space, int64 point[]);
extern DimensionSlice *dimension_slice_copy(const DimensionSlice *original);
extern void dimension_slice_free(DimensionSlice *slice);
extern DimensionVec *dimension_vec_create(int32 initial_num_slices);
extern DimensionVec *dimension_vec_add_slice(DimensionVec **vec, DimensionSlice *slice);
extern DimensionVec *dimension_vec_add_slice_sort(DimensionVec **vec, DimensionSlice *slice);
extern DimensionSlice *dimension_vec_find_slice(DimensionVec *vec, int64 coordinate);
extern void dimension_vec_free(DimensionVec *vec);
extern Hypercube *hypercube_from_constraints(ChunkConstraint constraints[], int16 num_constraints);

#endif /* TIMESCALEDB_DIMENSION_SLICE_H */
