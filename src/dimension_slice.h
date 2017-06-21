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
	int16 num_dimensions;
	int16 num_slices;
	/* Open slices are stored before closed slices */
	DimensionSlice *slices[0];
} Hypercube;

#define HYPERCUBE_NUM_SLICES(hc) \
	((hc)->num_open_slices + (hc)->num_closed_slices)

#define HYPERCUBE_SIZE(num_dimensions)			\
	(sizeof(Hypercube) + sizeof(DimensionSlice *) * num_dimensions)

/*
 *  DimensionAxis is a collection of all slices (ranges) along one dimension for
 *  a time range.
 */
typedef struct DimensionAxis
{
	DimensionType type;
	int32 num_slots; /* The allocated num slots in slices array */
	int32 num_slices; /* The current number of slices in slices array */
	DimensionSlice *slices[0];
} DimensionAxis;

extern DimensionSlice *dimension_slice_scan(int32 dimension_id, int64 coordinate);
extern Hypercube *dimension_slice_point_scan(Hyperspace *space, int64 point[]);
extern Hypercube *dimension_slice_point_scan_heap(Hyperspace *space, int64 point[]);
extern void dimension_slice_free(DimensionSlice *slice);
extern DimensionAxis *dimension_axis_create(DimensionType type, int32 num_slices);
extern int32 dimension_axis_add_slice(DimensionAxis **axis, DimensionSlice *slice);
extern int32 dimension_axis_add_slice_sort(DimensionAxis **axis, DimensionSlice *slice);
extern DimensionSlice *dimension_axis_find_slice(DimensionAxis *axis, int64 coordinate);
extern void dimension_axis_free(DimensionAxis *axis);
extern Hypercube *hypercube_from_constraints(ChunkConstraint constraints[], int16 num_constraints);

#endif /* TIMESCALEDB_DIMENSION_SLICE_H */
