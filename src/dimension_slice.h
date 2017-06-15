#ifndef TIMESCALEDB_DIMENSION_SLICE_H
#define TIMESCALEDB_DIMENSION_SLICE_H

#include <postgres.h>

#include "catalog.h"
#include "dimension.h"

typedef struct DimensionSlice
{
	FormData_dimension_slice fd;
} DimensionSlice;

#define MAX_SLICES 100

/*
 *  DimensionAxis is a collection of all slices (ranges) along one dimension.
 */
typedef struct DimensionAxis
{
	DimensionType type;
	int16 num_slices;
	DimensionSlice *slices[MAX_SLICES];
} DimensionAxis;

/*
 * Hypercube is a collection of slices from N distinct dimensions, i.e., the
 * N-dimensional analogue of a square or a cube.
 */
typedef struct Hypercube
{
	int16 num_time_slices;
	int16 num_space_slices;
	union {
		struct {
			DimensionSlice *time_slices[MAX_TIME_DIMENSIONS];
			DimensionSlice *space_slices[MAX_SPACE_DIMENSIONS];
		};
		DimensionSlice *slices[MAX_DIMENSIONS];
	};
} Hypercube;

#define HYPERCUBE_NUM_SLICES(hc) \
	((hc)->num_time_slices + (hc)->num_space_slices)

extern DimensionSlice *dimension_slice_scan(int32 dimension_id, int64 coordinate);
extern Hypercube *dimension_slice_point_scan(Hyperspace *space, int64 point[]);
extern Hypercube *dimension_slice_point_scan_heap(Hyperspace *space, int64 point[]);

#endif /* TIMESCALEDB_DIMENSION_SLICE_H */
