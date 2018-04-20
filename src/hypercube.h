#ifndef TIMESCALEDB_HYPERCUBE_H
#define TIMESCALEDB_HYPERCUBE_H

#include <postgres.h>

#include "dimension_slice.h"

/*
 * Hypercube is a collection of slices from N distinct dimensions, i.e., the
 * N-dimensional analogue of a cube.
 */
typedef struct Hypercube
{
	int16		capacity;		/* capacity of slices[] */
	int16		num_slices;		/* actual number of slices (should equal
								 * capacity after create) */
	/* Slices are stored in dimension order */
	DimensionSlice *slices[FLEXIBLE_ARRAY_MEMBER];
} Hypercube;

#define HYPERCUBE_SIZE(num_dimensions)									\
	(sizeof(Hypercube) + sizeof(DimensionSlice *) * (num_dimensions))


extern Hypercube *hypercube_alloc(int16 num_dimensions);
extern void hypercube_free(Hypercube *hc);
extern void hypercube_add_slice(Hypercube *hc, DimensionSlice *slice);
extern Hypercube *hypercube_from_constraints(ChunkConstraints *constraints, MemoryContext mctx);
extern Hypercube *hypercube_calculate_from_point(Hyperspace *hs, Point *p);
extern bool hypercubes_collide(Hypercube *cube1, Hypercube *cube2);
extern DimensionSlice *hypercube_get_slice_by_dimension_id(Hypercube *hc, int32 dimension_id);
extern Hypercube *hypercube_copy(Hypercube *hc);
extern void hypercube_slice_sort(Hypercube *hc);

#endif							/* TIMESCALEDB_HYPERCUBE_H */
