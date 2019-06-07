/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_HYPERCUBE_H
#define TIMESCALEDB_HYPERCUBE_H

#include <postgres.h>
#include <utils/jsonb.h>

#include "dimension_slice.h"

/*
 * Hypercube is a collection of slices from N distinct dimensions, i.e., the
 * N-dimensional analogue of a cube.
 */
typedef struct Hypercube
{
	int16 capacity;   /* capacity of slices[] */
	int16 num_slices; /* actual number of slices (should equal
					   * capacity after create) */
	/* Slices are stored in dimension order */
	DimensionSlice *slices[FLEXIBLE_ARRAY_MEMBER];
} Hypercube;

#define HYPERCUBE_SIZE(num_dimensions)                                                             \
	(sizeof(Hypercube) + sizeof(DimensionSlice *) * (num_dimensions))

extern TSDLLEXPORT Hypercube *ts_hypercube_alloc(int16 num_dimensions);
extern void ts_hypercube_free(Hypercube *hc);
extern TSDLLEXPORT void ts_hypercube_add_slice(Hypercube *hc, DimensionSlice *slice);
extern Hypercube *ts_hypercube_from_constraints(ChunkConstraints *constraints, MemoryContext mctx);
extern Hypercube *ts_hypercube_calculate_from_point(Hyperspace *hs, Point *p);
extern bool ts_hypercubes_collide(Hypercube *cube1, Hypercube *cube2);
extern TSDLLEXPORT DimensionSlice *ts_hypercube_get_slice_by_dimension_id(Hypercube *hc,
																		  int32 dimension_id);
extern Hypercube *ts_hypercube_copy(Hypercube *hc);
extern bool ts_hypercube_equal(Hypercube *hc1, Hypercube *hc2);
extern void ts_hypercube_slice_sort(Hypercube *hc);

#endif /* TIMESCALEDB_HYPERCUBE_H */
