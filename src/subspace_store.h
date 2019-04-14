/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_SUBSPACE_STORE_H
#define TIMESCALEDB_SUBSPACE_STORE_H

#include <postgres.h>
#include "dimension.h"

/* A subspace store allows you to save data associated with
 * a multidimensional-subspace. Subspaces are defined conceptually
 * via a Hypercube (that is a collection of slices -- one for each dimension).
 * Thus, a subspace is a "rectangular" cutout in a multidimensional space.
 */

typedef struct Hypercube Hypercube;
typedef struct Point Point;
typedef struct SubspaceStore SubspaceStore;

extern SubspaceStore *ts_subspace_store_init(Hyperspace *space, MemoryContext mcxt,
											 int16 max_items);

/* Store an object associate with the subspace represented by a hypercube */
extern void ts_subspace_store_add(SubspaceStore *cache, const Hypercube *hc, void *object,
								  void (*object_free)(void *));

/* Get the object stored for the subspace that a point is in.
 * Return the object stored or NULL if this subspace is not in the store.
 */
extern void *ts_subspace_store_get(SubspaceStore *cache, Point *target);
extern void ts_subspace_store_free(SubspaceStore *cache);
extern MemoryContext ts_subspace_store_mcxt(SubspaceStore *cache);

#endif /* TIMESCALEDB_SUBSPACE_STORE_H */
