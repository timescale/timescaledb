#ifndef TIMESCALEDB_SUBSPACE_STORE_H
#define TIMESCALEDB_SUBSPACE_STORE_H
#include <postgres.h>

#include "dimension.h"
#include "dimension_slice.h"

typedef struct SubspaceStore SubspaceStore;

extern SubspaceStore *subspace_store_init(int16 num_dimensions);

extern void subspace_store_add(SubspaceStore *cache, Hypercube *hc,
								   void *end_store, void (*end_store_free)(void *));

extern void *subspace_store_get(SubspaceStore *cache, Point *target);

extern void subspace_store_free(SubspaceStore *cache);

#endif /* TIMESCALEDB_SUBSPACE_STORE_H */
