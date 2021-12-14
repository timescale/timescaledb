/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/memutils.h>

#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypercube.h"
#include "subspace_store.h"

/*
 * In terms of datastructures, the subspace store is actually a tree. At the
 * root of a tree is a DimensionVec representing the different DimensionSlices
 * for the first dimension. Each of the DimensionSlices of the
 * first dimension point to a DimensionVec of the second dimension. This recurses
 * for the N dimensions. The leaf DimensionSlice points to the data being stored.
 *
 * */

typedef struct SubspaceStoreInternalNode
{
	DimensionVec *vector;
	size_t descendants;
	bool last_internal_node;
} SubspaceStoreInternalNode;

typedef struct SubspaceStore
{
	MemoryContext mcxt;
	int16 num_dimensions;
	/* limit growth of store by  limiting number of slices in first dimension,	0 for no limit */
	int16 max_items;
	SubspaceStoreInternalNode *origin; /* origin of the tree */
} SubspaceStore;

static inline SubspaceStoreInternalNode *
subspace_store_internal_node_create(bool last_internal_node)
{
	SubspaceStoreInternalNode *node = palloc(sizeof(SubspaceStoreInternalNode));

	node->vector = ts_dimension_vec_create(DIMENSION_VEC_DEFAULT_SIZE);
	node->descendants = 0;
	node->last_internal_node = last_internal_node;
	return node;
}

static inline void
subspace_store_internal_node_free(void *node)
{
	ts_dimension_vec_free(((SubspaceStoreInternalNode *) node)->vector);
	pfree(node);
}

static size_t
subspace_store_internal_node_descendants(SubspaceStoreInternalNode *node, int index)
{
	const DimensionSlice *slice = ts_dimension_vec_get(node->vector, index);

	if (slice == NULL)
		return 0;

	if (node->last_internal_node)
		return 1;

	return ((SubspaceStoreInternalNode *) slice->storage)->descendants;
}

SubspaceStore *
ts_subspace_store_init(const Hyperspace *space, MemoryContext mcxt, int16 max_items)
{
	MemoryContext old = MemoryContextSwitchTo(mcxt);
	SubspaceStore *sst = palloc(sizeof(SubspaceStore));

	/*
	 * make sure that the first dimension is a time dimension, otherwise the
	 * tree will grow in a way that makes pruning less effective.
	 */
	Assert(space->num_dimensions < 1 || space->dimensions[0].type == DIMENSION_TYPE_OPEN);

	sst->origin = subspace_store_internal_node_create(space->num_dimensions == 1);
	sst->num_dimensions = space->num_dimensions;
	/* max_items = 0 is treated as unlimited */
	sst->max_items = max_items;
	sst->mcxt = mcxt;
	MemoryContextSwitchTo(old);
	return sst;
}

void
ts_subspace_store_add(SubspaceStore *store, const Hypercube *hc, void *object,
					  void (*object_free)(void *))
{
	SubspaceStoreInternalNode *node = store->origin;
	DimensionSlice *last = NULL;
	MemoryContext old = MemoryContextSwitchTo(store->mcxt);
	int i;

	Assert(hc->num_slices == store->num_dimensions);

	for (i = 0; i < hc->num_slices; i++)
	{
		const DimensionSlice *target = hc->slices[i];
		DimensionSlice *match;

		Assert(target->storage == NULL);

		if (node == NULL)
		{
			/*
			 * We should have one internal node per dimension in the
			 * hypertable. If we don't have one for the current dimension,
			 * create one now. (There will always be one for time)
			 */
			Assert(last != NULL);
			last->storage = subspace_store_internal_node_create(i == (hc->num_slices - 1));
			last->storage_free = subspace_store_internal_node_free;
			node = last->storage;
		}

		Assert(store->max_items == 0 || node->descendants <= (size_t) store->max_items);

		/*
		 * We only call this function on a cache miss, so number of leaves
		 * will definitely increase see `Assert(last != NULL && last->storage
		 * == NULL);` at bottom.
		 */
		node->descendants += 1;

		Assert(0 == node->vector->num_slices ||
			   node->vector->slices[0]->fd.dimension_id == target->fd.dimension_id);

		/* Do we have enough space to store the object? */
		if (store->max_items > 0 && node->descendants > store->max_items)
		{
			/*
			 * Always delete the slice corresponding to the earliest time
			 * range. In the normal case that inserts are performed in
			 * time-order this is the one least likely to be reused. (Note
			 * that we made sure that the first dimension is a time dimension
			 * when creating the subspace_store). If out-of-order inserts are
			 * become significant we may wish to change this to something more
			 * sophisticated like LRU.
			 */
			size_t items_removed = subspace_store_internal_node_descendants(node, i);

			/*
			 * descendants at the root is inclusive of the descendants at the
			 * children, so if we have an overflow it must be in the time dim
			 */
			Assert(i == 0);

			Assert(store->max_items + 1 == node->descendants);

			ts_dimension_vec_remove_slice(&node->vector, i);

			/*
			 * Note we would have to do this to ancestors if this was not the
			 * root.
			 */
			node->descendants -= items_removed;
		}

		match = ts_dimension_vec_find_slice(node->vector, target->fd.range_start);

		/* Do we have a slot in this vector for the new object? */
		if (match == NULL)
		{
			DimensionSlice *copy;

			/*
			 * create a new copy of the range this slice covers, to store the
			 * object in
			 */
			copy = ts_dimension_slice_copy(target);

			ts_dimension_vec_add_slice_sort(&node->vector, copy);
			match = copy;
		}

		Assert(store->max_items == 0 || node->descendants <= (size_t) store->max_items);

		last = match;
		/* internal slices point to the next SubspaceStoreInternalNode */
		node = last->storage;
	}

	Assert(last != NULL && last->storage == NULL);
	last->storage = object; /* at the end we store the object */
	last->storage_free = object_free;
	MemoryContextSwitchTo(old);
}

void *
ts_subspace_store_get(const SubspaceStore *store, const Point *target)
{
	int i;
	DimensionVec *vec = store->origin->vector;
	DimensionSlice *match = NULL;

	Assert(target->cardinality == store->num_dimensions);

	/* The internal compressed hypertable has no dimensions as
	 * chunks are created explicitly by compress_chunk and linked
	 * to the source chunk. */
	if (store->num_dimensions == 0)
		return NULL;

	for (i = 0; i < target->cardinality; i++)
	{
		match = ts_dimension_vec_find_slice(vec, target->coordinates[i]);

		if (NULL == match)
			return NULL;

		vec = ((SubspaceStoreInternalNode *) match->storage)->vector;
	}
	Assert(match != NULL);
	return match->storage;
}

void
ts_subspace_store_free(SubspaceStore *store)
{
	subspace_store_internal_node_free(store->origin);
	pfree(store);
}

MemoryContext
ts_subspace_store_mcxt(const SubspaceStore *store)
{
	return store->mcxt;
}
