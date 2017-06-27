#include <postgres.h>

#include "dimension.h"
#include "dimension_slice.h"
#include "subspace_store.h"

/*
 * In terms of datastructures, the subspace store is actually a tree. At the
 * root of a tree is a DimensionVec representing the different DimensionSlices
 * for the first dimension. Each of the DimensionSlices of the
 * first dimension point to a DimensionVec of the second dimension. This recurses
 * for the N dimensions. The leaf DimensionSlice points to the data being stored.
 *
 * */
typedef struct SubspaceStore
{
	MemoryContext mcxt;
	int16		num_dimensions;
	DimensionVec *origin;		/* origin of the tree */
} SubspaceStore;

static inline DimensionVec *
subspace_store_dimension_create()
{
	return dimension_vec_create(DIMENSION_VEC_DEFAULT_SIZE);
}

SubspaceStore *
subspace_store_init(int16 num_dimensions, MemoryContext mcxt)
{
	MemoryContext old = MemoryContextSwitchTo(mcxt);
	SubspaceStore *sst = palloc(sizeof(SubspaceStore));

	sst->origin = subspace_store_dimension_create();
	sst->num_dimensions = num_dimensions;
	sst->mcxt = mcxt;
	MemoryContextSwitchTo(old);
	return sst;
}

static inline void
subspace_store_free_internal_node(void *node)
{
	dimension_vec_free((DimensionVec *) node);
}

void
subspace_store_add(SubspaceStore *store, const Hypercube *hc,
				   void *object, void (*object_free) (void *))
{
	DimensionVec **vecptr = &store->origin;
	DimensionSlice *last = NULL;
	MemoryContext old = MemoryContextSwitchTo(store->mcxt);
	int			i;

	Assert(hc->num_slices == store->num_dimensions);

	for (i = 0; i < hc->num_slices; i++)
	{
		const DimensionSlice *target = hc->slices[i];
		DimensionSlice *match;
		DimensionVec *vec = *vecptr;

		Assert(target->storage == NULL);

		if (vec == NULL)
		{
			last->storage = subspace_store_dimension_create();
			last->storage_free = subspace_store_free_internal_node;
			vec = last->storage;
		}

		if (vec->num_slices > 0)
		{
			Assert(vec->slices[0]->fd.dimension_id == target->fd.dimension_id);
		}

		match = dimension_vec_find_slice(vec, target->fd.range_start);

		if (match == NULL)
		{
			DimensionSlice *copy;
			if (i == 0 && vec->num_slices > 0)
			{
				/*
				 * At dimension 0 only keep one slice. This is an optimization
				 * to prevent this store from growing too large.
				 */
				Assert(vec->num_slices = 1);
				dimension_vec_remove_slice(vecptr, 0);
			}
			copy = dimension_slice_copy(target);

			dimension_vec_add_slice_sort(vecptr, copy);
			match = copy;
		}

		last = match;
		/* internal nodes point to the next dimension's vector */
		vecptr = (DimensionVec **) &last->storage;
	}

	Assert(last->storage == NULL);
	last->storage = object;		/* at the end we store the object */
	last->storage_free = object_free;
	MemoryContextSwitchTo(old);
}


void *
subspace_store_get(SubspaceStore *store, Point *target)
{
	int			i;
	DimensionVec *vec = store->origin;
	DimensionSlice *match = NULL;

	Assert(target->cardinality == store->num_dimensions);

	for (i = 0; i < target->cardinality; i++)
	{
		match = dimension_vec_find_slice(vec, target->coordinates[i]);

		if (NULL == match)
			return NULL;

		vec = match->storage;
	}
	return match->storage;
}

void
subspace_store_free(SubspaceStore *store)
{
	dimension_vec_free(store->origin);
	pfree(store);
}

MemoryContext
subspace_store_mcxt(SubspaceStore *store)
{
	return store->mcxt;
}
