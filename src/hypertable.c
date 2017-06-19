#include <postgres.h>
#include <access/htup_details.h>
#include <utils/lsyscache.h>
#include <catalog/namespace.h>

#include "hypertable.h"
#include "dimension.h"
#include "chunk.h"
#include "subspace_store.h"

Hypertable *
hypertable_from_tuple(HeapTuple tuple)
{
	Hypertable *h;
	Oid namespace_oid;

	h = palloc0(sizeof(Hypertable));
	memcpy(&h->fd, GETSTRUCT(tuple), sizeof(FormData_hypertable));
	namespace_oid = get_namespace_oid(NameStr(h->fd.schema_name), false);
	h->main_table_relid = get_relname_relid(NameStr(h->fd.table_name), namespace_oid);
	h->space = dimension_scan(h->fd.id, h->main_table_relid);
	h->chunk_cache = subspace_store_init(h->space->num_closed_dimensions + h->space->num_open_dimensions, CurrentMemoryContext);

	return h;
}

Dimension *
hypertable_get_open_dimension(Hypertable *h)
{
	if (h->space->num_open_dimensions == 0)
		return NULL;

	Assert(h->space->num_open_dimensions == 1);
	return h->space->open_dimensions[0];
}

Dimension *
hypertable_get_closed_dimension(Hypertable *h)
{
	if (h->space->num_closed_dimensions == 0)
		return NULL;

	Assert(h->space->num_closed_dimensions == 1);
	return h->space->closed_dimensions[0];
}

Chunk *hypertable_get_chunk(Hypertable *h, Point *point)
{
	Chunk *chunk = subspace_store_get(h->chunk_cache, point);

	if (NULL == chunk)
	{
		Hypercube *hc;
		MemoryContext old = MemoryContextSwitchTo(subspace_store_mcxt(h->chunk_cache));

		chunk = chunk_get_or_create_new(h->space, point); 
		/*
		chunk = chunk_find(h->space, point);

		if (NULL == chunk)
		{
			chunk = chunk_create_new(h->space, point);
			elog(NOTICE, "Created new chunk %d", chunk->fd.id);
		}
		*/
		Assert(NULL != chunk);
		
		hc = hypercube_from_constraints(chunk->constraints, chunk->num_constraints);
		chunk->cube = hc;
		subspace_store_add(h->chunk_cache, hc, chunk, pfree);
		MemoryContextSwitchTo(old);
	}

	return chunk;
}
