#include <postgres.h>
#include <access/htup_details.h>
#include <utils/lsyscache.h>
#include <nodes/memnodes.h>
#include <catalog/namespace.h>
#include <utils/memutils.h>

#include "hypertable.h"
#include "dimension.h"
#include "chunk.h"
#include "subspace_store.h"
#include "hypertable_cache.h"

Hypertable *
hypertable_from_tuple(HeapTuple tuple)
{
	Hypertable *h;
	Oid			namespace_oid;

	h = palloc0(sizeof(Hypertable));
	memcpy(&h->fd, GETSTRUCT(tuple), sizeof(FormData_hypertable));
	namespace_oid = get_namespace_oid(NameStr(h->fd.schema_name), false);
	h->main_table_relid = get_relname_relid(NameStr(h->fd.table_name), namespace_oid);
	h->space = dimension_scan(h->fd.id, h->main_table_relid, h->fd.num_dimensions);
	h->chunk_cache = subspace_store_init(HYPERSPACE_NUM_DIMENSIONS(h->space), CurrentMemoryContext);

	return h;
}

Chunk *
hypertable_get_chunk(Hypertable *h, Point *point)
{
	Chunk	   *chunk = subspace_store_get(h->chunk_cache, point);

	if (NULL == chunk)
	{
		MemoryContext old;

		/*
		 * chunk_find() must execute on the transaction memory context since
		 * it allocates a lot of transient data. We don't want this allocated
		 * on the cache's memory context.
		 */
		chunk = chunk_find(h->space, point);

		old = MemoryContextSwitchTo(subspace_store_mcxt(h->chunk_cache));

		if (NULL == chunk)
			chunk = chunk_create(h->space, point);
		else
			/* Make a copy which lives in the chunk cache's memory context */
			chunk = chunk_copy(chunk);

		Assert(NULL != chunk);
		subspace_store_add(h->chunk_cache, chunk->cube, chunk, pfree);
		MemoryContextSwitchTo(old);
	}

	Assert(MemoryContextContains(subspace_store_mcxt(h->chunk_cache), chunk));
	return chunk;
}

static inline Oid
hypertable_relid_lookup(Oid relid)
{
	Cache	   *hcache = hypertable_cache_pin();
	Hypertable *ht = hypertable_cache_get_entry(hcache, relid);
	Oid			result = ht == NULL ? InvalidOid : ht->main_table_relid;

	cache_release(hcache);

	return result;
}

/*
 * Returns a hypertable's relation ID (OID) iff the given RangeVar corresponds to
 * a hypertable, otherwise InvalidOid.
*/
Oid
hypertable_relid(RangeVar *rv)
{
	return hypertable_relid_lookup(RangeVarGetRelid(rv, NoLock, true));
}

bool
is_hypertable(Oid relid)
{
	return hypertable_relid_lookup(relid) != InvalidOid;
}
