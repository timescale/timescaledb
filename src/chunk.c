#include <postgres.h>
#include <catalog/namespace.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <access/htup_details.h>

#include "chunk.h"
#include "catalog.h"
#include "dimension.h"
#include "partitioning.h"
#include "metadata_queries.h"

Chunk *
chunk_create(HeapTuple tuple, int16 num_constraints)
{
	Chunk	   *chunk;

	chunk = palloc0(CHUNK_SIZE(num_constraints));
	memcpy(&chunk->fd, GETSTRUCT(tuple), sizeof(FormData_chunk));
	chunk->num_constraints = num_constraints;
    chunk->table_id = get_relname_relid(chunk->fd.table_name.data,
										get_namespace_oid(chunk->fd.schema_name.data, false));
	
	return chunk;
}

Chunk *
chunk_get_or_create(Hyperspace *hs, Point *p)
{
	/* NOTE: Currently supports only two dimensions */
	Assert(hs->num_open_dimensions == 1 && hs->num_closed_dimensions <= 1);	

	if (hs->num_closed_dimensions == 1) 
		return spi_chunk_get_or_create(hs->open_dimensions[0]->fd.id,
									   p->coordinates[0],
									   hs->closed_dimensions[0]->fd.id,
									   p->coordinates[1],
									   HYPERSPACE_NUM_DIMENSIONS(hs));
 
	return spi_chunk_get_or_create(hs->open_dimensions[0]->fd.id,
								   p->coordinates[0], 0, 0,
								   HYPERSPACE_NUM_DIMENSIONS(hs));
}
