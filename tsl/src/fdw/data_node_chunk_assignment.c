/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <foreign/foreign.h>
#include <utils/hsearch.h>
#include <utils/fmgrprotos.h>
#include <parser/parsetree.h>
#include <nodes/bitmapset.h>

#include "data_node_chunk_assignment.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypercube.h"
#include "chunk.h"
#include "ts_catalog/chunk_data_node.h"
#include "relinfo.h"
#include "planner.h"

/*
 * Find an existing data node chunk assignment or initialize a new one.
 */
static DataNodeChunkAssignment *
get_or_create_sca(DataNodeChunkAssignments *scas, Oid serverid, RelOptInfo *rel)
{
	DataNodeChunkAssignment *sca;
	bool found;

	Assert(rel == NULL || rel->serverid == serverid);

	sca = hash_search(scas->assignments, &serverid, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry */
		memset(sca, 0, sizeof(*sca));
		sca->node_server_oid = serverid;
	}

	return sca;
}

static const DimensionSlice *
get_slice_for_dimension(Chunk *chunk, int32 dimension_id)
{
	return ts_hypercube_get_slice_by_dimension_id(chunk->cube, dimension_id);
}

/*
 * Assign the given chunk relation to a data node.
 *
 * The chunk is assigned according to the strategy set in the
 * DataNodeChunkAssignments state.
 */
DataNodeChunkAssignment *
data_node_chunk_assignment_assign_chunk(DataNodeChunkAssignments *scas, RelOptInfo *chunkrel)
{
	DataNodeChunkAssignment *sca = get_or_create_sca(scas, chunkrel->serverid, NULL);
	TimescaleDBPrivate *chunk_private = ts_get_private_reloptinfo(chunkrel);
	MemoryContext old;

	/* Should never assign the same chunk twice */
	Assert(!bms_is_member(chunkrel->relid, sca->chunk_relids));

	/* If this is the first chunk we assign to this data node, increment the
	 * number of data nodes with one or more chunks on them */
	if (list_length(sca->chunks) == 0)
		scas->num_nodes_with_chunks++;

	scas->total_num_chunks++;

	/*
	 * Use the cached ChunkDataNode data to find the relid of the chunk on the
	 * data node.
	 */
	Oid remote_chunk_relid = InvalidOid;
	ListCell *lc;
	foreach (lc, chunk_private->chunk->data_nodes)
	{
		ChunkDataNode *cdn = (ChunkDataNode *) lfirst(lc);
		if (cdn->foreign_server_oid == chunkrel->serverid)
		{
			remote_chunk_relid = cdn->fd.node_chunk_id;
			break;
		}
	}
	Assert(remote_chunk_relid != InvalidOid);

	/*
	 * Fill the data node chunk assignment struct.
	 */
	old = MemoryContextSwitchTo(scas->mctx);
	sca->chunk_relids = bms_add_member(sca->chunk_relids, chunkrel->relid);
	sca->chunks = lappend(sca->chunks, chunk_private->chunk);
	sca->remote_chunk_ids = lappend_int(sca->remote_chunk_ids, remote_chunk_relid);
	sca->pages += chunkrel->pages;
	sca->rows += chunkrel->rows;
	sca->tuples += chunkrel->tuples;
	MemoryContextSwitchTo(old);

	return sca;
}

/*
 * Initialize a new chunk assignment state with a specific assignment strategy.
 */
void
data_node_chunk_assignments_init(DataNodeChunkAssignments *scas,
								 DataNodeChunkAssignmentStrategy strategy, PlannerInfo *root,
								 unsigned int nrels_hint)
{
	HASHCTL hctl = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(DataNodeChunkAssignment),
		.hcxt = CurrentMemoryContext,
	};

	scas->strategy = strategy;
	scas->root = root;
	scas->mctx = hctl.hcxt;
	scas->total_num_chunks = 0;
	scas->num_nodes_with_chunks = 0;
	scas->assignments = hash_create("data node chunk assignments",
									nrels_hint,
									&hctl,
									HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
}

/*
 * Assign chunks to data nodes.
 *
 * Each chunk in the chunkrels array is a assigned a data node using the strategy
 * set in the DataNodeChunkAssignments state.
 */
DataNodeChunkAssignments *
data_node_chunk_assignment_assign_chunks(DataNodeChunkAssignments *scas, RelOptInfo **chunkrels,
										 unsigned int nrels)
{
	unsigned int i;

	Assert(scas->assignments != NULL && scas->root != NULL);

	for (i = 0; i < nrels; i++)
	{
		RelOptInfo *chunkrel = chunkrels[i];

		Assert(IS_SIMPLE_REL(chunkrel) && chunkrel->fdw_private != NULL);
		data_node_chunk_assignment_assign_chunk(scas, chunkrel);
	}

	return scas;
}

/*
 * Get the data node assignment for the given relation (chunk).
 */
DataNodeChunkAssignment *
data_node_chunk_assignment_get_or_create(DataNodeChunkAssignments *scas, RelOptInfo *rel)
{
	return get_or_create_sca(scas, rel->serverid, rel);
}

/*
 * Check if a dimension slice overlaps with other slices.
 *
 * This is a naive implementation that runs in linear time. A more efficient
 * approach would be to use, e.g., an interval tree.
 */
static bool
dimension_slice_overlaps_with_others(const DimensionSlice *slice, const List *other_slices)
{
	ListCell *lc;

	foreach (lc, other_slices)
	{
		const DimensionSlice *other_slice = lfirst(lc);

		if (ts_dimension_slices_collide(slice, other_slice))
			return true;
	}

	return false;
}

/*
 * DataNodeSlice: a hash table entry to track the data node a chunk slice is placed
 * on.
 */
typedef struct DataNodeSlice
{
	int32 sliceid;
	Oid node_serverid;
} DataNodeSlice;

/*
 * Check whether chunks are assigned in an overlapping way.
 *
 * Assignments are overlapping if any data node has a chunk that overlaps (in the
 * given paritioning dimension) with a chunk on another data node. There are two
 * cases when this can happen:
 *
 * 1. The same slice exists on multiple data nodes (we optimize for detecting
 * this).
 *
 * 2. Two different slices overlap while existing on different data nodes (this
 * case is more costly to detect).
 */
bool
data_node_chunk_assignments_are_overlapping(DataNodeChunkAssignments *scas,
											int32 partitioning_dimension_id)
{
	HASH_SEQ_STATUS status;
	HASHCTL hashctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(DataNodeSlice),
		.hcxt = CurrentMemoryContext,
	};
	HTAB *all_data_node_slice_htab;
	DataNodeChunkAssignment *sca;
	List *all_data_node_slices = NIL;

	/* No overlapping can occur if there are chunks on only one data node (this
	 * covers also the case of a single chunk) */
	if (scas->num_nodes_with_chunks <= 1)
		return false;

	/* If there are multiple data nodes with chunks and they are not placed along
	 * a closed "space" dimension, we assume overlapping */
	if (partitioning_dimension_id <= 0)
		return true;

	/* Use a hash table to track slice data node mappings by slice ID. The same
	 * slice can exist on multiple data nodes, causing an overlap across data nodes
	 * in the slice dimension. This hash table is used to quickly detect such
	 * "same-slice overlaps" and avoids having to do a more expensive range
	 * overlap check.
	 */
	all_data_node_slice_htab = hash_create("all_data_node_slices",
										   scas->total_num_chunks,
										   &hashctl,
										   HASH_ELEM | HASH_BLOBS);

	hash_seq_init(&status, scas->assignments);

	while ((sca = hash_seq_search(&status)))
	{
		List *data_node_slices = NIL;
		ListCell *lc;

		/* Check each slice on the data node against the slices on other
		 * data nodes */
		foreach (lc, sca->chunks)
		{
			Chunk *chunk = (Chunk *) lfirst(lc);
			const DimensionSlice *slice;
			DataNodeSlice *ss;
			bool found;

			slice = get_slice_for_dimension(chunk, partitioning_dimension_id);

			Assert(NULL != slice);

			/* Get or create a new entry in the global slice set */
			ss = hash_search(all_data_node_slice_htab, &slice->fd.id, HASH_ENTER, &found);

			if (!found)
			{
				ss->sliceid = slice->fd.id;
				ss->node_serverid = sca->node_server_oid;
				data_node_slices = lappend(data_node_slices, ts_dimension_slice_copy(slice));
			}

			/* First detect "same-slice overlap", and then do a more expensive
			 * range overlap check */
			if (ss->node_serverid != sca->node_server_oid ||
				/* Check if the slice overlaps with the accumulated slices of
				 * other data nodes. This can be made more efficient by using an
				 * interval tree. */
				dimension_slice_overlaps_with_others(slice, all_data_node_slices))
			{
				/* The same slice exists on (at least) two data nodes, or it
				 * overlaps with a different slice on another data node */
				hash_seq_term(&status);
				hash_destroy(all_data_node_slice_htab);
				return true;
			}
		}

		/* Add the data node's slice set to the set of all data nodes checked so
		 * far */
		all_data_node_slices = list_concat(all_data_node_slices, data_node_slices);
	}

	hash_destroy(all_data_node_slice_htab);

	return false;
}
