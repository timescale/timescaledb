/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <foreign/foreign.h>
#include <utils/hsearch.h>
#include <parser/parsetree.h>
#include <nodes/relation.h>
#include <nodes/bitmapset.h>

#include "data_node_chunk_assignment.h"
#include "dimension.h"
#include "dimension_slice.h"
#include "dimension_vector.h"
#include "hypercube.h"
#include "chunk.h"
#include "chunk_data_node.h"
#include "utils/fmgrprotos.h"

#define DEFAULT_NUM_SERVERS 10

static int
get_remote_chunk_id_from_relid(Oid server_oid, Oid chunk_relid)
{
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, 0, false);
	ForeignServer *fs = GetForeignServer(server_oid);
	ChunkServer *cs = ts_chunk_server_scan_by_chunk_id_and_servername(chunk->fd.id,
																	  fs->servername,
																	  CurrentMemoryContext);
	return cs->fd.server_chunk_id;
}

/*
 * Find an existing server chunk assignment or initialize a new one.
 */
static ServerChunkAssignment *
get_or_create_sca(ServerChunkAssignments *scas, Oid serverid, RelOptInfo *rel)
{
	ServerChunkAssignment *sca;
	bool found;

	Assert(rel == NULL || rel->serverid == serverid);

	sca = hash_search(scas->assignments, &serverid, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry */
		memset(sca, 0, sizeof(*sca));
		sca->server_oid = serverid;
	}

	return sca;
}

#define DEFAULT_NUM_DIMENSIONS 2

static DimensionSlice *
get_slice_for_dimension(Oid chunk_relid, int32 dimension_id)
{
	Chunk *chunk = ts_chunk_get_by_relid(chunk_relid, DEFAULT_NUM_DIMENSIONS, true);

	return ts_hypercube_get_slice_by_dimension_id(chunk->cube, dimension_id);
}

/*
 * Assign the given chunk relation to a server.
 *
 * The chunk is assigned according to the strategy set in the
 * ServerChunkAssignments state.
 */
ServerChunkAssignment *
server_chunk_assignment_assign_chunk(ServerChunkAssignments *scas, RelOptInfo *chunkrel)
{
	ServerChunkAssignment *sca = get_or_create_sca(scas, chunkrel->serverid, NULL);
	RangeTblEntry *rte = planner_rt_fetch(chunkrel->relid, scas->root);
	MemoryContext old;

	/* Should never assign the same chunk twice */
	Assert(!bms_is_member(chunkrel->relid, sca->chunk_relids));

	old = MemoryContextSwitchTo(scas->mctx);

	/* If this is the first chunk we assign to this server, increment the
	 * number of servers with one or more chunks on them */
	if (list_length(sca->chunk_oids) == 0)
		scas->num_servers_with_chunks++;

	sca->chunk_relids = bms_add_member(sca->chunk_relids, chunkrel->relid);
	sca->chunk_oids = lappend_oid(sca->chunk_oids, rte->relid);
	sca->remote_chunk_ids =
		lappend_int(sca->remote_chunk_ids,
					get_remote_chunk_id_from_relid(chunkrel->serverid, rte->relid));
	sca->rows += chunkrel->rows;
	sca->tuples += chunkrel->tuples;

	MemoryContextSwitchTo(old);

	scas->total_num_chunks++;

	return sca;
}

/*
 * Initialize a new chunk assignment state with a specific assignment strategy.
 */
void
server_chunk_assignments_init(ServerChunkAssignments *scas, ServerChunkAssignmentStrategy strategy,
							  PlannerInfo *root, unsigned int nrels_hint)
{
	HASHCTL hctl = {
		.keysize = sizeof(Oid),
		.entrysize = sizeof(ServerChunkAssignment),
		.hcxt = CurrentMemoryContext,
	};

	scas->strategy = strategy;
	scas->root = root;
	scas->mctx = hctl.hcxt;
	scas->total_num_chunks = 0;
	scas->num_servers_with_chunks = 0;
	scas->assignments = hash_create("server chunk assignments",
									nrels_hint,
									&hctl,
									HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);
}

/*
 * Assign chunks to servers.
 *
 * Each chunk in the chunkrels array is a assigned a server using the strategy
 * set in the ServerChunkAssignments state.
 */
ServerChunkAssignments *
server_chunk_assignment_assign_chunks(ServerChunkAssignments *scas, RelOptInfo **chunkrels,
									  unsigned int nrels)
{
	unsigned int i;

	Assert(scas->assignments != NULL && scas->root != NULL);

	for (i = 0; i < nrels; i++)
	{
		RelOptInfo *chunkrel = chunkrels[i];

		Assert(IS_SIMPLE_REL(chunkrel) && chunkrel->fdw_private != NULL);
		server_chunk_assignment_assign_chunk(scas, chunkrel);
	}

	return scas;
}

/*
 * Get the server assignment for the given relation (chunk).
 */
ServerChunkAssignment *
server_chunk_assignment_get_or_create(ServerChunkAssignments *scas, RelOptInfo *rel)
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
dimension_slice_overlaps_with_others(DimensionSlice *slice, List *other_slices)
{
	ListCell *lc;

	foreach (lc, other_slices)
	{
		DimensionSlice *other_slice = lfirst(lc);

		if (ts_dimension_slices_collide(slice, other_slice))
			return true;
	}

	return false;
}

/*
 * ServerSlice: a hash table entry to track the server a chunk slice is placed
 * on.
 */
typedef struct ServerSlice
{
	int32 sliceid;
	Oid serverid;
} ServerSlice;

/*
 * Check whether chunks are assigned in an overlapping way.
 *
 * Assignments are overlapping if any server has a chunk that overlaps (in the
 * given paritioning dimension) with a chunk on another server. There are two
 * cases when this can happen:
 *
 * 1. The same slice exists on multiple servers (we optimize for detecting
 * this).
 *
 * 2. Two different slices overlap while existing on different servers (this
 * case is more costly to detect).
 */
bool
server_chunk_assignments_are_overlapping(ServerChunkAssignments *scas,
										 int32 partitioning_dimension_id)
{
	HASH_SEQ_STATUS status;
	HASHCTL hashctl = {
		.keysize = sizeof(int32),
		.entrysize = sizeof(ServerSlice),
		.hcxt = CurrentMemoryContext,
	};
	HTAB *all_server_slice_htab;
	ServerChunkAssignment *sca;
	List *all_server_slices = NIL;

	/* No overlapping can occur if there are chunks on only one server (this
	 * covers also the case of a single chunk) */
	if (scas->num_servers_with_chunks <= 1)
		return false;

	/* If there are multiple servers with chunks and they are not placed along
	 * a closed "space" dimension, we assume overlapping */
	if (partitioning_dimension_id <= 0)
		return true;

	/* Use a hash table to track slice server mappings by slice ID. The same
	 * slice can exist on multiple servers, causing an overlap across servers
	 * in the slice dimension. This hash table is used to quickly detect such
	 * "same-slice overlaps" and avoids having to do a more expensive range
	 * overlap check.
	 */
	all_server_slice_htab =
		hash_create("all_server_slices", scas->total_num_chunks, &hashctl, HASH_ELEM | HASH_BLOBS);

	hash_seq_init(&status, scas->assignments);

	while ((sca = hash_seq_search(&status)))
	{
		List *server_slices = NIL;
		ListCell *lc;

		/* Check each slice on the server against the slices on other
		 * servers */
		foreach (lc, sca->chunk_oids)
		{
			Oid chunk_oid = lfirst_oid(lc);
			DimensionSlice *slice;
			ServerSlice *ss;
			bool found;

			slice = get_slice_for_dimension(chunk_oid, partitioning_dimension_id);

			Assert(NULL != slice);

			/* Get or create a new entry in the global slice set */
			ss = hash_search(all_server_slice_htab, &slice->fd.id, HASH_ENTER, &found);

			if (!found)
			{
				ss->sliceid = slice->fd.id;
				ss->serverid = sca->server_oid;
				server_slices = lappend(server_slices, slice);
			}

			/* First detect "same-slice overlap", and then do a more expensive
			 * range overlap check */
			if (ss->serverid != sca->server_oid ||
				/* Check if the slice overlaps with the accumulated slices of
				 * other servers. This can be made more efficient by using an
				 * interval tree. */
				dimension_slice_overlaps_with_others(slice, all_server_slices))
			{
				/* The same slice exists on (at least) two servers, or it
				 * overlaps with a different slice on another server */
				hash_seq_term(&status);
				hash_destroy(all_server_slice_htab);
				return true;
			}
		}

		/* Add the server's slice set to the set of all servers checked so
		 * far */
		all_server_slices = list_concat(all_server_slices, server_slices);
	}

	hash_destroy(all_server_slice_htab);

	return false;
}
