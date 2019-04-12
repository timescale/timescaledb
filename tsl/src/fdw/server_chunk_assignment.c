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

#include "server_chunk_assignment.h"
#include "chunk.h"
#include "chunk_server.h"
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

	if (!bms_is_member(chunkrel->relid, sca->chunk_relids))
	{
		RangeTblEntry *rte = planner_rt_fetch(chunkrel->relid, scas->root);
		Path *path = chunkrel->cheapest_total_path;
		MemoryContext old = MemoryContextSwitchTo(scas->mctx);

		sca->chunk_relids = bms_add_member(sca->chunk_relids, chunkrel->relid);
		sca->chunk_oids = lappend_oid(sca->chunk_oids, rte->relid);
		sca->remote_chunk_ids =
			lappend_int(sca->remote_chunk_ids,
						get_remote_chunk_id_from_relid(chunkrel->serverid, rte->relid));
		sca->rows += chunkrel->rows;

		if (path != NULL)
		{
			if (sca->startup_cost == 0.0)
			{
				sca->startup_cost = path->startup_cost;
				sca->total_cost = path->startup_cost;
			}
			sca->total_cost += (path->total_cost - path->startup_cost);
		}

		MemoryContextSwitchTo(old);
	}

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
