/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_SERVER_CHUNK_ASSIGNMENT
#define TIMESCALEDB_TSL_SERVER_CHUNK_ASSIGNMENT

#include <postgres.h>
#include <nodes/pg_list.h>
#include <nodes/relation.h>

/*
 * server-chunk assignments map chunks to the servers that will be responsible
 * for handling those chunks. For replicated chunks several such strategies
 * are possible. For example, the system can aim to use as many servers as
 * possible to increase parallelism or as few as possible to decrease coordination
 * overhead.
 */

typedef struct ServerChunkAssignment
{
	Oid server_oid;
	double rows;
	Cost startup_cost;
	Cost total_cost;
	Relids chunk_relids;
	List *chunk_oids;
	List *remote_chunk_ids;
} ServerChunkAssignment;

/*
 * Only "attached server" strategy is supported at this time. This strategy
 * picks the server that is associated with a chunk's foreign table
 */
typedef enum ServerChunkAssignmentStrategy
{
	SCA_STRATEGY_ATTACHED_SERVER,
} ServerChunkAssignmentStrategy;

typedef struct ServerChunkAssignments
{
	ServerChunkAssignmentStrategy strategy;
	PlannerInfo *root;
	HTAB *assignments;
	MemoryContext mctx;
} ServerChunkAssignments;

extern ServerChunkAssignment *server_chunk_assignment_assign_chunk(ServerChunkAssignments *scas,
																   RelOptInfo *chunkrel);

extern ServerChunkAssignments *server_chunk_assignment_assign_chunks(ServerChunkAssignments *scas,
																	 RelOptInfo **chunkrels,
																	 unsigned int nrels);

extern ServerChunkAssignment *server_chunk_assignment_get_or_create(ServerChunkAssignments *scas,
																	RelOptInfo *rel);

extern void server_chunk_assignments_init(ServerChunkAssignments *scas,
										  ServerChunkAssignmentStrategy strategy, PlannerInfo *root,
										  unsigned int nrels_hint);

#endif /* TIMESCALEDB_TSL_SERVER_CHUNK_ASSIGNMENT */
