/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_DATA_NODE_CHUNK_ASSIGNMENT
#define TIMESCALEDB_TSL_DATA_NODE_CHUNK_ASSIGNMENT

#include <postgres.h>
#include <nodes/pathnodes.h>
#include <nodes/pg_list.h>
#include <storage/block.h>
#include <utils/hsearch.h>

/*
 * data node-chunk assignments map chunks to the data nodes that will be responsible
 * for handling those chunks. For replicated chunks several such strategies
 * are possible. For example, the system can aim to use as many data nodes as
 * possible to increase parallelism or as few as possible to decrease coordination
 * overhead.
 */

typedef struct DataNodeChunkAssignment
{
	Oid node_server_oid;
	BlockNumber pages;
	double rows;
	double tuples;
	Cost startup_cost;
	Cost total_cost;
	Relids chunk_relids;
	List *chunks;
	List *remote_chunk_ids;
} DataNodeChunkAssignment;

/*
 * Only "attached data node" strategy is supported at this time. This strategy
 * picks the data node that is associated with a chunk's foreign table
 */
typedef enum DataNodeChunkAssignmentStrategy
{
	SCA_STRATEGY_ATTACHED_DATA_NODE,
} DataNodeChunkAssignmentStrategy;

typedef struct DataNodeChunkAssignments
{
	DataNodeChunkAssignmentStrategy strategy;
	PlannerInfo *root;
	HTAB *assignments;
	unsigned long total_num_chunks;
	unsigned long num_nodes_with_chunks;
	MemoryContext mctx;
} DataNodeChunkAssignments;

extern DataNodeChunkAssignment *
data_node_chunk_assignment_assign_chunk(DataNodeChunkAssignments *scas, RelOptInfo *chunkrel);

extern DataNodeChunkAssignments *
data_node_chunk_assignment_assign_chunks(DataNodeChunkAssignments *scas, RelOptInfo **chunkrels,
										 unsigned int nrels);

extern DataNodeChunkAssignment *
data_node_chunk_assignment_get_or_create(DataNodeChunkAssignments *scas, RelOptInfo *rel);

extern void data_node_chunk_assignments_init(DataNodeChunkAssignments *scas,
											 DataNodeChunkAssignmentStrategy strategy,
											 PlannerInfo *root, unsigned int nrels_hint);

extern bool data_node_chunk_assignments_are_overlapping(DataNodeChunkAssignments *scas,
														int32 partitioning_dimension_id);

#endif /* TIMESCALEDB_TSL_DATA_NODE_CHUNK_ASSIGNMENT */
