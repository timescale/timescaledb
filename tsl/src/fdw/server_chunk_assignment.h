/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_SERVER_CHUNK_ASSIGNMENT
#define TIMESCALEDB_TSL_SERVER_CHUNK_ASSIGNMENT
#include <postgres.h>
#include <nodes/pg_list.h>

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
	List *chunk_oids;
	List *remote_chunk_ids;
} ServerChunkAssignment;

/*
 * Use the attached foreign server on a chunk to decide which server is used for a chunk.
 * Returns a list of ServerChunkAssignments.
 */
extern List *server_chunk_assignment_by_using_attached_server(List *chunk_oids);

#endif /* TIMESCALEDB_TSL_SERVER_CHUNK_ASSIGNMENT */
