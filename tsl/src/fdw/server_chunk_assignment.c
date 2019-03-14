/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <foreign/foreign.h>
#include <utils/hsearch.h>

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

static List *
server_chunk_assignment_list_add_chunk(List *server_chunk_assignment_list, Oid server_oid,
									   Oid chunk_oid)
{
	ListCell *lc;
	ServerChunkAssignment *sca_match = NULL;
	foreach (lc, server_chunk_assignment_list)
	{
		ServerChunkAssignment *sca = lfirst(lc);
		if (sca->server_oid == server_oid)
		{
			sca_match = sca;
			break;
		}
	}

	if (sca_match == NULL)
	{
		sca_match = palloc0(sizeof(*sca_match));
		sca_match->server_oid = server_oid;
		server_chunk_assignment_list = lappend(server_chunk_assignment_list, sca_match);
	}

	sca_match->chunk_oids = lappend_oid(sca_match->chunk_oids, chunk_oid);
	sca_match->remote_chunk_ids =
		lappend_int(sca_match->remote_chunk_ids,
					get_remote_chunk_id_from_relid(server_oid, chunk_oid));
	return server_chunk_assignment_list;
}

List *
server_chunk_assignment_by_using_attached_server(List *chunk_oids)
{
	ListCell *lc;
	List *server_chunk_assignment_list = NIL;

	foreach (lc, chunk_oids)
	{
		Oid chunk_oid = lfirst_oid(lc);
		ForeignTable *ftable = GetForeignTable(chunk_oid);
		server_chunk_assignment_list =
			server_chunk_assignment_list_add_chunk(server_chunk_assignment_list,
												   ftable->serverid,
												   chunk_oid);
	}
	return server_chunk_assignment_list;
}
