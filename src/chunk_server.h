/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CHUNK_SERVER_H
#define TIMESCALEDB_CHUNK_SERVER_H

#include "catalog.h"
#include "export.h"

typedef struct ChunkServer
{
	FormData_chunk_server fd;
	Oid foreign_server_oid;
} ChunkServer;

extern List *ts_chunk_server_scan_by_chunk_id(int32 chunk_id, MemoryContext mctx);
extern TSDLLEXPORT ChunkServer *
ts_chunk_server_scan_by_chunk_id_and_servername(int32 chunk_id, const char *servername,
												MemoryContext mctx);
extern TSDLLEXPORT void ts_chunk_server_insert(ChunkServer *server);
extern void ts_chunk_server_insert_multi(List *chunk_servers);
extern int ts_chunk_server_delete_by_chunk_id(int32 chunk_id);
extern int ts_chunk_server_delete_by_servername(const char *servername);

#endif /* TIMESCALEDB_CHUNK_SERVER_H */
