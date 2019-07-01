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
extern TSDLLEXPORT int ts_chunk_server_delete_by_chunk_id_and_server_name(int32 chunk_id,
																		  const char *server_name);
extern int ts_chunk_server_delete_by_servername(const char *servername);
extern TSDLLEXPORT List *
ts_chunk_server_scan_by_servername_and_hypertable_id(const char *server_name, int32 hypertable_id,
													 MemoryContext mctx);
extern TSDLLEXPORT bool ts_chunk_server_contains_non_replicated_chunks(List *chunk_servers);

extern TSDLLEXPORT void ts_chunk_server_update_foreign_table_server(Oid relid, Oid new_server_id);
extern TSDLLEXPORT void
ts_chunk_server_update_foreign_table_server_if_needed(int32 chunk_id, Oid existing_server_id);

#endif /* TIMESCALEDB_CHUNK_SERVER_H */
