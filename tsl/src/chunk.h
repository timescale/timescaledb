/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CHUNK_H
#define TIMESCALEDB_TSL_CHUNK_H

#include <postgres.h>
#include <fmgr.h>
#include <chunk.h>

extern bool chunk_update_foreign_server_if_needed(const Chunk *chunk, Oid data_node_id,
												  bool available);
extern Datum chunk_set_default_data_node(PG_FUNCTION_ARGS);
extern Datum chunk_drop_replica(PG_FUNCTION_ARGS);
extern Datum chunk_freeze_chunk(PG_FUNCTION_ARGS);
extern Datum chunk_unfreeze_chunk(PG_FUNCTION_ARGS);
extern Datum chunk_drop_stale_chunks(PG_FUNCTION_ARGS);
extern void ts_chunk_drop_stale_chunks(const char *node_name, ArrayType *chunks_array);
extern int chunk_invoke_drop_chunks(Oid relid, Datum older_than, Datum older_than_type);
extern Datum chunk_create_replica_table(PG_FUNCTION_ARGS);
extern void chunk_update_stale_metadata(Chunk *new_chunk, List *chunk_data_nodes);

#endif /* TIMESCALEDB_TSL_CHUNK_H */
