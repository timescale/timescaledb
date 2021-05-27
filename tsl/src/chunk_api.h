/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CHUNK_API_H
#define TIMESCALEDB_TSL_CHUNK_API_H

#include <postgres.h>

#include <chunk.h>

#include "hypertable_data_node.h"

extern Datum chunk_show(PG_FUNCTION_ARGS);
extern Datum chunk_create(PG_FUNCTION_ARGS);
extern void chunk_api_create_on_data_nodes(const Chunk *chunk, const Hypertable *ht,
										   const char *remote_chunk_name, List *data_nodes);
extern Datum chunk_api_get_chunk_relstats(PG_FUNCTION_ARGS);
extern Datum chunk_api_get_chunk_colstats(PG_FUNCTION_ARGS);
extern void chunk_api_update_distributed_hypertable_stats(Oid relid);
extern Datum chunk_create_empty_table(PG_FUNCTION_ARGS);
extern void chunk_api_call_create_empty_chunk_table_wrapper(ChunkCopyData *ccd, bool transactional);
extern void chunk_api_call_chunk_attach_replica_wrapper(ForeignServer *server, ChunkCopyData *ccd,
														bool transactional);
void chunk_api_call_chunk_drop_replica_wrapper(ChunkCopyData *ccd, Oid serverid,
											   bool transactional);
extern void chunk_api_call_create_empty_chunk_table(const Hypertable *ht, const Chunk *chunk,
													const char *node_name);
#endif /* TIMESCALEDB_TSL_CHUNK_API_H */
