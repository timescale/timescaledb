/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CHUNK_API_H
#define TIMESCALEDB_TSL_CHUNK_API_H

#include <postgres.h>

#include <chunk.h>
#include "chunk.h"

#include "ts_catalog/hypertable_data_node.h"

extern Datum chunk_status(PG_FUNCTION_ARGS);
extern Datum chunk_show(PG_FUNCTION_ARGS);
extern Datum chunk_create(PG_FUNCTION_ARGS);
extern void chunk_api_create_on_data_nodes(const Chunk *chunk, const Hypertable *ht,
										   const char *remote_chunk_name, List *data_nodes);
extern Datum chunk_api_get_chunk_relstats(PG_FUNCTION_ARGS);
extern Datum chunk_api_get_chunk_colstats(PG_FUNCTION_ARGS);
extern void chunk_api_update_distributed_hypertable_stats(Oid relid);
extern Datum chunk_create_empty_table(PG_FUNCTION_ARGS);
extern void chunk_api_call_create_empty_chunk_table(const Hypertable *ht, const Chunk *chunk,
													const char *node_name);
extern void chunk_api_call_chunk_drop_replica(const Chunk *chunk, const char *node_name,
											  Oid serverid);

#endif /* TIMESCALEDB_TSL_CHUNK_API_H */
