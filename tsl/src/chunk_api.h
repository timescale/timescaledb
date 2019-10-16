/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CHUNK_API_H
#define TIMESCALEDB_TSL_CHUNK_API_H

#include <postgres.h>

extern Datum chunk_show(PG_FUNCTION_ARGS);
extern Datum chunk_create(PG_FUNCTION_ARGS);
extern void chunk_api_create_on_data_nodes(Chunk *chunk, Hypertable *ht);
extern Datum chunk_api_get_chunk_relstats(PG_FUNCTION_ARGS);

#endif /* TIMESCALEDB_TSL_CHUNK_API_H */
