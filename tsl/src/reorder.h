/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_REORDER_H
#define TIMESCALEDB_TSL_REORDER_H

#include <postgres.h>

extern Datum tsl_reorder_chunk(PG_FUNCTION_ARGS);
extern Datum tsl_move_chunk(PG_FUNCTION_ARGS);
extern void reorder_chunk(Oid chunk_id, Oid index_id, bool verbose, Oid wait_id,
						  Oid destination_tablespace, Oid index_tablespace);

#endif /* TIMESCALEDB_TSL_REORDER_H */
