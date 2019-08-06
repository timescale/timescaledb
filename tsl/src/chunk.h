/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CHUNK_H
#define TIMESCALEDB_TSL_CHUNK_H

#include <postgres.h>

#include <chunk.h>

extern void chunk_update_foreign_server_if_needed(int32 chunk_id, Oid existing_server_id);
extern Datum chunk_set_default_data_node(PG_FUNCTION_ARGS);
extern void chunk_drop_remote_chunks(Name table_name, Name schema_name, Datum older_than_datum,
									 Datum newer_than_datum, Oid older_than_type,
									 Oid newer_than_type, bool cascade,
									 bool cascades_to_materializations, bool verbose,
									 List *data_node_oids);

#endif /* TIMESCALEDB_TSL_CHUNK_H */
