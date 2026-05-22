/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

typedef struct Chunk Chunk;
typedef struct Hypertable Hypertable;

/*
 * Create all extended statistics on a chunk, given the extended statistics
 * that exist on the chunk's hypertable.
 *
 * Similar to ts_chunk_index_create_all() for indexes.
 */
extern void ts_chunk_extended_statistics_create_all(int32 hypertable_id, Oid hypertable_relid,
													int32 chunk_id, Oid chunk_relid);

/*
 * Create a specific extended statistics on a chunk by name.
 *
 * Used for DDL propagation (CREATE STATISTICS on hypertable).
 */
extern void ts_chunk_extended_statistics_create_from_stat(Oid hypertable_relid, Oid chunk_relid,
														  const char *stat_ext_name);

/*
 * Find a chunk extended statistics that matches the structure of a hypertable
 * extended statistics.
 *
 * This function searches through all extended statistics on the chunk and returns
 * the OID of the first statistics that has the same structure as the hypertable
 * extended statistics.
 *
 * Returns InvalidOid if no matching extended statistics is found.
 */
extern Oid ts_chunk_extended_statistics_get_by_hypertable_relid(Oid chunk_relid,
																Oid hypertable_stat_ext_oid);

/*
 * Compare two extended statistics to determine if they have the same structure.
 *
 * Two extended statistics are considered structurally identical if they have:
 * - Same stxkeys (column numbers)
 * - Same stxkind (statistics types: d, f, m, e)
 * - Same stxexprs (expressions, if any)
 *
 * Returns true if the extended statistics have identical structure, false otherwise.
 */
extern bool ts_extended_statistics_compare(Oid stat_ext_oid1, Oid stat_ext_oid2);

/*
 * Get the relation OID that an extended statistics object belongs to.
 *
 * Given an extended statistics OID, returns the OID of the table that the
 * extended statistics object is defined on (stxrelid from pg_statistic_ext).
 *
 * Returns InvalidOid if the extended statistics object doesn't exist.
 */
extern Oid ts_get_relation_from_extended_statistics(Oid stat_ext_oid);

/*
 * Rename chunk extended statistics to match the new hypertable statistics name.
 *
 * Similar to ts_chunk_index_rename() for indexes.
 */
extern void ts_chunk_extended_statistics_rename(Hypertable *ht, Oid hypertable_stat_oid,
												const char *new_name);
