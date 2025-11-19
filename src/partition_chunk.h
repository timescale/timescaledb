/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "export.h"
#include "guc.h"

#define is_partitioning_allowed(relid)                                                             \
	(ts_guc_enable_partitioned_hypertables && (get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE))

/*
 * Cache entry for chunks to be attached as partitions
 */
typedef struct PartChunkCacheEntry
{
	Oid ht_relid;
	List *chunk_oids;
} PartChunkCacheEntry;
extern void ts_partition_cache_insert_chunk(const Hypertable *ht, Oid chunk_relid);
extern PartChunkCacheEntry *ts_partition_cache_get_by_hypertable(Oid ht_relid);
extern void ts_partition_cache_destroy(void);

extern void ts_partition_chunk_prepare_attributes(Oid ht_relid, List **attlist, List **constraints);
