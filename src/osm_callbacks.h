/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <catalog/objectaddress.h>
#include <nodes/plannodes.h>

/* range_start and range_end are in PG internal timestamp format. */
typedef int (*chunk_insert_check_hook_type)(Oid ht_oid, int64 range_start, int64 range_end);
typedef void (*hypertable_drop_hook_type)(const char *schema_name, const char *table_name);
typedef List *(*hypertable_drop_chunks_hook_type)(Oid osm_chunk_oid,
												  const char *hypertable_schema_name,
												  const char *hypertable_name, int64 range_start,
												  int64 range_end);
typedef int (*chunk_startup_exclusion_hook_type)(const char *hypertable_schema_name,
												 const char *hypertable_name, Oid relid,
												 ForeignScan *scan, List *constified_restrictinfos,
												 int32 varno); // scan->plan->fdw_private

/*
 * Object Storage Manager callbacks.
 *
 * chunk_insert_check_hook - checks whether the specified range is managed by OSM
 * hypertable_drop_hook - used for OSM catalog cleanups
 */
/* This struct is retained for backward compatibility. We'll remove this in one
 * of the upcoming releases
 */
typedef struct
{
	chunk_insert_check_hook_type chunk_insert_check_hook;
	hypertable_drop_hook_type hypertable_drop_hook;
} OsmCallbacks;

typedef struct
{
	int64 version_num;
	chunk_insert_check_hook_type chunk_insert_check_hook;
	hypertable_drop_hook_type hypertable_drop_hook;
	hypertable_drop_chunks_hook_type hypertable_drop_chunks_hook;
	chunk_startup_exclusion_hook_type chunk_startup_exclusion_hook;
} OsmCallbacks_Versioned;

extern chunk_insert_check_hook_type ts_get_osm_chunk_insert_hook(void);
extern hypertable_drop_hook_type ts_get_osm_hypertable_drop_hook(void);
extern hypertable_drop_chunks_hook_type ts_get_osm_hypertable_drop_chunks_hook(void);
extern chunk_startup_exclusion_hook_type ts_get_osm_chunk_startup_exclusion_hook(void);
