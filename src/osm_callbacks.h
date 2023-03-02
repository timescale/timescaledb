/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_OSM_CALLBACKS_H
#define TIMESCALEDB_OSM_CALLBACKS_H

#include <postgres.h>
#include <catalog/objectaddress.h>

typedef int (*chunk_insert_check_hook_type)(Oid ht_oid, int64 range_start, int64 range_end);
typedef void (*hypertable_drop_hook_type)(const char *schema_name, const char *table_name);

/*
 * Object Storage Manager callbacks.
 *
 * chunk_insert_check_hook - checks whether the specified range is managed by OSM
 * hypertable_drop_hook - used for OSM catalog cleanups
 */
typedef struct
{
	chunk_insert_check_hook_type chunk_insert_check_hook;
	hypertable_drop_hook_type hypertable_drop_hook;
} OsmCallbacks;

extern OsmCallbacks *ts_get_osm_callbacks(void);

#endif /* TIMESCALEDB_OSM_CALLBACKS_H */
