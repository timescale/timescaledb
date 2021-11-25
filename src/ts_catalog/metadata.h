/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_METADATA_H
#define TIMESCALEDB_METADATA_H

#include <postgres.h>
#include "export.h"

#define METADATA_UUID_KEY_NAME "uuid"
#define METADATA_EXPORTED_UUID_KEY_NAME "exported_uuid"
#define METADATA_TIMESTAMP_KEY_NAME "install_timestamp"

/*
 * When added to a distributed database, this key in the metadata table will be set
 * to match the uuid (from ts_metadata_get_uuid()) of the access node.
 *
 * Therefore we can check if a database is the access node or not simply by
 * comparing the results of dist_util_get_id() and ts_metadata_get_uuid().
 */
#define METADATA_DISTRIBUTED_UUID_KEY_NAME "dist_uuid"

extern TSDLLEXPORT Datum ts_metadata_get_value(const char *metadata_key, Oid value_type,
											   bool *isnull);
extern TSDLLEXPORT Datum ts_metadata_insert(const char *metadata_key, Datum metadata_value,
											Oid value_type, bool include_in_telemetry);
extern TSDLLEXPORT void ts_metadata_drop(const char *metadata_key);
extern TSDLLEXPORT Datum ts_metadata_get_uuid(void);
extern Datum ts_metadata_get_exported_uuid(void);
extern Datum ts_metadata_get_install_timestamp(void);

#endif /* TIMESCALEDB_METADATA_H */
