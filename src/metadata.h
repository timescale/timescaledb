/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_METADATA_H
#define TIMESCALEDB_METADATA_H

#include <postgres.h>
#include "export.h"

extern TSDLLEXPORT Datum ts_metadata_get_value(const char *metadata_key, Oid value_type,
											   bool *isnull);
extern TSDLLEXPORT Datum ts_metadata_insert(const char *metadata_key, Datum metadata_value,
											Oid value_type, bool include_in_telemetry);
extern TSDLLEXPORT void ts_metadata_drop(const char *metadata_key);

#endif /* TIMESCALEDB_METADATA_H */
