/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TELEMETRY_METADATA_H
#define TIMESCALEDB_TELEMETRY_METADATA_H

#include <postgres.h>

extern Datum ts_telemetry_metadata_get_value(Datum metadata_key, Oid key_type, Oid value_type,
											 bool *isnull);
extern Datum ts_telemetry_metadata_insert(Datum metadata_key, Oid key_type, Datum metadata_value,
										  Oid value_type);

#endif /* TIMESCALEDB_TELEMETRY_METADATA_H */
