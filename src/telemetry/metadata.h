/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TELEMETRY_METADATA_H
#define TIMESCALEDB_TELEMETRY_METADATA_H

#include <postgres.h>

extern Datum ts_metadata_get_uuid(void);
extern Datum ts_metadata_get_exported_uuid(void);
extern Datum ts_metadata_get_install_timestamp(void);

#endif /* TIMESCALEDB_TELEMETRY_METADATA_H */
