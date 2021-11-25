/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TELEMETRY_TELEMETRY_METADATA_H
#define TIMESCALEDB_TELEMETRY_TELEMETRY_METADATA_H

#include <postgres.h>
#include <utils/jsonb.h>

#include <export.h>

extern void ts_telemetry_metadata_add_values(JsonbParseState *state);

#endif /* TIMESCALEDB_TELEMETRY_TELEMETRY_METADATA_H */
