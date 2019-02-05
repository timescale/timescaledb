/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TELEMETRY_UUID_H
#define TIMESCALEDB_TELEMETRY_UUID_H

#include <postgres.h>
#include <utils/uuid.h>

extern pg_uuid_t *ts_uuid_create(void);

#endif /* TIMESCALEDB_TELEMETRY_UUID_H */
