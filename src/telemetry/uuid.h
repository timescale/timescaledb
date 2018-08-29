#ifndef TIMESCALEDB_TELEMETRY_UUID_H
#define TIMESCALEDB_TELEMETRY_UUID_H

#include <postgres.h>
#include <utils/uuid.h>

extern pg_uuid_t *uuid_create(void);

#endif							/* TIMESCALEDB_TELEMETRY_UUID_H */
