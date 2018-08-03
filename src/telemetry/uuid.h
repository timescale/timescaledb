#ifndef TIMESCALEDB_TELEMETRY_UUID_H
#define TIMESCALEDB_TELEMETRY_UUID_H
#include <utils/uuid.h>

pg_uuid_t	   *get_uuid(void);
pg_uuid_t	   *get_exported_uuid(void);
const char	   *get_install_timestamp(void);
#endif							/* TIMESCALEDB_TELEMETRY_UUID_H */
