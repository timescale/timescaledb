#ifndef TIMESCALEDB_TELEMETRY_METADATA_H
#define TIMESCALEDB_TELEMETRY_METADATA_H

#include <postgres.h>

extern Datum metadata_get_uuid(void);
extern Datum metadata_get_exported_uuid(void);
extern Datum metadata_get_install_timestamp(void);

#endif							/* TIMESCALEDB_TELEMETRY_METADATA_H */
