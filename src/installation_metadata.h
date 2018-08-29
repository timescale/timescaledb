#ifndef TIMESCALEDB_INSTALLATION_METADATA_H
#define TIMESCALEDB_INSTALLATION_METADATA_H

#include <postgres.h>

extern Datum installation_metadata_get_value(Datum metadata_key, Oid key_type, Oid value_type, bool *isnull);
extern Datum installation_metadata_insert(Datum metadata_key, Oid key_type, Datum metadata_value, Oid value_type);

#endif							/* TIMESCALEDB_INSTALLATION_METADATA_H */
