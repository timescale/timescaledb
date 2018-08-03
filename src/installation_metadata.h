#ifndef TIMESCALEDB_INSTALLATION_METADATA_H
#define TIMESCALEDB_INSTALLATION_METADATA_H

const char	   *installation_metadata_get_value(const char *metadata_key);
const char	   *installation_metadata_insert(const char *metadata_key, const char *metadata_value);

#endif							/* TIMESCALEDB_INSTALLATION_METADATA_H */
