#ifndef TIMESCALEDB_LOADER_H
#define TIMESCALEDB_LOADER_H

#include <postgres.h>

extern char *loader_extension_version(void);

extern bool loader_extension_exists(void);

extern void loader_extension_check(void);

#endif							/* TIMESCALDB_LOADER_H */
