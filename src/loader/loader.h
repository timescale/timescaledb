/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_LOADER_H
#define TIMESCALEDB_LOADER_H

#include <postgres.h>

extern char *ts_loader_extension_version(void);

extern bool ts_loader_extension_exists(void);

extern void ts_loader_extension_check(void);

#endif /* TIMESCALEDB_LOADER_H */
