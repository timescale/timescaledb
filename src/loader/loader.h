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

/* WaitLatch expects a long, so make sure to cast the value */
/* Default value for timescaledb.launcher_poll_time */
#ifdef TS_DEBUG
#define BGW_LAUNCHER_POLL_TIME_MS 10
#else
#define BGW_LAUNCHER_POLL_TIME_MS 60000
#endif

/* GUC to control launcher timeout */
extern int ts_guc_bgw_launcher_poll_time;

#endif /* TIMESCALEDB_LOADER_H */
