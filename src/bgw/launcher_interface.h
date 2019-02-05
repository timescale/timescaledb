/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_BGW_LAUNCHER_INTERFACE_H
#define TIMESCALEDB_BGW_LAUNCHER_INTERFACE_H

#include <postgres.h>

extern bool ts_bgw_worker_reserve(void);
extern void ts_bgw_worker_release(void);
extern int ts_bgw_num_unreserved(void);
extern int ts_bgw_loader_api_version(void);
extern void ts_bgw_check_loader_api_version(void);
#endif /* TIMESCALEDB_BGW_LAUNCHER_INTERFACE_H */
