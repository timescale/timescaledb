/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

#define TS_BGW_TYPE_LAUNCHER "TimescaleDB Background Worker Launcher"
#define TS_BGW_TYPE_SCHEDULER "TimescaleDB Background Worker Scheduler"

extern int ts_guc_bgw_scheduler_restart_time_sec;

extern void ts_bgw_cluster_launcher_init(void);

/*called by postmaster at launcher bgw startup*/
TSDLLEXPORT extern Datum ts_bgw_cluster_launcher_main(PG_FUNCTION_ARGS);
TSDLLEXPORT extern Datum ts_bgw_db_scheduler_entrypoint(PG_FUNCTION_ARGS);
