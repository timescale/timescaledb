/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

extern void ts_bgw_cluster_launcher_register(void);

/*called by postmaster at launcher bgw startup*/
TSDLLEXPORT extern Datum ts_bgw_cluster_launcher_main(PG_FUNCTION_ARGS);
TSDLLEXPORT extern Datum ts_bgw_db_scheduler_entrypoint(PG_FUNCTION_ARGS);
