/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */

#ifndef TIMESCALEDB_BGW_LAUNCHER_H
#define TIMESCALEDB_BGW_LAUNCHER_H

#include <postgres.h>
#include <fmgr.h>

extern void ts_bgw_cluster_launcher_register(void);

/*called by postmaster at launcher bgw startup*/
TSDLLEXPORT extern Datum ts_bgw_cluster_launcher_main(PG_FUNCTION_ARGS);
TSDLLEXPORT extern Datum ts_bgw_db_scheduler_entrypoint(PG_FUNCTION_ARGS);



#endif							/* TIMESCALEDB_BGW_LAUNCHER_H */
