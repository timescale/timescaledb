/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_BGW_LAUNCHER_INTERFACE_H
#define TIMESCALEDB_BGW_LAUNCHER_INTERFACE_H

#include <postgres.h>

extern bool ts_bgw_worker_reserve(void);
extern void ts_bgw_worker_release(void);
extern int	ts_bgw_num_unreserved(void);
extern int	ts_bgw_loader_api_version(void);
extern void ts_bgw_check_loader_api_version(void);
#endif							/* TIMESCALEDB_BGW_LAUNCHER_INTERFACE_H */
