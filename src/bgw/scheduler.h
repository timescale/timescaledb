/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef BGW_SCHEDULER_H
#define BGW_SCHEDULER_H
#include <postgres.h>
#include <fmgr.h>
#include "compat.h"
#include <postmaster/bgworker.h>

#include "timer.h"

/* callback used in testing */
typedef void (*register_background_worker_callback_type) (BackgroundWorkerHandle *);

void		bgw_scheduler_process(int32 run_for_interval_ms, register_background_worker_callback_type bgw_register);

/* exposed for access by mock */
void		bgw_scheduler_setup_callbacks(void);

#endif							/* BGW_SCHEDULER_H */
