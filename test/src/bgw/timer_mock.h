/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIME_MOCK_H
#define TIME_MOCK_H

#include <postgres.h>
#include <postmaster/bgworker.h>

#include "bgw/timer.h"

extern void timer_mock_register_bgw_handle(BackgroundWorkerHandle *handle);

const Timer mock_timer;

#endif							/* tTIME_MOCK_H */
