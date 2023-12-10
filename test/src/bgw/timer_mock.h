/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <postmaster/bgworker.h>

#include "bgw/timer.h"

extern void ts_timer_mock_register_bgw_handle(BackgroundWorkerHandle *handle,
											  MemoryContext scheduler_mctx);

extern const Timer ts_mock_timer;
