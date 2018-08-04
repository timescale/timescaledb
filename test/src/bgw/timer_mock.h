#ifndef TIME_MOCK_H
#define TIME_MOCK_H

#include <postgres.h>
#include <postmaster/bgworker.h>

#include "bgw/timer.h"

extern void timer_mock_register_bgw_handle(BackgroundWorkerHandle *handle);

const Timer mock_timer;

#endif							/* tTIME_MOCK_H */
