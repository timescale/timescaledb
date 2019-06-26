/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef BGW_TIMER_H
#define BGW_TIMER_H

#include <postgres.h>
#include <utils/timestamp.h>

#include "config.h"
#include "export.h"

typedef struct Timer
{
	TimestampTz (*get_current_timestamp)();
	bool (*wait)(TimestampTz until);

} Timer;

extern bool ts_timer_wait(TimestampTz until);
extern TSDLLEXPORT TimestampTz ts_timer_get_current_timestamp(void);

#ifdef TS_DEBUG
extern void ts_timer_set(const Timer *timer);
extern const Timer *ts_get_standard_timer(void);
#endif

#endif /* BGW_TIMER_H */
