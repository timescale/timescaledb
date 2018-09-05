#ifndef BGW_TIMER_H
#define BGW_TIMER_H

#include <postgres.h>
#include <utils/timestamp.h>

#include "config.h"

typedef struct Timer
{
	TimestampTz (*get_current_timestamp) ();
	bool		(*wait) (TimestampTz until);

} Timer;

extern bool timer_wait(TimestampTz until);
extern TimestampTz timer_get_current_timestamp(void);

#ifdef TS_DEBUG
extern void timer_set(const Timer *timer);
#endif

#endif							/* BGW_TIMER_H */
