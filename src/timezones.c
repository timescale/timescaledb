/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <port.h>
#include <pgtime.h>
#include <datatype/timestamp.h>
#include <utils/timestamp.h>
#include <access/xact.h>
#include "timezones.h"

/* Checks if the given TZ name is valid. */
bool
ts_is_valid_timezone_name(const char *tz_name)
{
	pg_tz *tz;
	int tzoff;
	struct pg_tm tm;
	fsec_t fsec;
	const char *abbrev;
	bool found = false;
	TimestampTz now = GetCurrentTransactionStartTimestamp();
	pg_tzenum *tzenum = pg_tzenumerate_start();

	while (true)
	{
		tz = pg_tzenumerate_next(tzenum);
		if (!tz)
			break;

		/*
		 * Convert now() to time in this TZ and skip if conversion fails.
		 * This check is the same that pg_timezone_names() does.
		 */
		if (timestamp2tm(now, &tzoff, &tm, &fsec, &abbrev, tz) != 0)
			continue;

		if ((!strcmp(tz_name, pg_get_timezone_name(tz))) || (abbrev && !strcmp(tz_name, abbrev)))
		{
			found = true;
			break;
		}
	}

	pg_tzenumerate_end(tzenum);
	return found;
}
