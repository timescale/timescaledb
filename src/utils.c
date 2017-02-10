/* -*- Mode: C; tab-width: 4; indent-tabs-mode: t; c-basic-offset: 4 -*- */
#include <unistd.h>

#include <postgres.h>
#include <fmgr.h>
#include <utils/datetime.h>

Datum pg_timestamp_to_microseconds(PG_FUNCTION_ARGS);
Datum pg_microseconds_to_timestamp(PG_FUNCTION_ARGS);
Datum pg_timestamp_to_unix_microseconds(PG_FUNCTION_ARGS);
Datum pg_unix_microseconds_to_timestamp(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_timestamp_to_microseconds);

/*
 * Convert a Postgres TIMESTAMP to BIGINT microseconds relative the Postgres epoch.
 */
Datum
pg_timestamp_to_microseconds(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	int64 microseconds;

	if (!IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	microseconds = timestamp;
#else
	if (1)
	{
		int64 seconds = (int64)timestamp;
		microseconds = (seconds * USECS_PER_SEC) + ((timestamp - seconds) * USECS_PER_SEC);
	}
#endif
	PG_RETURN_INT64(microseconds);
}

PG_FUNCTION_INFO_V1(pg_microseconds_to_timestamp);

/*
 * Convert BIGINT microseconds relative the UNIX epoch to a Postgres TIMESTAMP.
 */
Datum
pg_microseconds_to_timestamp(PG_FUNCTION_ARGS)
{
	int64 microseconds = PG_GETARG_INT64(0);
	TimestampTz timestamp;

#ifdef HAVE_INT64_TIMESTAMP
	timestamp = microseconds;
#else
	timestamp = microseconds / USECS_PER_SEC;
#endif

	if (!IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	PG_RETURN_TIMESTAMPTZ(timestamp);
}

PG_FUNCTION_INFO_V1(pg_timestamp_to_unix_microseconds);

/*
 * Convert a Postgres TIMESTAMP to BIGINT microseconds relative the UNIX epoch.
 */
Datum
pg_timestamp_to_unix_microseconds(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	int64 epoch_diff_microseconds = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY;
	int64 microseconds;

	if (timestamp < MIN_TIMESTAMP)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	if (timestamp >= (END_TIMESTAMP - epoch_diff_microseconds))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	microseconds = timestamp + epoch_diff_microseconds;
#else
	if (1)
	{
		int64 seconds = (int64)timestamp;
		microseconds = (seconds * USECS_PER_SEC) + ((timestamp - seconds) * USECS_PER_SEC) + epoch_diff_microseconds;
	}
#endif
	PG_RETURN_INT64(microseconds);
}

PG_FUNCTION_INFO_V1(pg_unix_microseconds_to_timestamp);

/*
 * Convert BIGINT microseconds relative the UNIX epoch to a Postgres TIMESTAMP.
 */
Datum
pg_unix_microseconds_to_timestamp(PG_FUNCTION_ARGS)
{
	int64 microseconds = PG_GETARG_INT64(0);
	TimestampTz timestamp;

	/*
	   Test that the UNIX us timestamp is within bounds.
	   Note that an int64 at UNIX epoch and microsecond precision cannot represent
	   the upper limit of the supported date range (Julian end date), so INT64_MAX
	   is the natural upper bound for this function.
	*/
	if (microseconds < ((int64)USECS_PER_DAY * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	timestamp = microseconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY);
#else
	/* Shift the epoch using integer arithmetic to reduce precision errors */
	timestamp = microseconds / USECS_PER_SEC; /* seconds */
	microseconds = microseconds - ((int64)timestamp * USECS_PER_SEC);
	timestamp = (float8)((int64)seconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY))
				+ (float8)microseconds / USECS_PER_SEC;
#endif
	PG_RETURN_TIMESTAMPTZ(timestamp);
}
