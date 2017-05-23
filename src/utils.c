#include <unistd.h>

#include <postgres.h>
#include <fmgr.h>
#include <utils/datetime.h>
#include <catalog/pg_type.h>
#include <catalog/namespace.h>
#include <utils/guc.h>

#include "utils.h"
#include "nodes/nodes.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"

PG_FUNCTION_INFO_V1(pg_timestamp_to_microseconds);

/*
 * Convert a Postgres TIMESTAMP to BIGINT microseconds relative the Postgres epoch.
 */
Datum
pg_timestamp_to_microseconds(PG_FUNCTION_ARGS)
{
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(0);
	int64		microseconds;

	if (!IS_VALID_TIMESTAMP(timestamp))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	microseconds = timestamp;
#else
	if (1)
	{
		int64		seconds = (int64) timestamp;

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
	int64		microseconds = PG_GETARG_INT64(0);
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
	int64		epoch_diff_microseconds = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY;
	int64		microseconds;

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
		int64		seconds = (int64) timestamp;

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
	int64		microseconds = PG_GETARG_INT64(0);
	TimestampTz timestamp;

	/*
	 * Test that the UNIX us timestamp is within bounds. Note that an int64 at
	 * UNIX epoch and microsecond precision cannot represent the upper limit
	 * of the supported date range (Julian end date), so INT64_MAX is the
	 * natural upper bound for this function.
	 */
	if (microseconds < ((int64) USECS_PER_DAY * (DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE)))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

#ifdef HAVE_INT64_TIMESTAMP
	timestamp = microseconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY);
#else
	/* Shift the epoch using integer arithmetic to reduce precision errors */
	timestamp = microseconds / USECS_PER_SEC;	/* seconds */
	microseconds = microseconds - ((int64) timestamp * USECS_PER_SEC);
	timestamp = (float8) ((int64) seconds - ((POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY))
		+ (float8) microseconds / USECS_PER_SEC;
#endif
	PG_RETURN_TIMESTAMPTZ(timestamp);
}


/*	*/
int64
time_value_to_internal(Datum time_val, Oid type)
{
	if (type == INT8OID)
	{
		return DatumGetInt64(time_val);
	}
	if (type == INT4OID)
	{
		return (int64) DatumGetInt32(time_val);
	}
	if (type == INT2OID)
	{
		return (int64) DatumGetInt16(time_val);
	}
	if (type == TIMESTAMPOID)
	{
		Datum		tz = DirectFunctionCall1(timestamp_timestamptz, time_val);
		Datum		res = DirectFunctionCall1(pg_timestamp_to_unix_microseconds, tz);

		return DatumGetInt64(res);
	}
	if (type == TIMESTAMPTZOID)
	{
		Datum		res = DirectFunctionCall1(pg_timestamp_to_unix_microseconds, time_val);

		return DatumGetInt64(res);
	}

	elog(ERROR, "unkown time type oid '%d'", type);
}

char *
internal_time_to_column_literal_sql(int64 internal_time, Oid type)
{
	char	   *sql = palloc(100 * sizeof(char));

	/* ok to waste a little space */
	if (type == INT8OID || type == INT4OID || type == INT8OID)
	{
		snprintf(sql, 100, "%ld", internal_time);
		return sql;
	}
	/* todo avoid these function calls */
	if (type == TIMESTAMPOID)
	{
		snprintf(sql, 100, "_timescaledb_internal.to_timestamp(%ld)::TIMESTAMP", internal_time);
		return sql;
	}
	if (type == TIMESTAMPTZOID)
	{
		snprintf(sql, 100, "_timescaledb_internal.to_timestamp(%ld)", internal_time);
		return sql;
	}
	elog(ERROR, "unkown time type oid '%d'", type);
}

/* Make a RangeVar from a regclass Oid */
RangeVar *
makeRangeVarFromRelid(Oid relid)
{
	Oid			namespace = get_rel_namespace(relid);
	char	   *tableName = get_rel_name(relid);
	char	   *schemaName = get_namespace_name(namespace);

	return makeRangeVar(schemaName, tableName, -1);
}

int
int_cmp(const void *a, const void *b)
{
	const int  *ia = (const int *) a;
	const int  *ib = (const int *) b;

	return *ia - *ib;
}

FmgrInfo *
create_fmgr(char *schema, char *function_name, int num_args)
{
	FmgrInfo   *finfo = palloc(sizeof(FmgrInfo));
	FuncCandidateList func_list = FuncnameGetCandidates(list_make2(makeString(schema),
												  makeString(function_name)),
										num_args, NULL, false, false, false);

	if (func_list == NULL)
	{
		elog(ERROR, "couldn't find the function %s.%s", schema, function_name);
	}
	if (func_list->next != NULL)
	{
		elog(ERROR, "multiple functions found");
	}

	fmgr_info(func_list->oid, finfo);

	return finfo;
}

static inline int64
get_interval_period(Interval *interval)
{
	if (interval->month != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval defined in terms of month, year, century etc. not supported")
				 ));
	}
#ifdef HAVE_INT64_TIMESTAMP
	return interval->time + (interval->day * USECS_PER_DAY);
#else
	if (interval->time != trunc(interval->time))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("interval must not have sub-second precision")
				 ));
	}
	return interval->time + (interval->day * SECS_PER_DAY);
#endif
}

PG_FUNCTION_INFO_V1(timestamp_bucket);
Datum
timestamp_bucket(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	Timestamp	timestamp = PG_GETARG_TIMESTAMP(1);
	Timestamp	result;
	int64		period = -1;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMP(timestamp);

	period = get_interval_period(interval);
	/* result = (timestamp / period) * period */
	TMODULO(timestamp, result, period);
	if (timestamp < 0)
	{
		/*
		 * need to subtract another period if remainder < 0 this only happens
		 * if timestamp is negative to begin with and there is a remainder
		 * after division. Need to subtract another period since division
		 * truncates toward 0 in C99.
		 */
		result = (result * period) - period;
	}
	else
	{
		result *= period;
	}
	PG_RETURN_TIMESTAMP(result);
}

PG_FUNCTION_INFO_V1(timestamptz_bucket);
Datum
timestamptz_bucket(PG_FUNCTION_ARGS)
{
	Interval   *interval = PG_GETARG_INTERVAL_P(0);
	TimestampTz timestamp = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz result;
	int64		period = -1;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		PG_RETURN_TIMESTAMPTZ(timestamp);

	period = get_interval_period(interval);
	/* result = (timestamp / period) * period */
	TMODULO(timestamp, result, period);
	if (timestamp < 0)
	{
		/*
		 * need to subtract another period if remainder < 0 this only happens
		 * if timestamp is negative to begin with and there is a remainder
		 * after division. Need to subtract another period since division
		 * truncates toward 0 in C99.
		 */
		result = (result * period) - period;
	}
	else
	{
		result *= period;
	}
	PG_RETURN_TIMESTAMPTZ(result);
}
