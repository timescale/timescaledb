#include <postgres.h>
#include <fmgr.h>

#include "compat.h"

#define TIME_BUCKET(period, timestamp, min, result)			\
	do															\
	{															\
		if (period <= 0) \
			ereport(ERROR, \
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), \
				 errmsg("period must be greater then 0"))); \
		*(result) = (timestamp / period) * period;				\
		if (timestamp < 0)										\
			if (timestamp % period)								\
			{													\
				if (*(result) < min + period)				\
					ereport(ERROR, \
						(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE), \
						 errmsg("timestamp out of range"))); \
				else											\
					*(result) = *(result) - period;				\
			}													\
	} while (0)


TS_FUNCTION_INFO_V1(ts_int16_bucket);
Datum
ts_int16_bucket(PG_FUNCTION_ARGS)
{
	int16		result;

	TIME_BUCKET(PG_GETARG_INT16(0), PG_GETARG_INT16(1), PG_INT16_MIN, &result);

	PG_RETURN_INT16(result);
}

TS_FUNCTION_INFO_V1(ts_int32_bucket);
Datum
ts_int32_bucket(PG_FUNCTION_ARGS)
{
	int32		result;

	TIME_BUCKET(PG_GETARG_INT32(0), PG_GETARG_INT32(1), PG_INT32_MIN, &result);

	PG_RETURN_INT32(result);
}

TS_FUNCTION_INFO_V1(ts_int64_bucket);
Datum
ts_int64_bucket(PG_FUNCTION_ARGS)
{
	int64		result;

	TIME_BUCKET(PG_GETARG_INT64(0), PG_GETARG_INT64(1), PG_INT64_MIN, &result);

	PG_RETURN_INT64(result);
}
