/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <utils/builtins.h>
#include "dimension.h"
#include "guc.h"
#include "jsonb_utils.h"
#include "policy_utils.h"
#include "time_utils.h"

/* Helper function to compare jsonb label value in the config
 * with passed in value.
 * This function is used for labels defined on the hypertable's dimension
 * Parameters:
 * config - jsonb config value
 * label - label we are looking for inside the config
 * partitioning_type - Oid for hypertable's dimension column
 * lag_value - value we will compare against the config's
 *             value for the label
 * lag_type - Oid for lag_value
 * Returns:
 *    True, if config value is equal to lag_value
 */
bool
policy_config_check_hypertable_lag_equality(Jsonb *config, const char *json_label,
											Oid partitioning_type, Oid lag_type, Datum lag_datum)
{
	if (IS_INTEGER_TYPE(partitioning_type))
	{
		bool found;
		int64 config_value = ts_jsonb_get_int64_field(config, json_label, &found);

		if (!found)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not find %s in config for existing job", json_label)));

		switch (lag_type)
		{
			case INT2OID:
				return config_value == DatumGetInt16(lag_datum);
			case INT4OID:
				return config_value == DatumGetInt32(lag_datum);
			case INT8OID:
				return config_value == DatumGetInt64(lag_datum);
			default:
				return false;
		}
	}
	else
	{
		if (lag_type != INTERVALOID)
			return false;
		Interval *config_value = ts_jsonb_get_interval_field(config, json_label);
		if (config_value == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not find %s in config for job", json_label)));

		return DatumGetBool(
			DirectFunctionCall2(interval_eq, IntervalPGetDatum(config_value), lag_datum));
	}
}

int64
subtract_integer_from_now(int64 interval, Oid time_dim_type, Oid now_func)
{
	Datum now;
	int64 res;

	AssertArg(IS_INTEGER_TYPE(time_dim_type));

	now = OidFunctionCall0(now_func);

	switch (time_dim_type)
	{
		case INT2OID:
			res = DatumGetInt16(now) - interval;
			if (res < PG_INT16_MIN || res > PG_INT16_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
						 errmsg("integer time overflow")));
			return res;
		case INT4OID:
			res = DatumGetInt32(now) - interval;
			if (res < PG_INT32_MIN || res > PG_INT32_MAX)
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
						 errmsg("integer time overflow")));
			return res;
		case INT8OID:
		{
			bool overflow = pg_sub_s64_overflow(DatumGetInt64(now), interval, &res);
			if (overflow)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERVAL_FIELD_OVERFLOW),
						 errmsg("integer time overflow")));
			}
			return res;
		}
		default:
			pg_unreachable();
	}
}

Datum
subtract_interval_from_now(Interval *lag, Oid time_dim_type)
{
#ifdef TS_DEBUG
	Datum res = ts_get_mock_time_or_current_time();
#else
	Datum res = TimestampTzGetDatum(GetCurrentTimestamp());
#endif

	switch (time_dim_type)
	{
		case TIMESTAMPOID:
			res = DirectFunctionCall1(timestamptz_timestamp, res);
			res = DirectFunctionCall2(timestamp_mi_interval, res, IntervalPGetDatum(lag));

			return res;
		case TIMESTAMPTZOID:
			res = DirectFunctionCall2(timestamptz_mi_interval, res, IntervalPGetDatum(lag));

			return res;
		case DATEOID:
			res = DirectFunctionCall1(timestamptz_timestamp, res);
			res = DirectFunctionCall2(timestamp_mi_interval, res, IntervalPGetDatum(lag));
			res = DirectFunctionCall1(timestamp_date, res);

			return res;
		default:
			/* this should never happen as otherwise hypertable has unsupported time type */
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("unsupported time type %s", format_type_be(time_dim_type))));
			pg_unreachable();
	}
}
