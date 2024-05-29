/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include "policy_utils.h"
#include "dimension.h"
#include "errors.h"
#include "guc.h"
#include "hypertable.h"
#include "jsonb_utils.h"
#include "policies_v2.h"
#include "time_utils.h"
#include "ts_catalog/continuous_agg.h"
#include <utils/builtins.h>

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
											Oid partitioning_type, Oid lag_type, Datum lag_datum,
											bool isnull)
{
	/*
	 * start_offset and end_offset for CAgg policies are allowed to have NULL values
	 * In that case, config_value will be NULL but this is not an error
	 */

	bool null_ok = (strcmp(json_label, POL_REFRESH_CONF_KEY_END_OFFSET) == 0 ||
					strcmp(json_label, POL_REFRESH_CONF_KEY_START_OFFSET) == 0);

	if (IS_INTEGER_TYPE(partitioning_type) && lag_type != INTERVALOID)
	{
		bool found;
		int64 config_value = ts_jsonb_get_int64_field(config, json_label, &found);

		if (!found && !null_ok)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not find %s in config for existing job", json_label)));

		if (!found && isnull)
			return true;

		if ((!found && !isnull) || (found && isnull))
			return false;

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
		if (config_value == NULL && !null_ok)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("could not find %s in config for job", json_label)));

		if (config_value == NULL && isnull)
			return true;

		if ((config_value == NULL && !isnull) || (config_value != NULL && isnull))
			return false;

		return DatumGetBool(
			DirectFunctionCall2(interval_eq, IntervalPGetDatum(config_value), lag_datum));
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

const Dimension *
get_open_dimension_for_hypertable(const Hypertable *ht, bool fail_if_not_found)
{
	int32 mat_id = ht->fd.id;

	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
		elog(ERROR, "invalid operation on compressed hypertable");

	const Dimension *open_dim = hyperspace_get_open_dimension(ht->space, 0);
	Oid partitioning_type = ts_dimension_get_partition_type(open_dim);
	if (IS_INTEGER_TYPE(partitioning_type))
	{
		/* if this a materialization hypertable related to cont agg
		 * then need to get the right dimension which has
		 * integer_now function
		 */

		open_dim = ts_continuous_agg_find_integer_now_func_by_materialization_id(mat_id);
		if (open_dim == NULL && fail_if_not_found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_TS_UNEXPECTED),
					 errmsg("missing integer_now function for hypertable \"%s\" ",
							get_rel_name(ht->main_table_relid))));
		}
	}
	return open_dim;
}

bool
policy_get_verbose_log(const Jsonb *config)
{
	bool found;
	bool verbose_log = ts_jsonb_get_bool_field(config, CONFIG_KEY_VERBOSE_LOG, &found);

	return found ? verbose_log : false;
}
