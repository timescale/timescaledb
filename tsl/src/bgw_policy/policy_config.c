/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Module with common functions, variables, and constants for working with
 * policy configurations.
 */

#include <postgres.h>

#include "jsonb_utils.h"
#include "policies_v2.h"
#include "policy_config.h"

int32
policy_config_get_hypertable_id(const Jsonb *config)
{
	bool found;
	int32 hypertable_id = ts_jsonb_get_int32_field(config, POLICY_CONFIG_KEY_HYPERTABLE_ID, &found);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_SQL_JSON_MEMBER_NOT_FOUND),
				 errmsg("could not find hypertable_id in config for job")));

	return hypertable_id;
}

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
