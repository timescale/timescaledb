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
