/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <miscadmin.h>

#include <jsonb_utils.h>

#include "bgw_policy/continuous_aggregate_api.h"
#include "bgw_policy/job.h"

#define CONFIG_KEY_MAT_HYPERTABLE_ID "mat_hypertable_id"

int32
policy_continuous_aggregate_get_mat_hypertable_id(const Jsonb *config)
{
	bool found;
	int32 mat_hypertable_id =
		ts_jsonb_get_int32_field(config, CONFIG_KEY_MAT_HYPERTABLE_ID, &found);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find \"%s\" in config for job", CONFIG_KEY_MAT_HYPERTABLE_ID)));

	return mat_hypertable_id;
}

Datum
policy_continuous_aggregate_proc(PG_FUNCTION_ARGS)
{
	if (PG_NARGS() != 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_VOID();

	PreventCommandIfReadOnly("policy_continuous_aggregate()");

	policy_continuous_aggregate_execute(PG_GETARG_INT32(0), PG_GETARG_JSONB_P(1));

	PG_RETURN_VOID();
}
