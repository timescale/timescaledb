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

Datum
subtract_interval_from_now(Interval *lag, Oid time_dim_type)
{
#ifdef TS_DEBUG
	Datum res = ts_get_mock_time_or_current_time();
#else
	Datum res = TimestampTzGetDatum(GetCurrentTransactionStartTimestamp());
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
