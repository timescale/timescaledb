/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/lsyscache.h>
#include <utils/fmgrprotos.h>
#include <fmgr.h>

#include <catalog.h>
#include <time_utils.h>

#include "common.h"
#include "freeze.h"
#include "invalidation.h"

Datum
continuous_agg_freeze(PG_FUNCTION_ARGS)
{
	InternalTimeRange freeze_window;
	InternalTimeRange bucketed_freeze_window;
	ContinuousAgg *cagg;

	cagg = continuous_agg_func_with_window(fcinfo, &freeze_window);
	bucketed_freeze_window =
		compute_bucketed_refresh_window(&freeze_window, cagg->data.bucket_width);

	invalidation_cagg_log_add_entry(cagg->data.mat_hypertable_id,
									0,
									bucketed_freeze_window.start,
									/* End is exclusive, so the invalidation added must be adjusted
									 */
									ts_time_saturating_sub(bucketed_freeze_window.end,
														   1,
														   freeze_window.type),
									true);

	PG_RETURN_VOID();
}

Datum
continuous_agg_unfreeze(PG_FUNCTION_ARGS)
{
	InternalTimeRange unfreeze_window;
	InternalTimeRange bucketed_unfreeze_window;
	ContinuousAgg *cagg;

	cagg = continuous_agg_func_with_window(fcinfo, &unfreeze_window);
	bucketed_unfreeze_window =
		compute_bucketed_refresh_window(&unfreeze_window, cagg->data.bucket_width);
	invalidation_cagg_log_unfreeze(cagg, &bucketed_unfreeze_window);

	PG_RETURN_VOID();
}
