/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>

#include "ts_catalog/continuous_agg.h"
#include <test_utils.h>

TS_TEST_FN(ts_test_continuous_agg_find_by_view_name)
{
	Oid cagg_relid = PG_GETARG_OID(0);
	ContinuousAgg *cagg;
	ContinuousAgg *cagg2;

	cagg = ts_continuous_agg_find_by_relid(cagg_relid);
	TestAssertTrue(cagg != NULL);

	/* Get a cagg by direct view  */
	cagg2 = ts_continuous_agg_find_by_view_name(NameStr(cagg->data.direct_view_schema),
												NameStr(cagg->data.direct_view_name),
												ContinuousAggDirectView);
	TestAssertTrue(cagg2->data.mat_hypertable_id == cagg->data.mat_hypertable_id);
	cagg2 = ts_continuous_agg_find_by_view_name(NameStr(cagg->data.direct_view_schema),
												NameStr(cagg->data.direct_view_name),
												ContinuousAggAnyView);
	TestAssertTrue(cagg2->data.mat_hypertable_id == cagg->data.mat_hypertable_id);

	/* Get a cagg by user view */
	cagg2 = ts_continuous_agg_find_by_view_name(NameStr(cagg->data.user_view_schema),
												NameStr(cagg->data.user_view_name),
												ContinuousAggUserView);
	TestAssertTrue(cagg2->data.mat_hypertable_id == cagg->data.mat_hypertable_id);

	cagg2 = ts_continuous_agg_find_by_view_name(NameStr(cagg->data.user_view_schema),
												NameStr(cagg->data.user_view_name),
												ContinuousAggAnyView);
	TestAssertTrue(cagg2->data.mat_hypertable_id == cagg->data.mat_hypertable_id);

	/* Get a cagg by partial view */
	cagg2 = ts_continuous_agg_find_by_view_name(NameStr(cagg->data.partial_view_schema),
												NameStr(cagg->data.partial_view_name),
												ContinuousAggPartialView);
	TestAssertTrue(cagg2->data.mat_hypertable_id == cagg->data.mat_hypertable_id);

	cagg2 = ts_continuous_agg_find_by_view_name(NameStr(cagg->data.partial_view_schema),
												NameStr(cagg->data.partial_view_name),
												ContinuousAggAnyView);
	TestAssertTrue(cagg2->data.mat_hypertable_id == cagg->data.mat_hypertable_id);

	PG_RETURN_VOID();
}
