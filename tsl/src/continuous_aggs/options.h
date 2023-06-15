/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_OPTIONS_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_OPTIONS_H
#include <postgres.h>

#include "with_clause_parser.h"
#include "ts_catalog/continuous_agg.h"

extern void continuous_agg_update_options(ContinuousAgg *cagg,
										  WithClauseResult *with_clause_options);
void warn_if_hierarchical_realtime_cagg(int32 parent_mat_hypertable_id, bool materialized_only);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_OPTIONS_H */
