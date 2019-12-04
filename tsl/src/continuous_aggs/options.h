/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_OPTIONS_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_OPTIONS_H
#include <postgres.h>
#include <c.h>

#include "with_clause_parser.h"
#include "continuous_agg.h"

extern int64 continuous_agg_parse_refresh_lag(Oid column_type,
											  WithClauseResult *with_clause_options);
extern int64
continuous_agg_parse_ignore_invalidation_older_than(Oid column_type,
													WithClauseResult *with_clause_options);

extern int64 continuous_agg_parse_max_interval_per_job(Oid column_type,
													   WithClauseResult *with_clause_options,
													   int64 bucket_width);

extern void continuous_agg_update_options(ContinuousAgg *cagg,
										  WithClauseResult *with_clause_options);

#endif
