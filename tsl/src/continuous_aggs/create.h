/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_CREATE_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_CREATE_H
#include <postgres.h>

#include <process_utility.h>
#include "with_clause_parser.h"
#include "ts_catalog/continuous_agg.h"

DDLResult tsl_process_continuous_agg_viewstmt(Node *node, const char *query_string, void *pstmt,
											  WithClauseResult *with_clause_options);

extern void cagg_flip_realtime_view_definition(ContinuousAgg *agg, Hypertable *mat_ht);
extern void cagg_rename_view_columns(ContinuousAgg *agg);

#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_CREATE_H */
