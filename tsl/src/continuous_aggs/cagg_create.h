/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_CREATE_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_CREATE_H
#include <postgres.h>
#include <nodes/parsenodes.h>

#include "with_clause_parser.h"

bool tsl_process_continuous_agg_viewstmt(ViewStmt *stmt, const char *query_string, void *pstmt,
										 WithClauseResult *with_clause_options);
#endif /* TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_CREATE_H */
