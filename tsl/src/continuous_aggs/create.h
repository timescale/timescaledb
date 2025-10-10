/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "ts_catalog/continuous_agg.h"
#include "with_clause/with_clause_parser.h"
#include <process_utility.h>

DDLResult tsl_process_continuous_agg_viewstmt(Node *node, const char *query_string, void *pstmt,
											  WithClauseResult *with_clause_options);

extern void ts_cagg_add_logical_decoding_slot_finalize(void);
extern void ts_cagg_add_logical_decoding_slot_prepare(const char *slot_name);

extern void cagg_flip_realtime_view_definition(ContinuousAgg *agg, Hypertable *mat_ht);
extern void cagg_rename_view_columns(ContinuousAgg *agg);
extern void cagg_add_trigger_hypertable(Oid relid, int32 hypertable_id);
