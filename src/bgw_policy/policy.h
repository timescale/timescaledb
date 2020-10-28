/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_BGW_POLICY_POLICY_H
#define TIMESCALEDB_BGW_POLICY_POLICY_H

#include "scanner.h"
#include "catalog.h"
#include "export.h"

typedef struct BgwJobTypeCount
{
	int32 policy_cagg;
	int32 policy_compression;
	int32 policy_reorder;
	int32 policy_retention;
	int32 policy_telemetry;
	int32 user_defined_action;
} BgwJobTypeCount;

extern ScanTupleResult ts_bgw_policy_delete_row_only_tuple_found(TupleInfo *ti, void *const data);

extern void ts_bgw_policy_delete_by_hypertable_id(int32 hypertable_id);

extern BgwJobTypeCount ts_bgw_job_type_counts(void);

#endif /* TIMESCALEDB_BGW_POLICY_POLICY_H */
