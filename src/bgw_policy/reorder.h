/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_BGW_POLICY_REORDER_H
#define TIMESCALEDB_BGW_POLICY_REORDER_H

#include "catalog.h"
#include "export.h"

typedef struct BgwPolicyReorder
{
	FormData_bgw_policy_reorder fd;
} BgwPolicyReorder;

extern TSDLLEXPORT BgwPolicyReorder *ts_bgw_policy_reorder_find_by_job(int32 job_id);
extern TSDLLEXPORT BgwPolicyReorder *ts_bgw_policy_reorder_find_by_hypertable(int32 hypertable_id);
extern TSDLLEXPORT void ts_bgw_policy_reorder_insert(BgwPolicyReorder *policy);
extern TSDLLEXPORT bool ts_bgw_policy_reorder_delete_row_only_by_job_id(int32 job_id);
extern TSDLLEXPORT int32 ts_bgw_policy_reorder_count(void);

#endif /* TIMESCALEDB_BGW_POLICY_REORDER_H */
