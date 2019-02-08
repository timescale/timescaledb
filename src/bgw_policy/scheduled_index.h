/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_BGW_POLICY_SCHEDULED_INDEX_H
#define TIMESCALEDB_BGW_POLICY_SCHEDULED_INDEX_H

#include "catalog.h"
#include "export.h"

typedef struct BgwPolicyScheduledIndex
{
	FormData_bgw_policy_scheduled_index fd;
} BgwPolicyScheduledIndex;

extern TSDLLEXPORT BgwPolicyScheduledIndex *ts_bgw_policy_scheduled_index_find_by_job(int32 job_id);
extern TSDLLEXPORT BgwPolicyScheduledIndex *
ts_bgw_policy_scheduled_index_find_by_hypertable(int32 hypertable_id);
extern TSDLLEXPORT void ts_bgw_policy_scheduled_index_insert(BgwPolicyScheduledIndex *policy);
extern TSDLLEXPORT bool ts_bgw_policy_scheduled_index_delete_row_only_by_job_id(int32 job_id);

#endif /*TIMESCALEDB_BGW_POLICY_SCHEDULED_INDEX_H*/
