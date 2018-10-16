/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */

#ifndef TIMESCALEDB_BGW_POLICY_RECLUSTER_H
#define TIMESCALEDB_BGW_POLICY_RECLUSTER_H

#include "catalog.h"
#include "export.h"

typedef struct BgwPolicyRecluster
{
	FormData_bgw_policy_recluster fd;
} BgwPolicyRecluster;

extern TSDLLEXPORT BgwPolicyRecluster *ts_bgw_policy_recluster_find_by_job(int32 job_id);
extern TSDLLEXPORT BgwPolicyRecluster *ts_bgw_policy_recluster_find_by_hypertable(int32 hypertable_id);
extern TSDLLEXPORT void ts_bgw_policy_recluster_insert(BgwPolicyRecluster *policy);
extern TSDLLEXPORT bool ts_bgw_policy_recluster_delete_row_only_by_job_id(int32 job_id);

#endif							/* TIMESCALEDB_BGW_POLICY_RECLUSTER_H */
