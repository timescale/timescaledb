/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#ifndef TIMESCALEDB_BGW_POLICY_JOB_H
#define TIMESCALEDB_BGW_POLICY_JOB_H
#include <c.h>

#include  "bgw/job.h"
extern bool tsl_bgw_policy_job_execute(BgwJob *job);

#endif							/* TIMESCALEDB_BGW_POLICY_JOB_H */
