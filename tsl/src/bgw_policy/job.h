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
#include "hypertable.h"
#include "bgw_policy/chunk_stats.h"

/* Reorder function type. Necessary for testing */
typedef void (*reorder_func) (Oid tableOid, Oid indexOid, bool verbose, Oid wait_id);

/* Functions exposed only for testing */
extern bool execute_reorder_policy(int32 job_id, reorder_func reorder);
extern bool execute_drop_chunks_policy(int32 job_id);

extern bool tsl_bgw_policy_job_execute(BgwJob *job);
extern Datum bgw_policy_alter_policy_schedule(PG_FUNCTION_ARGS);

#endif							/* TIMESCALEDB_BGW_POLICY_JOB_H */
