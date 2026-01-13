/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

#define POLICY_PROCESS_HYPER_INVAL_PROC_NAME "policy_process_hypertable_invalidations"
#define POLICY_PROCESS_HYPER_INVAL_CHECK_NAME "policy_process_hypertable_invalidations_check"

extern Datum policy_process_hyper_inval_add(PG_FUNCTION_ARGS);
extern Datum policy_process_hyper_inval_proc(PG_FUNCTION_ARGS);
extern Datum policy_process_hyper_inval_check(PG_FUNCTION_ARGS);
extern Datum policy_process_hyper_inval_remove(PG_FUNCTION_ARGS);
