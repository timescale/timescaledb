/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_BGW_POLICY_CAGG_API_H
#define TIMESCALEDB_TSL_BGW_POLICY_CAGG_API_H

#include <postgres.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>

extern Datum policy_continuous_aggregate_proc(PG_FUNCTION_ARGS);

int32 policy_continuous_aggregate_get_mat_hypertable_id(const Jsonb *config);

#endif /* TIMESCALEDB_TSL_BGW_POLICY_CAGG_API_H */
