/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_REPAIR_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_CAGG_REPAIR_H
#include <postgres.h>

#include <commands/view.h>

#include "continuous_aggs/common.h"
#include "continuous_aggs/finalize.h"
#include "continuous_aggs/materialize.h"
#include "ts_catalog/continuous_agg.h"

extern Datum tsl_cagg_try_repair(PG_FUNCTION_ARGS);

#endif
