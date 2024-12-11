/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include "ts_catalog/continuous_agg.h"
#include "with_clause_parser.h"

extern void continuous_agg_update_options(ContinuousAgg *cagg,
										  WithClauseResult *with_clause_options);
