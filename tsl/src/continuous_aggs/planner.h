/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_CONTINUOUS_AGGS_PLANNER_H
#define TIMESCALEDB_TSL_CONTINUOUS_AGGS_PLANNER_H

#include "planner/planner.h"

void constify_cagg_watermark(Query *parse);

#endif
