/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>

#include "ts_catalog/continuous_agg.h"

extern void continuous_agg_add_column(ContinuousAgg *cagg, AlterTableStmt *stmt);
