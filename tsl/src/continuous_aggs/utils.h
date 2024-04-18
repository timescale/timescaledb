/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <funcapi.h>
#include <catalog/pg_collation.h>
#include <parser/analyze.h>
#include <parser/parser.h>
#include <tcop/tcopprot.h>

#include "common.h"
#include "compat/compat.h"

extern Datum continuous_agg_validate_query(PG_FUNCTION_ARGS);
extern Datum continuous_agg_migrate_to_time_bucket(PG_FUNCTION_ARGS);
