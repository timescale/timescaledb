/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <catalog/pg_collation.h>
#include <funcapi.h>
#include <parser/analyze.h>
#include <parser/parser.h>
#include <tcop/tcopprot.h>

#include "compat/compat.h"
#include "common.h"

extern Datum continuous_agg_validate_query(PG_FUNCTION_ARGS);
extern Datum continuous_agg_get_bucket_function(PG_FUNCTION_ARGS);
extern Datum continuous_agg_get_bucket_function_info(PG_FUNCTION_ARGS);
extern TimestampTz continuous_agg_get_default_origin(Oid new_bucket_function);
