/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <chunk.h>
#include "chunk.h"

extern Datum chunk_status(PG_FUNCTION_ARGS);
extern Datum chunk_show(PG_FUNCTION_ARGS);
extern Datum chunk_create(PG_FUNCTION_ARGS);
extern Datum chunk_api_get_chunk_relstats(PG_FUNCTION_ARGS);
extern Datum chunk_api_get_chunk_colstats(PG_FUNCTION_ARGS);
extern Datum chunk_create_empty_table(PG_FUNCTION_ARGS);
