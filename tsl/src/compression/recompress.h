/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_RECOMPRESS_H
#define TIMESCALEDB_TSL_RECOMPRESS_H

#include <postgres.h>
#include <fmgr.h>

#include "compat.h"

extern Datum tsl_recompress_chunk_sfunc(PG_FUNCTION_ARGS);
extern Datum tsl_recompress_chunk_ffunc(PG_FUNCTION_ARGS);
extern void recompress_chunk_tuple(Chunk *uncompressed_chunk);
#endif
