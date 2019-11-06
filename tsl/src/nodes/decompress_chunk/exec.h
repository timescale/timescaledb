/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H
#define TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H

#include <postgres.h>
#include <nodes/bitmapset.h>
#include <nodes/extensible.h>

#include "compat.h"

#if PG12_LT /* nodes/relation.h renamed in fa2cf16 */
#include <nodes/relation.h>
#else
#include <nodes/pathnodes.h>
#endif

#include "compression/compression.h"

#define DECOMPRESS_CHUNK_COUNT_ID -9
#define DECOMPRESS_CHUNK_SEQUENCE_NUM_ID -10

extern Node *decompress_chunk_state_create(CustomScan *cscan);

#endif /* TIMESCALEDB_DECOMPRESS_CHUNK_EXEC_H */
