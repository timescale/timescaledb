/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains a narrow fork of a handful of Postgres core heap/toast
 * insert functions. The compressed row written by the row compressor almost
 * always carries a large compresseddata attribute that core's automatic
 * toasting pushes out to the toast relation one ~2KB chunk at a time, each
 * chunk getting its own heap_insert()+index_insert() call. compression_heap_insert()
 * follows the same tuple decisions core would make, but writes the toast
 * chunks for a value with a single heap_multi_insert() call instead, so the
 * WAL and buffer-lock overhead is amortized across all chunks that fit on a
 * page rather than paid once per chunk.
 *
 * Everything here is forked from (and should stay behaviorally identical to)
 * the corresponding core function; see the comment above each function for
 * the exact core source it was copied from.
 */

#pragma once

#include <postgres.h>
#include <access/heapam.h>
#include <access/htup.h>
#include <storage/bufmgr.h>
#include <utils/relcache.h>

#include "compression/compression.h"

extern void compression_heap_insert(BulkWriter *writer, HeapTuple tup);
extern void compression_toast_writer_close(BulkWriter *writer);
