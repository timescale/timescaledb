/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This is the header for a narrow fork of a handful of Postgres core
 * heap/toast insert functions. The compressed row written by the row
 * compressor almost always carries a large compresseddata attribute that
 * core's automatic toasting pushes out to the toast relation one ~2KB chunk
 * at a time, each chunk getting its own heap_insert()+index_insert() call.
 * compression_heap_insert() follows the same tuple decisions core would make,
 * but writes the toast chunks for a value with a single heap_multi_insert()
 * call instead, so the WAL and buffer-lock overhead is amortized across all
 * chunks that fit on a page rather than paid once per chunk.
 *
 * Everything here is forked from (and should stay behaviorally identical to)
 * the corresponding core function. The forked code lives under access/, in
 * files named after the core file each function was copied from, so it can be
 * diffed against a Postgres checkout when picking up core changes:
 *
 *   access/heap/heapam.c           compression_heap_insert()
 *   access/heap/heaptoast.c        compression_toast_insert_or_update()
 *   access/table/toast_helper.c    compression_toast_tuple_externalize()
 *   access/common/toast_internals.c
 *                                  compression_toast_save_datum_multi()
 *
 * The call chain runs top to bottom in that list, same as core's does. The
 * comment above each function says which core function it copies and what was
 * changed. Declarations for all of them live here rather than in per-file
 * headers: they only exist so the forked files can call each other, and one
 * header is one less thing to keep in sync.
 */

#pragma once

#include <postgres.h>
#include <access/heapam.h>
#include <access/htup.h>
#include <access/toast_helper.h>
#include <storage/bufmgr.h>
#include <utils/relcache.h>

#include "compression/compression.h"

extern void compression_heap_insert(BulkWriter *writer, HeapTuple tup);
extern void compression_toast_writer_close(BulkWriter *writer);

extern HeapTuple compression_toast_insert_or_update(BulkWriter *writer, HeapTuple newtup);
extern void compression_toast_tuple_externalize(BulkWriter *writer, ToastTupleContext *ttc,
												int attribute);
extern Datum compression_toast_save_datum_multi(BulkWriter *writer, Datum value,
												struct varlena *oldexternal);
