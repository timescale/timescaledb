/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <postgres.h>

#include "compression/compression.h"

extern ArrowArray *arrow_create_with_buffers(MemoryContext mcxt, int n_buffers);
extern ArrowArray *arrow_from_iterator(MemoryContext mcxt, DecompressionIterator *iterator,
									   Oid typid);
extern NullableDatum arrow_get_datum(ArrowArray *array, Oid typid, int64 index);
extern ArrowArray *default_decompress_all(Datum compressed, Oid element_type,
										  MemoryContext dest_mctx);
extern void arrow_release_buffers(ArrowArray *array);

#define TYPLEN_VARLEN (-1)
#define TYPLEN_CSTRING (-2)
