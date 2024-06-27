/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <postgres.h>

#include "compression/compression.h"

extern ArrowArray *arrow_create_with_buffers(MemoryContext mcxt, int n_buffers);
extern NullableDatum arrow_get_datum(ArrowArray *array, Oid typid, int16 typlen, uint16 index);
extern ArrowArray *arrow_from_compressed(Datum compressed, Oid typid, MemoryContext dest_mcxt,
										 MemoryContext tmp_mcxt);
