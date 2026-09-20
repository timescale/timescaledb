/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <postgres.h>
#include "rr_internal.h"

extern size_t rr_null_size_cap(int total_count);

extern RRGlobalFlags rr_null_bitmap_encode(const uint64 *bitmap, size_t total_count,
										   size_t null_count, uint8 *out, size_t *out_size);

extern size_t rr_null_bitmap_decode(StringInfo si, size_t total_count, size_t null_count,
									uint64 *bitmap, RRGlobalFlags flags, size_t *out_n_valid_runs);
