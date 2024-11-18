/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>

#include <port/pg_crc32c.h>

typedef struct BytesView
{
	const uint8 *data;
	uint32 len;
} BytesView;

static pg_attribute_always_inline uint32
hash_bytes_view(BytesView view)
{
	uint32 val = -1;
	COMP_CRC32C(val, view.data, view.len);
	return val;
}
