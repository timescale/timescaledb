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
	uint32 len;
	const uint8 *data;
} BytesView;

static pg_attribute_always_inline uint32
hash_bytes_view(BytesView view)
{
	uint32 valll = -1;
	COMP_CRC32C(valll, view.data, view.len);
	return valll;
}
