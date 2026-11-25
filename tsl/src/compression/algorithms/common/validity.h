/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#pragma once

#include <postgres.h>

typedef enum CmValidityFlags
{
	/* Validity storage format flags, if there are NULLs */
	CM_VALIDITY_RAW = 0x00,		/* Whether validity is stored as a raw bitmap */
	CM_VALIDITY_SUMMARY = 0x01, /* .. or stored as a two-level summary */
} CmValidityFlags;

#define CM_VALIDITY_MASK 0x01

extern size_t cm_validity_size_cap(int total_count);

extern CmValidityFlags cm_validity_bitmap_encode(const uint64 *bitmap, size_t total_count,
												 size_t null_count, uint8 *out, size_t *out_size);

extern size_t cm_validity_bitmap_decode(StringInfo si, size_t total_count, size_t null_count,
										uint64 *bitmap, CmValidityFlags flags,
										size_t *out_n_valid_runs);
