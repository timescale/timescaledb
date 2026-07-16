/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * fastlanes/fastlanes.h -- public interface for the FastLanes pack/unpack
 * layer.
 *
 *   This is the ONLY header callers should include. All functions are
 *   declared here; their definitions live in fastlanes_sizing.c
 *   (tier selection + sizing API), fastlanes_pack.c (plain pack and
 *   unpack), and fastlanes_ffor.c (FFOR pack and unpack).
 *
 * Conventions:
 *
 *   - 'N' is the number of values to pack (0..256).
 *   - 'T' is the element bit width (8, 16, 32, or 64), expressed
 *     via fl_elem_width_t.
 *   - 'W' is the actual bit width of the packed elements (0..T).
 *
 *   See README.md for the full caller contract: alignment and
 *   sizing rules.
 */
#pragma once

#include <postgres.h>

#include "fastlanes_types.h" /* fl_elem_width_t, fl_tier_width_t */

/*
 * Tier selection.
 *
 *   | N range  | T = 8 | T = 16 | T = 32 | T = 64 |
 *   |   0..8   | FL8   | FL16   | FL32   | FL128  |
 *   |   9..16  | FL16  | FL16   | FL32   | FL128  |
 *   |  17..32  | FL32  | FL32   | FL32   | FL128  |
 *   |  33..64  | FL64  | FL64   | FL64   | FL128  |
 *   |  65..128 | FL128 | FL128  | FL128  | FL128  |
 *   | 129..256 | FL256 | FL256  | FL256  | FL256  |
 *
 * T=64 routes straight to FL128/256 (better SIMD utilisation than
 * FL64 for 64-bit values).
 */
static inline fl_tier_width_t
fl_tier_select(uint32 n, fl_elem_width_t t)
{
	if (t == FL_ELEM_W64)
	{
		return (n <= 128) ? FL_TIER_W128 : FL_TIER_W256;
	}
	if (n <= 8 && t == FL_ELEM_W8)
	{
		return FL_TIER_W8;
	}
	if (n <= 16 && (t == FL_ELEM_W8 || t == FL_ELEM_W16))
	{
		return FL_TIER_W16;
	}
	if (n <= 32)
	{
		return FL_TIER_W32;
	}
	if (n <= 64)
	{
		return FL_TIER_W64;
	}
	if (n <= 128)
	{
		return FL_TIER_W128;
	}
	return FL_TIER_W256;
}

/* Required size for the tier (selected by fl_tier_select(N, T)).
 * Determines the sizes of pack output and unpack input buffers exactly.
 */
extern size_t fl_required_bytes(uint32 n, uint8 w, fl_elem_width_t t);

/* Bytes the encoded output carries for N elements (<= fl_required_bytes).
 * Matches the return of fl_pack / fl_pack_ffor; useful for sizing the
 * truncated prefix without running the encoder. */
static inline size_t
fl_result_bytes(uint32 n, uint8 w, fl_elem_width_t t)
{
	/* tier is defined as bitwidth */
	uint32 tier = (uint32) fl_tier_select(n, t);
	/* stride length is the number of 't' elems fit in the tier */
	uint32 s = tier / (uint32) t;
	/* r is the full strides needed to cover n */
	uint32 r = (n + s - 1) / s;

	/* the size is defined as the product of full strides and the encoding
	 * bitwidth (w), in full tier widths, expressed in bytes */
	return (size_t) ((r * w + (uint32) t - 1) / (uint32) t) * (tier / 8);
}

/* Required alignment for the _packed_ input and output buffers.
 * Recommended alignment for the input and output _values_.
 */
extern size_t fl_alignment(uint32 n, fl_elem_width_t t);

/* Number of input ELEMENTS the kernel reads -- callers must provide
 * at least this many readable elements at `values` (positions past N
 * are read but only the [0..N) outputs are meaningful). */
extern uint32 fl_input_count(uint32 n, fl_elem_width_t t);

/* Byte size of the input buffer (= fl_input_count() * t / 8). */
extern size_t fl_input_bytes(uint32 n, fl_elem_width_t t);

/*
 * Plain pack / unpack.
 *
 *   fl_pack returns the truncated byte count (<= fl_required_bytes) --
 *   the size of the `packed` data in bytes. The kernel reads
 *   fl_input_count() elements from `values` and writes fl_required_bytes
 *   to `packed`.
 *
 *   fl_unpack reverses the operation. The caller MUST pre-zero
 *   packed[truncated_bytes..alloc_bytes) before calling unpack.
 *
 *   W = 0 (constant block) is a special case: fl_pack returns 0 and
 *   writes nothing; fl_unpack fills outputs [0..N) with 0.
 */
extern size_t fl_pack(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t);
extern void fl_unpack(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t);

/*
 * FFOR (Frame Of Reference) variants.
 *
 *   `base` is subtracted from each value before packing and added back
 *   after unpacking. Equivalent to packing `values - base` with the
 *   plain functions, but combined into the same loop.
 *
 *   W = 0: fl_pack_ffor returns 0 and writes nothing; fl_unpack_ffor
 *   fills outputs [0..N) with `base`.
 */
extern size_t fl_pack_ffor(const void *values, void *packed, uint32 n, uint8 w, fl_elem_width_t t,
						   uint64 base);
extern void fl_unpack_ffor(const void *packed, void *values, uint32 n, uint8 w, fl_elem_width_t t,
						   uint64 base);

/* the inline implementation of fl_tier_select */
