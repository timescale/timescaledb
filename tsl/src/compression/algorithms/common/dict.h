/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "compression/algorithms/common/utils.h"
#include "compression/algorithms/fastlanes/fastlanes.h"

/*
 * common dictionary functions. the dictionary blocks store `K`
 * keys and an index into the keys for each element. the keys
 * and the block headers belong to the compressors, the indices
 * are stored the same way: either as raw values or as a zigzag
 * encoded delta stream, depending on the `zigzag_idxs` flag.
 */

/* DICT probe constants */
#define CM_DICT_ZIGZAG_MIN_K 4 /* delta-coded indices pay off from K > 4 */
#define CM_DICT_HASH_SHIFT 55  /* bucket = top bits of the Fibonacci product */
#define CM_DICT_K_CEIL 128	   /* the probe is limited to 'ceil' distinct keys */
#define CM_DICT_HASH_SIZE 512  /* the size of the dictionary hash table */

/* the ht_idx slots are int8 and the sign bit tells if the slot is
 * occupied. this leaves room for the key indexes 0..127, so the K
 * ceiling must not exceed 128. */
StaticAssertDecl(CM_DICT_K_CEIL <= 128, "int8 ht_idx cannot represent dict key indexes above 127");

/* the idx elem width depends on the zigzag choice */
static inline fl_elem_width_t
cm_dict_idx_elem_width(bool zigzag_idxs)
{
	return zigzag_idxs ? FL_ELEM_W16 : FL_ELEM_W8;
}

/*
 * cm_dict_pack_indices : pack the `n` indices into `dst` and return
 * the packed size. `idx` and `idx_zz` are the raw and the zigzag
 * staged indices from cm_dict_probe_*.
 */
static inline size_t
cm_dict_pack_indices(uint8 *idx, uint16 *idx_zz, uint32 n, uint8 W_idx, bool zigzag_idxs,
					 uint8 *dst)
{
	/* the analyzer staged the indexes up to n; fl_pack reads
	 * fl_input_count() elements, so zero the padding above n */
	uint32 staged_idx = fl_input_count(n, cm_dict_idx_elem_width(zigzag_idxs));
	if (staged_idx > n)
	{
		if (zigzag_idxs)
		{
			memset(&idx_zz[n], 0, (size_t) (staged_idx - n) * sizeof(uint16));
		}
		else
		{
			memset(&idx[n], 0, (size_t) (staged_idx - n) * sizeof(uint8));
		}
	}
	/* the idx elem width depends on the zigzag choice, in both
	 * cases we pack without a base value, because zero is
	 * the min idx */
	return zigzag_idxs ? fl_pack(idx_zz, dst, n, W_idx, FL_ELEM_W16) :
						 fl_pack(idx, dst, n, W_idx, FL_ELEM_W8);
}

/*
 * cm_dict_unpack_indices : unpack the `n` indices into `idx` and
 * return the idx_mask for cm_dict_reconstruct_*. the `keys` must be
 * unpacked by the caller before this call, `key_bytes` is the
 * size of one key.
 *
 * the idx_mask bounds the maximum key index to the next power
 * of two minus one, which keeps the keys[] access in
 * cm_dict_reconstruct_* branch-less instead of a per-element
 * bounds check. out-of-range key indexes only occur on corrupt
 * input: the non-zigzag case cannot read past the block's maximum
 * capacity anyway, but the zigzag case's delta/prefix sum stream
 * can easily go over it. so the idx_mask is more important there.
 *
 * the below memset clears the keys between the first invalid slot
 * (K) and the idx_mask clamp, so corrupt blocks decode these slots
 * to zero instead of leaking stale buffer contents.
 */
static inline uint32
cm_dict_unpack_indices(const uint8 *packed, size_t packed_size, uint32 n, uint16 K, uint8 W_idx,
					   bool zigzag_idxs, void *keys, size_t key_bytes, uint8 *scratch, uint16 *idx)
{
	uint32 idx_mask = ((uint32) 1 << cm_bit_width_u64((uint64) (K - 1))) - 1;

	if (K <= idx_mask)
	{
		uint32 undefined_slots = idx_mask - K + 1;

		memset((uint8 *) keys + (size_t) K * key_bytes, 0, (size_t) undefined_slots * key_bytes);
	}

	/* the indices into the index array, scratch reused */
	memcpy(scratch, packed, packed_size);
	fl_unpack(scratch, idx, n, W_idx, cm_dict_idx_elem_width(zigzag_idxs));
	return idx_mask;
}
