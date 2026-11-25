/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * common/dict_impl.h -- per-type dictionary probe/reconstruct template.
 *
 * Usage (after common/dict.h, at the start of the decompression section):
 *     #define CM_DICT_TY uint16                // uint16/uint32/uint64
 *     #include "compression/algorithms/common/dict_impl.h"
 *     #undef CM_DICT_TY
 *
 * Emits these functions per parameter set (all static inline):
 *
 *   cm_dict_probe_{CM_DICT_TY}
 *   cm_dict_reconstruct_{CM_DICT_TY}
 *   cm_dict_reconstruct_plain_{CM_DICT_TY}
 */

#if !defined(CM_DICT_TY)
#error "dict_impl.h requires CM_DICT_TY (uint16/uint32/uint64)"
#endif

/* Symbol-name plumbing: two-level paste so CM_DICT_TY expands before ## */
#define CM_DICT_PASTE2_(a, b) a##b
#define CM_DICT_PASTE2(a, b) CM_DICT_PASTE2_(a, b)
#define CM_DICT_TFN(prefix) CM_DICT_PASTE2(prefix, CM_DICT_TY)

/*
 * cm_dict_probe_.. : fills the 'keys' and 'idx' arrays from the `n`
 * `values`, which the encoder will pack into the block output,
 * and calculates the cost of the idx storage. returns -1 if there
 * are more than `k_cap` distinct keys, the cost of the keys is left
 * to the caller. the zigzag+delta idx storage is only tried if the
 * caller allows it.
 */
static inline int32
CM_DICT_TFN(cm_dict_probe_)(const CM_DICT_TY *values, uint32 n, int32 k_cap, bool zigzag_allowed,
							CM_DICT_TY *keys, uint8 *idx, uint16 *idx_zz, int32 *K_out,
							uint8 *W_idx, bool *zigzag_idxs)
{
	CM_DICT_TY ht_keys[CM_DICT_HASH_SIZE];
	int8 ht_idx[CM_DICT_HASH_SIZE];
	int32 K = 0;
	uint8 w_idx;
	int32 idx_cost;
	bool zz_idx = false;
	uint32 i;

	/* build up the 'keys' and 'idx' arrays */
	Assert(k_cap <= CM_DICT_K_CEIL);
	memset(ht_idx, 0xFF, sizeof(ht_idx));
	for (i = 0; i < n; i += 8)
	{
		uint32 end = Min(i + 8, n);

		for (uint32 j = i; j < end; ++j)
		{
			CM_DICT_TY key = values[j];
			uint32 h =
				(uint32) (((uint64) key * UINT64CONST(0x9E3779B97F4A7C15)) >> CM_DICT_HASH_SHIFT) &
				(CM_DICT_HASH_SIZE - 1);

			int32 slot = (int32) ht_idx[h];
			int32 miss = (slot < 0) | (ht_keys[h] != key);
			int32 kidx = miss ? K : slot;

			/* note: keys are rewritten unconditionally, because
			 * the branch makes a measurable regression in performance */
			keys[K] = key;
			ht_keys[h] = key;
			ht_idx[h] = (int8) kidx;
			idx[j] = (uint8) kidx;
			K += miss;
		}
		if (K > k_cap)
		{
			return -1;
		}
	}
	Assert(K >= 2);

	/* check which idx storage is tighter, raw or zigzag+delta */
	w_idx = cm_bit_width_u64((uint64) (K - 1));
	idx_cost = (int32) fl_result_bytes(n, w_idx, FL_ELEM_W8);
	if (zigzag_allowed && K > CM_DICT_ZIGZAG_MIN_K)
	{
		uint64 zz_or = 0;
		uint8 w_zz;
		int32 zz_encoded_cost;

		idx_zz[0] = 0;
		for (i = 1; i < n; ++i)
		{
			int32 dlt = (int32) idx[i] - (int32) idx[i - 1];
			uint32 zz = cm_zigzag_encode_uint32((uint32) dlt);
			idx_zz[i] = (uint16) zz;
			zz_or |= (uint64) zz;
		}
		w_zz = cm_bit_width_u64(zz_or);
		zz_encoded_cost = (int32) fl_result_bytes(n, w_zz, FL_ELEM_W16);
		if (zz_encoded_cost < idx_cost)
		{
			idx_cost = zz_encoded_cost;
			w_idx = w_zz;
			zz_idx = true;
		}
	}

	*K_out = K;
	*W_idx = w_idx;
	*zigzag_idxs = zz_idx;
	return idx_cost;
}

static pg_noinline void
CM_DICT_TFN(cm_dict_reconstruct_plain_)(CM_DICT_TY *v, const CM_DICT_TY *keys, const uint8 *idx,
										uint32 n, uint32 idx_mask)
{
	size_t m = idx_mask;
	size_t i = 0;

	for (; i + 8 <= n; i += 8)
	{
		v[i] = keys[idx[i] & m];
		v[i + 1] = keys[idx[i + 1] & m];
		v[i + 2] = keys[idx[i + 2] & m];
		v[i + 3] = keys[idx[i + 3] & m];
		v[i + 4] = keys[idx[i + 4] & m];
		v[i + 5] = keys[idx[i + 5] & m];
		v[i + 6] = keys[idx[i + 6] & m];
		v[i + 7] = keys[idx[i + 7] & m];
	}
	for (; i < n; ++i)
	{
		v[i] = keys[idx[i] & m];
	}
}

/* cm_dict_reconstruct_.. : write each element's dictionary value,
 * selected by its unpacked index, into dest.
 *
 * depending on the zigzag flag we either use the indexes as-is
 * or decode them from the delta-coded index form. the zigzag
 * flag also determines the index array's element type: zigzag
 * indexes are uint16, non-zigzag indexes are uint8.
 *
 * delta endoced indices arrive as zigzag deltas and are
 * prefix-summed on the fly.
 *
 * the idx_mask is a cheap way to ensure that the index is in
 * close range of the dictionary keys. this offers a cheap
 * branch-less optimisation for the non-zigzag case, and
 * more importantly, it prevents the zigzag case from going
 * wildly out of range (in case of corrupt data)
 */
static inline void
CM_DICT_TFN(cm_dict_reconstruct_)(uint8 *dest, const CM_DICT_TY *keys, const uint16 *idx_area,
								  uint32 n, uint32 idx_mask, bool zigzag_idxs)
{
	CM_DICT_TY *v = (CM_DICT_TY *) dest;
	if (zigzag_idxs)
	{
		const uint16 *idx = idx_area;
		uint32 prev = 0;
		for (uint32 i = 0; i < n; ++i)
		{
			uint32 zz = (uint32) idx[i];
			prev += cm_zigzag_decode_uint32(zz);
			v[i] = keys[prev & idx_mask];
		}
	}
	else
	{
		CM_DICT_TFN(cm_dict_reconstruct_plain_)(v, keys, (const uint8 *) idx_area, n, idx_mask);
	}
}

#undef CM_DICT_TFN
#undef CM_DICT_PASTE2
#undef CM_DICT_PASTE2_
