/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include "compression/algorithms/fastlanes/fastlanes_types.h"
#include "compression/compression.h"

/*
 * the RLE and DELTA_RLE blocks can store elements
 * between the CHECKPOINT value up to the whole batch,
 * potentially more than 256 elements
 */
#define RR_DELTA_RLE_CHECKPOINT 32
#define RR_RLE_CHECKPOINT 128

/*
 * the other, NON-RLE block types have a fixed maximum of 256
 */
#define RR_FIXED_BLOCK_MAX_COUNT 256

/*
 * during compression we don't store the NULL values in the same stream
 * as the values. during decompression, we need to merge the NULL and the
 * value streams (back scatter). the scatter algorithm can go in bulk mode
 * using memmove or scatter one-by-one. when there are multiple consecutive
 * values, memmove is faster. based on the benchmark corpus, the savings
 * start paying off at 8 elements
 */
#define RR_SCATTER_RUN_THRESHOLD 8

/*
 * the maximum number of PFOR exceptions we support. these are the
 * entries we don't want to store as full width in the FOR storage.
 */
#define RR_PFOR_MAX_EXCEPTIONS 64

/* TODO : move this into fastlanes */
#define FL_MAX_ALIGNMENT 32

/*
 * the worst case for one exception stream is a full tier of T-bit
 * elements. T = 64 selects FL tier 128 for any n_exc <= 128
 */
#define RR_PFOR_EXC_BYTES ((size_t) FL_TIER_W128 * FL_ELEM_W64 / 8)
#define RR_SCRATCH_STAGING_BYTES (sizeof(uint64) * RR_FIXED_BLOCK_MAX_COUNT)
#define RR_SCRATCH_BYTES (RR_SCRATCH_STAGING_BYTES + RR_PFOR_EXC_BYTES + FL_MAX_ALIGNMENT)
#define RR_SCRATCH_WORDS (((RR_SCRATCH_BYTES + 63) / 64) * 8)

StaticAssertDecl(RR_PFOR_MAX_EXCEPTIONS <= 128, "exception stream exceeds tier 128");

/* given the RR_PFOR_MAX_EXCEPTIONS, we can size the staging elements
 * according to the element type, but this requires further tightening the
 * RR_PFOR_MAX_EXCEPTIONS checks */
#define RR_PFOR_EXC_STAGING_COUNT(FL_T) (((FL_T) == FL_ELEM_W64) ? 128 : 64)

StaticAssertDecl(RR_PFOR_MAX_EXCEPTIONS <= 64, "exception stream staging area needs more space"
											   " (RR_PFOR_EXC_STAGING_COUNT)");

typedef enum RRGlobalFlags
{
	/* Null storage format flags, if there are NULLs */
	RR_FLAG_NULL_RAW = 0x00,	 /* Whether nulls are stored as a raw bitmap */
	RR_FLAG_NULL_SUMMARY = 0x01, /* Whether nulls are stored as a two-level summary */
	/* Encoded fl_elem_width_t as: */
	RR_FLAG_ELEM_WIDTH_8 = 0x00,  /* 8 bits */
	RR_FLAG_ELEM_WIDTH_16 = 0x02, /* 16 bits */
	RR_FLAG_ELEM_WIDTH_32 = 0x04, /* 32 bits */
	RR_FLAG_ELEM_WIDTH_64 = 0x06, /* 64 bits */

} RRGlobalFlags;

typedef struct RRCompressed
{
	/* Five bytes, common compressed header */
	CompressedDataHeaderFields;
	uint8 flags;		/* RRGlobalFlags */
	uint16 valid_count; /* number of non-null elements */
	uint16 null_count;	/* number of null elements */
	uint16 num_blocks;	/* number of compressed blocks in the data stream */
	/*
	 * Followed by:
	 *  - optional null bitmap or summary (as indicated by the flags)
	 *  - data stream that includes the headers and the packed blocks
	 */
	uint8 values[FLEXIBLE_ARRAY_MEMBER];
} RRCompressed;

static inline uint8
rr_elem_width_to_global_flag(fl_elem_width_t elem_width)
{
	switch (elem_width)
	{
		case FL_ELEM_W8:
			return RR_FLAG_ELEM_WIDTH_8;
		case FL_ELEM_W16:
			return RR_FLAG_ELEM_WIDTH_16;
		case FL_ELEM_W32:
			return RR_FLAG_ELEM_WIDTH_32;
		case FL_ELEM_W64:
			return RR_FLAG_ELEM_WIDTH_64;
		default:
			Assert(false);
			return 0;
	}
}

static inline fl_elem_width_t
rr_global_flag_to_elem_width(uint8 flag)
{
	switch (flag & 0x06)
	{
		case RR_FLAG_ELEM_WIDTH_8:
			return FL_ELEM_W8;
		case RR_FLAG_ELEM_WIDTH_16:
			return FL_ELEM_W16;
		case RR_FLAG_ELEM_WIDTH_32:
			return FL_ELEM_W32;
		case RR_FLAG_ELEM_WIDTH_64:
			return FL_ELEM_W64;
		default:
			Assert(false);
			return 0;
	}
}

static inline fl_elem_width_t
rr_oid_to_elem_width_t(Oid element_type)
{
	fl_elem_width_t result;
	switch (element_type)
	{
		case INT2OID:
			result = FL_ELEM_W16;
			break;
		case INT4OID:
		case DATEOID:
			result = FL_ELEM_W32;
			break;
		case INT8OID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			result = FL_ELEM_W64;
			break;
		default:
			elog(ERROR,
				 "invalid type for rapid-raccoon compressor \"%s\"",
				 format_type_be(element_type));
	};

	return result;
}

typedef union
{
	uint64 uint64_elem[RR_FIXED_BLOCK_MAX_COUNT];
	uint32 uint32_elem[RR_FIXED_BLOCK_MAX_COUNT];
	uint16 uint16_elem[RR_FIXED_BLOCK_MAX_COUNT];
} RRIntBuffer;

/* PG16 on Windows doesn't link pg_popcount64 properly */
static inline int
rr_popcount64(uint64 word)
{
#if defined(WIN32) && PG17_LT
	return (int) pg_popcount((const char *) &word, sizeof(word));
#else
	return pg_popcount64(word);
#endif
}
