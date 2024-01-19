/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "pred_text.h"

#include <utils/pg_locale.h>
#include <miscadmin.h>

void
vector_const_texteq(const ArrowArray *arrow, const Datum constdatum, uint64 *restrict result)
{
	Assert(!arrow->dictionary);

	text *consttext = (text *) DatumGetPointer(constdatum);
	const size_t textlen = VARSIZE_ANY_EXHDR(consttext);
	const uint8 *cstring = (uint8 *) VARDATA_ANY(consttext);
	const uint32 *offsets = (uint32 *) arrow->buffers[1];
	const uint8 *values = (uint8 *) arrow->buffers[2];

	const size_t n = arrow->length;
	for (size_t outer = 0; outer < n / 64; outer++)
	{
		uint64 word = 0;
		for (size_t inner = 0; inner < 64; inner++)
		{
			const size_t row = outer * 64 + inner;
			const size_t bit_index = inner;
#define INNER_LOOP                                                                                 \
	const uint32 start = offsets[row];                                                             \
	const uint32 end = offsets[row + 1];                                                           \
	Assert(end >= start);                                                                          \
	const uint32 veclen = end - start;                                                             \
	bool valid = veclen != textlen ?                                                               \
					 false :                                                                       \
					 (strncmp((char *) &values[start], (char *) cstring, textlen) == 0);           \
	word |= ((uint64) valid) << bit_index;

			INNER_LOOP
		}
		result[outer] &= word;
	}

	if (n % 64)
	{
		uint64 word = 0;
		for (size_t row = (n / 64) * 64; row < n; row++)
		{
			const size_t bit_index = row % 64;
			INNER_LOOP
		}
		result[n / 64] &= word;
	}

#undef INNER_LOOP
}

#define LIKE_TRUE 1
#define LIKE_FALSE 0
#define LIKE_ABORT (-1)

#define NextByte(p, plen) ((p)++, (plen)--)

/* Set up to compile like_match.c for single-byte characters */
#define CHAREQ(p1, p2) (*(p1) == *(p2))
#define NextChar(p, plen) NextByte((p), (plen))
#define CopyAdvChar(dst, src, srclen) (*(dst)++ = *(src)++, (srclen)--)

#define MatchText SB_MatchText
#define do_like_escape SB_do_like_escape

#include "ts_like_match.c"

/* setup to compile like_match.c for single byte case insensitive matches */
#define MATCH_LOWER(t) (((t) >= 'A' && (t) <= 'Z') ? ((t) + 'a' - 'A') : (t))
#define NextChar(p, plen) NextByte((p), (plen))
#define MatchText SB_IMatchText

#include "ts_like_match.c"

/* setup to compile like_match.c for UTF8 encoding, using fast NextChar */

#define NextChar(p, plen)                                                                          \
	do                                                                                             \
	{                                                                                              \
		(p)++;                                                                                     \
		(plen)--;                                                                                  \
	} while ((plen) > 0 && (*(p) &0xC0) == 0x80)
#define MatchText UTF8_MatchText

#include "ts_like_match.c"

static void
vector_const_like_impl(const ArrowArray *arrow, const Datum constdatum, uint64 *restrict result,
					   int (*match)(const char *, int, const char *, int), bool should_match)
{
	Assert(!arrow->dictionary);

	text *consttext = (text *) DatumGetPointer(constdatum);
	const size_t textlen = VARSIZE_ANY_EXHDR(consttext);
	const char *restrict cstring = VARDATA_ANY(consttext);
	const uint32 *offsets = (uint32 *) arrow->buffers[1];
	const char *restrict values = arrow->buffers[2];

	const size_t n = arrow->length;
	for (size_t outer = 0; outer < n / 64; outer++)
	{
		uint64 word = 0;
		for (size_t inner = 0; inner < 64; inner++)
		{
			const size_t row = outer * 64 + inner;
			const size_t bit_index = inner;
#define INNER_LOOP                                                                                 \
	const uint32 start = offsets[row];                                                             \
	const uint32 end = offsets[row + 1];                                                           \
	Assert(end >= start);                                                                          \
	const uint32 veclen = end - start;                                                             \
	int result = match(&values[start], veclen, cstring, textlen);                                  \
	bool valid = (result == LIKE_TRUE) == should_match;                                            \
	word |= ((uint64) valid) << bit_index;

			INNER_LOOP
		}
		result[outer] &= word;
	}

	if (n % 64)
	{
		uint64 word = 0;
		for (size_t row = (n / 64) * 64; row < n; row++)
		{
			const size_t bit_index = row % 64;
			INNER_LOOP
		}
		result[n / 64] &= word;
	}

#undef INNER_LOOP
}

void
vector_const_textlike_singlebyte(const ArrowArray *arrow, const Datum constdatum,
								 uint64 *restrict result)
{
	return vector_const_like_impl(arrow, constdatum, result, SB_MatchText, true);
}

void
vector_const_textnlike_singlebyte(const ArrowArray *arrow, const Datum constdatum,
								  uint64 *restrict result)
{
	return vector_const_like_impl(arrow, constdatum, result, SB_MatchText, false);
}

void
vector_const_texticlike_singlebyte(const ArrowArray *arrow, const Datum constdatum,
								   uint64 *restrict result)
{
	return vector_const_like_impl(arrow, constdatum, result, SB_IMatchText, true);
}

void
vector_const_texticnlike_singlebyte(const ArrowArray *arrow, const Datum constdatum,
									uint64 *restrict result)
{
	return vector_const_like_impl(arrow, constdatum, result, SB_IMatchText, false);
}

void
vector_const_textlike_utf8(const ArrowArray *arrow, const Datum constdatum, uint64 *restrict result)
{
	return vector_const_like_impl(arrow, constdatum, result, UTF8_MatchText, true);
}

void
vector_const_textnlike_utf8(const ArrowArray *arrow, const Datum constdatum,
							uint64 *restrict result)
{
	return vector_const_like_impl(arrow, constdatum, result, UTF8_MatchText, false);
}
