/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "pred_text.h"

#include <miscadmin.h>

#include "compat/compat.h"

#if PG16_GE
#include <varatt.h>
#endif

static void
vector_const_text_comparison(const ArrowArray *arrow, const Datum constdatum, bool needequal,
							 uint64 *restrict result)
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
			const size_t row = (outer * 64) + inner;
			const size_t bit_index = inner;
#define INNER_LOOP                                                                                 \
	const uint32 start = offsets[row];                                                             \
	const uint32 end = offsets[row + 1];                                                           \
	Assert(end >= start);                                                                          \
	const uint32 veclen = end - start;                                                             \
	bool isequal = veclen != textlen ?                                                             \
					   false :                                                                     \
					   (strncmp((char *) &values[start], (char *) cstring, textlen) == 0);         \
	word |= ((uint64) (isequal == needequal)) << bit_index;

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
vector_const_texteq(const ArrowArray *arrow, const Datum constdatum, uint64 *restrict result)
{
	vector_const_text_comparison(arrow, constdatum, /* needequal = */ true, result);
}

void
vector_const_textne(const ArrowArray *arrow, const Datum constdatum, uint64 *restrict result)
{
	vector_const_text_comparison(arrow, constdatum, /* needequal = */ false, result);
}

/*
 * Generate specializations for LIKE functions based on database encoding. This
 * follows the Postgres code from backend/utils/adt/like.c, version 15.0,
 * commit sha 2a7ce2e2ce474504a707ec03e128fde66cfb8b48.
 * The copy of PG code begins here.
 * ----------------------------------------------------------------------------
 */

#define LIKE_TRUE 1
#define LIKE_FALSE 0
#define LIKE_ABORT (-1)

/* setup to compile like_match.c for UTF8 encoding, using fast NextChar */
#define NextByte(p, plen) ((p)++, (plen)--)
#define NextChar(p, plen)                                                                          \
	do                                                                                             \
	{                                                                                              \
		(p)++;                                                                                     \
		(plen)--;                                                                                  \
	} while ((plen) > 0 && (*(p) &0xC0) == 0x80)
#define MatchText UTF8_MatchText

#include "import/ts_like_match.c"

/*
 * ----------------------------------------------------------------------------
 * The copy of PG code ends here.
 */

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
			const size_t row = (outer * 64) + inner;
			const size_t bit_index = inner;
			/*
			 * The inner loop could have been an inline function, but it would have 5
			 * parameters and one of them in/out, so a macro probably has better
			 * readability.
			 */
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
vector_const_textlike_utf8(const ArrowArray *arrow, const Datum constdatum, uint64 *restrict result)
{
	vector_const_like_impl(arrow, constdatum, result, UTF8_MatchText, /* should_match = */ true);
}

void
vector_const_textnlike_utf8(const ArrowArray *arrow, const Datum constdatum,
							uint64 *restrict result)
{
	vector_const_like_impl(arrow, constdatum, result, UTF8_MatchText, /* should_match = */ false);
}
