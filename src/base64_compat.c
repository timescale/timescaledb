/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * Copy of postgres's src/common/base64.c for use in PG 9.6
 *
 * original license:
 *   Copyright (c) 2001-2018, PostgreSQL Global Development Group
 */

#ifndef FRONTEND
#include <postgres.h>
#else
#include <postgres_fe.h>
#endif

#include "base64_compat.h"
#if PG96

/*
 * BASE64
 */

static const char _base64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static const int8 b64lookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 62,
	-1, -1, -1, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0,
	1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
	23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
	39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -1, -1, -1, -1, -1,
};

/*
 * pg_b64_encode
 *
 * Encode into base64 the given string.  Returns the length of the encoded
 * string.
 */
TSDLLEXPORT int
pg_b64_encode(const char *src, int len, char *dst)
{
	char *p;
	const char *s, *end = src + len;
	int pos = 2;
	uint32 buf = 0;

	s = src;
	p = dst;

	while (s < end)
	{
		buf |= (unsigned char) *s << (pos << 3);
		pos--;
		s++;

		/* write it out */
		if (pos < 0)
		{
			*p++ = _base64[(buf >> 18) & 0x3f];
			*p++ = _base64[(buf >> 12) & 0x3f];
			*p++ = _base64[(buf >> 6) & 0x3f];
			*p++ = _base64[buf & 0x3f];

			pos = 2;
			buf = 0;
		}
	}
	if (pos != 2)
	{
		*p++ = _base64[(buf >> 18) & 0x3f];
		*p++ = _base64[(buf >> 12) & 0x3f];
		*p++ = (pos == 0) ? _base64[(buf >> 6) & 0x3f] : '=';
		*p++ = '=';
	}

	return p - dst;
}

/*
 * pg_b64_decode
 *
 * Decode the given base64 string.  Returns the length of the decoded
 * string on success, and -1 in the event of an error.
 */
TSDLLEXPORT int
pg_b64_decode(const char *src, int len, char *dst)
{
	const char *srcend = src + len, *s = src;
	char *p = dst;
	char c;
	int b = 0;
	uint32 buf = 0;
	int pos = 0, end = 0;

	while (s < srcend)
	{
		c = *s++;

		/* Leave if a whitespace is found */
		if (c == ' ' || c == '\t' || c == '\n' || c == '\r')
			return -1;

		if (c == '=')
		{
			/* end sequence */
			if (!end)
			{
				if (pos == 2)
					end = 1;
				else if (pos == 3)
					end = 2;
				else
				{
					/*
					 * Unexpected "=" character found while decoding base64
					 * sequence.
					 */
					return -1;
				}
			}
			b = 0;
		}
		else
		{
			b = -1;
			if (c > 0 && c < 127)
				b = b64lookup[(unsigned char) c];
			if (b < 0)
			{
				/* invalid symbol found */
				return -1;
			}
		}
		/* add it to buffer */
		buf = (buf << 6) + b;
		pos++;
		if (pos == 4)
		{
			*p++ = (buf >> 16) & 255;
			if (end == 0 || end > 1)
				*p++ = (buf >> 8) & 255;
			if (end == 0 || end > 2)
				*p++ = buf & 255;
			buf = 0;
			pos = 0;
		}
	}

	if (pos != 0)
	{
		/*
		 * base64 end sequence is invalid.  Input data is missing padding, is
		 * truncated or is otherwise corrupted.
		 */
		return -1;
	}

	return p - dst;
}

/*
 * pg_b64_enc_len
 *
 * Returns to caller the length of the string if it were encoded with
 * base64 based on the length provided by caller.  This is useful to
 * estimate how large a buffer allocation needs to be done before doing
 * the actual encoding.
 */
TSDLLEXPORT int
pg_b64_enc_len(int srclen)
{
	/* 3 bytes will be converted to 4 */
	return (srclen + 2) * 4 / 3;
}

/*
 * pg_b64_dec_len
 *
 * Returns to caller the length of the string if it were to be decoded
 * with base64, based on the length given by caller.  This is useful to
 * estimate how large a buffer allocation needs to be done before doing
 * the actual decoding.
 */
TSDLLEXPORT int
pg_b64_dec_len(int srclen)
{
	return (srclen * 3) >> 2;
}
#endif /* PG96 */
