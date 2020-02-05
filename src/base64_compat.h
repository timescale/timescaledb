/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef BASE64_COMPAT_H
#define BASE64_COMPAT_H

#include "compat.h"

#if PG10_GE

#include <common/base64.h>

#elif PG96

/* base 64 */
extern TSDLLEXPORT int pg_b64_encode(const char *src, int len, char *dst);
extern TSDLLEXPORT int pg_b64_decode(const char *src, int len, char *dst);
extern TSDLLEXPORT int pg_b64_enc_len(int srclen);
extern TSDLLEXPORT int pg_b64_dec_len(int srclen);

#else

#error "Unsupported PostgreSQL version"

#endif

#endif /* BASE64_COMPAT_H */
