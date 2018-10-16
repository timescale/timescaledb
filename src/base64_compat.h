/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef BASE64_COMPAT_H
#define BASE64_COMPAT_H

#include <compat.h>

#if PG10

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

#endif							/* BASE64_COMPAT_H */
