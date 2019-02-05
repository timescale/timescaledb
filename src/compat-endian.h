/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef _COMPAT_ENDIAN_H_
#define _COMPAT_ENDIAN_H_

#if defined(_MSC_VER)

#include <stdlib.h>
#define bswap_32(x) _byteswap_ulong(x)
#define bswap_64(x) _byteswap_uint64(x)
#define _NEED_ENDIAN_COMPAT

#elif defined(__APPLE__)

/*	Mac OS X / Darwin */
#include <libkern/OSByteOrder.h>
#define bswap_32(x) OSSwapInt32(x)
#define bswap_64(x) OSSwapInt64(x)
#define _NEED_ENDIAN_COMPAT

#elif defined(__FreeBSD__)
#include <sys/endian.h>
#else
#include <endian.h>
#endif

#ifdef _NEED_ENDIAN_COMPAT

/* os does not have endian.h macros */

#ifdef WORDS_BIGENDIAN

static inline uint16_t
compat_bswap16(uint16_t v)
{
	return (v << 8) | (v >> 8);
}
#define bswap_16(v) compat_bswap16(v)

#define htobe16(x) ((uint16_t)(x))
#define htobe32(x) ((uint32_t)(x))
#define htobe64(x) ((uint64_t)(x))
#define htole16(x) bswap_16(x)
#define htole32(x) bswap_32(x)
#define htole64(x) bswap_64(x)

#define be16toh(x) ((uint16_t)(x))
#define be32toh(x) ((uint32_t)(x))
#define be64toh(x) ((uint64_t)(x))
#define le16toh(x) bswap_16(x)
#define le32toh(x) bswap_32(x)
#define le64toh(x) bswap_64(x)

#else /* !WORDS_BIGENDIAN */

#define htobe16(x) bswap_16(x)
#define htobe32(x) bswap_32(x)
#define htobe64(x) bswap_64(x)
#define htole16(x) ((uint16_t)(x))
#define htole32(x) ((uint32_t)(x))
#define htole64(x) ((uint64_t)(x))

#define be16toh(x) bswap_16(x)
#define be32toh(x) bswap_32(x)
#define be64toh(x) bswap_64(x)
#define le16toh(x) ((uint16_t)(x))
#define le32toh(x) ((uint32_t)(x))
#define le64toh(x) ((uint64_t)(x))

#endif /* !WORDS_BIGENDIAN */

#endif /* _NEED_ENDIAN_COMPAT */

#endif
