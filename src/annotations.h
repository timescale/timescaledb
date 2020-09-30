/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_ANNOTATIONS_H
#define TIMESCALEDB_ANNOTATIONS_H

/* Supported since clang 12 and GCC 7 */
#if defined __has_attribute
#if __has_attribute(fallthrough)
#define TS_FALLTHROUGH __attribute__((fallthrough))
#else
#define TS_FALLTHROUGH /* FALLTHROUGH */
#endif
#else
#define TS_FALLTHROUGH /* FALLTHROUGH */
#endif

#ifdef __has_attribute
#if __has_attribute(used)
#define TS_USED __attribute__((used))
#else
#define TS_USED
#endif
#else
#define TS_USED
#endif

#endif /* TIMESCALEDB_ANNOTATIONS_H */
