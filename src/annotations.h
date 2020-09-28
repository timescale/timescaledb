/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_ANNOTATIONS_H
#define TIMESCALEDB_ANNOTATIONS_H

/* Fall-through annotation */
#if defined(__clang__)
#if (__clang_major__ >= 12) || (__clang_analyzer__)
#define TS_FALLTHROUGH __attribute__((fallthrough))
#else
#define TS_FALLTHROUGH /* FALLTHROUGH */
#endif				   /* __clang__ */
#elif defined(__GNUC__)
/* Supported since GCC 7 */
#define TS_FALLTHROUGH __attribute__((fallthrough))
#else
/*  MSVC and other compilers */
#define TS_FALLTHROUGH /* FALLTHROUGH */
#endif

#endif /* TIMESCALEDB_ANNOTATIONS_H */
