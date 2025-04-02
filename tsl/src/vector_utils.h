/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

/* Enable vectorization for a specific function */
#ifdef __clang__
    #define VECTORIZE_FUNCTION __attribute__((target("sse,sse2,sse3,ssse3,sse4,popcnt,avx")))
#elif defined(__GNUC__) || defined(__GNUG__)
    #define VECTORIZE_FUNCTION __attribute__((optimize("tree-vectorize")))
#else
    #define VECTORIZE_FUNCTION
#endif

/* Enable vectorization for a code block or function */
#ifdef __clang__
    #define BEGIN_VECTORIZE _Pragma("clang attribute push (__attribute__((target(\"sse,sse2,sse3,ssse3,sse4,popcnt,avx\"))), apply_to = function)")
    #define END_VECTORIZE _Pragma("clang attribute pop")
#elif defined(__GNUC__) || defined(__GNUG__)
    #define BEGIN_VECTORIZE _Pragma("GCC push_options") _Pragma("GCC optimize (\"O3\",\"tree-vectorize\")")
    #define END_VECTORIZE _Pragma("GCC pop_options")
#else
    #define BEGIN_VECTORIZE
    #define END_VECTORIZE
#endif

/* Enable vectorization for a specific loop */
#ifdef __clang__
    #define VECTORIZE_LOOP _Pragma("clang loop vectorize(enable)")
#elif defined(__GNUC__) || defined(__GNUG__)
    #define VECTORIZE_LOOP _Pragma("GCC ivdep")
#else
    #define VECTORIZE_LOOP
#endif
