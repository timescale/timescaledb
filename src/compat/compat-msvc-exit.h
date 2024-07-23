/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#pragma once

/*
 * Included after all files that need compatibility are included, this undoes
 * the 'extern' macro so as not to break other headers (e.g. Windows headers).
 */
#ifdef _MSC_VER
#undef extern
#undef PGDLLIMPORT
#define PGDLLIMPORT __declspec(dllexport)
#endif /* _MSC_VER */
