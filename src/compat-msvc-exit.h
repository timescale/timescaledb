/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_COMPAT_MSVC_EXIT_H
#define TIMESCALEDB_COMPAT_MSVC_EXIT_H

/*
* Included after all files that need compatibility are included, this undoes
* the 'extern' macro so as not to break other headers (e.g. Windows headers).
*/
#ifdef _MSC_VER
#undef extern
#undef PGDLLIMPORT
#define PGDLLIMPORT __declspec (dllexport)
#endif							/* _MSC_VER */

#endif							/* TIMESCALEDB_COMPAT_MSVC_EXIT_H */
