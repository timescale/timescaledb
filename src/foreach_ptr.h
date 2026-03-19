/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <compat/compat.h>
#include <postgres.h>
#include <nodes/pg_list.h>

#ifdef PG17_LT
/* In PG16 foreach_ptr is not available, this is a straight copy of the
 * Postgres code that defines foreach_ptr and foreach_internal.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */

#ifndef foreach_ptr
#define foreach_ptr(type, var, lst) foreach_internal(type, *, var, lst, lfirst)
#endif

#ifndef foreach_internal
#define foreach_internal(type, pointer, var, lst, func)                                            \
	for (type pointer var = 0, pointer var##__outerloop = (type pointer) 1; var##__outerloop;      \
		 var##__outerloop = 0)                                                                     \
		for (ForEachState var##__state = { (lst), 0 };                                             \
			 (var##__state.l != NIL && var##__state.i < var##__state.l->length &&                  \
			  (var = (type pointer) func(&var##__state.l->elements[var##__state.i]), true));       \
			 var##__state.i++)
#endif
#endif /* PG17_LT */
