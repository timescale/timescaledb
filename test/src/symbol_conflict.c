/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <utils/builtins.h>
#include <fmgr.h>

#include "export.h"

#define STR_EXPAND(x) #x
#define STR(x) STR_EXPAND(x)

#define FUNC_EXPAND(prefix, name) prefix##_##name
#define FUNC(prefix, name) FUNC_EXPAND(prefix, name)

/* Function with conflicting name when included in multiple modules */
extern const char *test_symbol_conflict(void);

const char *
test_symbol_conflict(void)
{
	return "hello from " STR(MODULE_NAME);
}

TS_FUNCTION_INFO_V1(FUNC(MODULE_NAME, hello));

Datum FUNC(MODULE_NAME, hello)(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(test_symbol_conflict()));
}
