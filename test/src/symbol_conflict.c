/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
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

Datum
FUNC(MODULE_NAME, hello) (PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(test_symbol_conflict()));
}
