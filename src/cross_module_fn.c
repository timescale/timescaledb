/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <fmgr.h>

#include "export.h"
#include "cross_module_fn.h"
#include "guc.h"

/*
 * casting a function pointer to a pointer of another type is undefined
 * behavior, so we need one of these for every function type we have
 */

static void
error_no_default_fn()
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("functionality not supported under the current license \"%s\", license", ts_guc_license_key),
			 errhint("Buy a Timescale license to enable the functionality")));

}

static bool
error_no_default_fn_bool_void()
{
	error_no_default_fn();
	return false;
}

static void
tsl_license_on_assign_default_fn(const char *newval, const void *license)
{
	error_no_default_fn();
}

static void
add_telemetry_default(JsonbParseState *parseState)
{
	error_no_default_fn();
}


/*
 * Define cross-module functions' default values:
 * If the submodule isn't activated, using one of the cm functions will throw an
 * exception.
 */
TSDLLEXPORT CrossModuleFunctions ts_cm_functions_default = {
	.tsl_license_on_assign = tsl_license_on_assign_default_fn,
	.enterprise_enabled_internal = error_no_default_fn_bool_void,
	.check_tsl_loaded = error_no_default_fn_bool_void,
	.tsl_module_shutdown = error_no_default_fn,
	.add_tsl_license_info_telemetry = add_telemetry_default,
};

TSDLLEXPORT CrossModuleFunctions *ts_cm_functions = &ts_cm_functions_default;
