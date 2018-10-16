/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/guc.h>

#include "extension_constants.h"
#include "export.h"
#include "license_guc.h"
#include "cross_module_fn.h"

TS_FUNCTION_INFO_V1(ts_enterprise_enabled);
TS_FUNCTION_INFO_V1(ts_current_license_key);
TS_FUNCTION_INFO_V1(ts_tsl_loaded);

static void *tsl_handle = NULL;
static PGFunction tsl_validate_license_fn = NULL;
static PGFunction tsl_startup_fn = NULL;

/*
 * License Functions
 *
 * License validation is performed via guc update-hooks.
 * In this file we check if the type of license supplied warrants loading an
 * additional module. It loading one is warranted, that module performs the
 * final validation that the license is correct.
 *
 */

static bool load_tsl();

static bool
current_license_can_downgrade_to_apache(void)
{
	return (ts_guc_license_key == NULL || TS_CURRENT_LICENSE_IS_APACHE_ONLY()) &&
		tsl_handle == NULL;
}

/*
 * GUC checks work in two parts:
 *   1. In the check function, all validation of the new value is performed
 *      and any auxiliary state is setup but _not_ installed. Postgres is
 *      allowed to call the check function in cases where the newval will never
 *      be used, so any effects of this function must be confined to `extra` or
 *      otherwise be un-observable. Further, this function is _not_ allowed to
 *      throw exceptions.
 *   2. In the assign function all user-visible state is installed. This
 *      function *MUST NOT FAIL* as it can be called from such places as
 *      transaction commitment, and will cause database restarts if it fails.
 *
 * Therefore our license validation also works in two parts, corresponding to
 * check and assign:
 *   1. In the first stage we check the license kind, load the submodule into
 *      memory if needed (but don't link any of the cross-module functions yet)
 *      and, if it's an enterprise license, valdate the license key.
 *   2. In the second stage we link all of the cross-module functions.
 *
 * The first stage will fail if the provide license key is invalid, or it's
 * trying to downgrade from an edition that uses the submodule
 * (Community and Enterprise) to one that does not (ApacheOnly). Currently only
 * upgrading is allowed within a session; downgrading requires starting a new
 * session.
 */

/*
 * `ts_license_update_check`
 * Used to validate license keys in preperation for `ts_license_on_assign`
 */
bool
ts_license_update_check(char **newval, void **extra, GucSource source)
{
	Datum		module_can_start;

	if (*newval == NULL)
		return false;

	if (!TS_LICENSE_TYPE_IS_VALID(*newval))
		return false;

	/*
	 * we can shutdown the submodule if it was never loaded, or it has a valid
	 * shutdown function.
	 */
	if (TS_LICENSE_IS_APACHE_ONLY(*newval))
	{
		if (current_license_can_downgrade_to_apache())
			return true;

		GUC_check_errdetail("Cannot downgrade a running session to Apache Only.");
		GUC_check_errhint("change the license in the configure file");
		return false;
	}

	if (!load_tsl())
	{
		GUC_check_errdetail("Could not find additional timescaledb module");
		GUC_check_errhint("check that %s-%s is available", TSL_LIBRARY_NAME, TIMESCALEDB_VERSION_MOD);
		return false;
	}

	Assert(tsl_handle != NULL);
	Assert(tsl_validate_license_fn != NULL);
	Assert(tsl_startup_fn != NULL);

	module_can_start = DirectFunctionCall2(
										   tsl_validate_license_fn,
										   CStringGetDatum(*newval),
										   PointerGetDatum(extra));

	return DatumGetBool(module_can_start);
}

/*
 * `ts_license_on_assign`
 * Links the cross-module function pointers.
 */
void
ts_license_on_assign(const char *newval, void *extra)
{
	Assert(TS_LICENSE_TYPE_IS_VALID(newval));
	if (TS_LICENSE_IS_APACHE_ONLY(newval))
	{
		Assert(current_license_can_downgrade_to_apache());
		Assert(extra == NULL);
		return;
	}

	DirectFunctionCall1(tsl_startup_fn, CharGetDatum(0));
	ts_cm_functions->tsl_license_on_assign(newval, extra);
}

/* Module Functions */

/*
 * Load the module, but do not start it.
 * If this function succeeds, `tsl_handle`, and
 * `tsl_startup_fn` will be set, but no `TS_FN` functions will
 * change. `tsl_startup_fn` can be used to enable the submodule
 * versions of `TS_FN`s.
 *
 * This function is idempotent, and will _not_ reload the module
 * if called multiple times.
 *
 * returns:
 *   a function pointer to `tsl_license_update_check` or NULL if the
 *   loading fails
 *
 */
static bool load_tsl(void);

static bool
load_tsl(void)
{
	char		soname[MAX_SO_NAME_LEN] = {0};

	if (tsl_handle != NULL)
	{
		Assert(tsl_startup_fn != NULL);

		/*
		 * We don't want to reload the submodule if it was loaded successfully
		 * in the past because that may relocate symbols we're using. Instead
		 * skip to loading the validation function.
		 */
		goto get_validation_function;
	}

	snprintf(soname, MAX_SO_NAME_LEN, "%s-%s", TSL_LIBRARY_NAME, TIMESCALEDB_VERSION_MOD);

	tsl_startup_fn = load_external_function(
											 /* filename= */ soname,
											 /* funcname= */ "ts_module_init",
											 /* signalNotFound= */ false,
											 /* filehandle= */ &tsl_handle);

	if (tsl_handle == NULL || tsl_startup_fn == NULL)
		goto loading_failed;

get_validation_function:
	tsl_validate_license_fn = lookup_external_function(tsl_handle, "tsl_license_update_check");

	if (tsl_validate_license_fn == NULL)
		goto loading_failed;

	return true;

/*
 * We want this function to be atomic, either all three relevant values are set
 * or none of them are: If we fail to find one of the values set the two static
 * ones back to NULL. (If they were non-NULL to start with that means the
 * function must have been called successfully in the past, so the lookups
 * should still succeed)
 */
loading_failed:
	tsl_handle = NULL;
	tsl_startup_fn = NULL;
	tsl_validate_license_fn = NULL;
	return false;
}

/* SQL functions */

Datum
ts_tsl_loaded(PG_FUNCTION_ARGS)
{
	if (TS_CURRENT_LICENSE_IS_APACHE_ONLY())
		PG_RETURN_BOOL(false);

	PG_RETURN_BOOL(ts_cm_functions->check_tsl_loaded());
}

Datum
ts_enterprise_enabled(PG_FUNCTION_ARGS)
{
	if (TS_CURRENT_LICENSE_IS_APACHE_ONLY())
		PG_RETURN_BOOL(false);

	PG_RETURN_BOOL(ts_cm_functions->enterprise_enabled_internal());
}


Datum
ts_current_license_key(PG_FUNCTION_ARGS)
{
	Assert(ts_guc_license_key != NULL);
	PG_RETURN_TEXT_P(cstring_to_text(ts_guc_license_key));
}
