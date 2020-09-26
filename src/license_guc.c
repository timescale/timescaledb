/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>
#include <utils/acl.h>
#include <utils/builtins.h>
#include <utils/guc.h>
#include <miscadmin.h>
#include <catalog/pg_authid.h>

#include "extension_constants.h"
#include "export.h"
#include "license_guc.h"
#include "cross_module_fn.h"

static bool load_enabled = false;
static GucSource load_source = PGC_S_DEFAULT;
static void *tsl_handle = NULL;
static PGFunction tsl_init_fn = NULL;

/*
 * License Functions.
 *
 * License validation is performed via guc update-hooks.
 * In this file we check if the type of license supplied warrants loading an
 * additional module.
 *
 * GUC checks work in two parts:
 *
 *   1. In the check function, all validation of the new value is performed
 *      and any auxiliary state is setup but not installed. This function
 *      is not allowed to throw exceptions.
 *
 *   2. In the assign function all user-visible state is installed. This
 *      function *MUST NOT FAIL* as it can be called from such places as
 *      transaction commitment, and will cause database restarts if it fails.
 *
 * Therefore license validation also works in two parts, corresponding to
 * check and assign:
 *
 *   1. In the first stage we check the license type, load the submodule into
 *      memory if needed (but don't link any of the cross-module functions yet).
 *
 *   2. In the second stage we link all of the cross-module functions and init
 *      tsl module.
 *
 * In order for restoring libraries to work (e.g. in parallel workers), loading
 * the submodule must happen strictly after the main timescaledb module is
 * loaded. In order to ensure that the initial value doesn't break this, we
 * disable loading submodules until the post_load_init.
 *
 * No license change from user session is allowed. License can be changed only
 * if it is set from server configuration file or the server command line.
 */

typedef enum
{
	LICENSE_UNDEF,
	LICENSE_APACHE,
	LICENSE_TIMESCALE
} LicenseType;

static LicenseType
license_type_of(const char *string)
{
	if (string == NULL)
		return LICENSE_UNDEF;
	if (strcmp(string, TS_LICENSE_TIMESCALE) == 0)
		return LICENSE_TIMESCALE;
	if (strcmp(string, TS_LICENSE_APACHE) == 0)
		return LICENSE_APACHE;
	return LICENSE_UNDEF;
}

bool
ts_license_is_apache(void)
{
	return license_type_of(ts_guc_license) == LICENSE_APACHE;
}

TSDLLEXPORT void
ts_license_enable_module_loading(void)
{
	int result;

	if (load_enabled)
		return;

	load_enabled = true;

	/* re-set the license to actually load the submodule if needed */
	result = set_config_option("timescaledb.license",
							   ts_guc_license,
							   PGC_SUSET,
							   load_source,
							   GUC_ACTION_SET,
							   true,
							   0,
							   false);

	if (result <= 0)
		elog(ERROR, "invalid value for timescaledb.license: \"%s\"", ts_guc_license);
}

/*
 * TSL module load function.
 *
 * Load the module, but do not start it. Set tsl_handle and
 * tsl_init_fn module init function pointer (tsl/src/init.c).
 *
 * This function is idempotent, and will not reload the module
 * if called multiple times.
 */
static bool
tsl_module_load(void)
{
	void *function;
	void *handle;

	if (tsl_handle != NULL)
		return true;

	function = load_external_function(EXTENSION_TSL_SO, "ts_module_init", false, &handle);
	if (function == NULL || handle == NULL)
		return false;
	tsl_init_fn = function;
	tsl_handle = handle;
	return true;
}

static void
tsl_module_init(void)
{
	Assert(tsl_handle != NULL);
	Assert(tsl_init_fn != NULL);
	DirectFunctionCall1(tsl_init_fn, CharGetDatum(0));
}

/*
 * Check hook function set by license guc.
 *
 * Used to validate license string before the assign hook
 * ts_license_guc_assign_hook() call.
 */
bool
ts_license_guc_check_hook(char **newval, void **extra, GucSource source)
{
	LicenseType type = license_type_of(*newval);

	/* Allow setting a license only if is is set from postgresql.conf
	 * or the server command line */
	switch (type)
	{
		case LICENSE_APACHE:
		case LICENSE_TIMESCALE:
			if (source == PGC_S_FILE || source == PGC_S_ARGV || source == PGC_S_DEFAULT)
				break;
			GUC_check_errdetail("Cannot change a license in a running session.");
			GUC_check_errhint(
				"Change the license in the configuration file or server command line.");
			return false;
		case LICENSE_UNDEF:
			GUC_check_errdetail("Unrecognized license type.");
			GUC_check_errhint("Supported license types are 'timescale' or 'apache'.");
			return false;
	}

	/* If loading is delayed, save the GucSource for later retry
	 * in the ts_license_enable_module_loading() */
	if (!load_enabled)
	{
		load_source = source;
		return true;
	}

	if (type == LICENSE_TIMESCALE && !tsl_module_load())
	{
		GUC_check_errdetail("Could not find TSL timescaledb module.");
		GUC_check_errhint("Check that \"%s\" is available.", EXTENSION_TSL_SO);
		return false;
	}

	return true;
}

/*
 * Assign hook function set by license guc, executed right after
 * ts_license_guc_check_hook() hook call.
 *
 * Executes tsl module init function (tsl/src/init.c) which sets the
 * cross-module function pointers.
 */
void
ts_license_guc_assign_hook(const char *newval, void *extra)
{
	if (load_enabled && license_type_of(newval) == LICENSE_TIMESCALE)
		tsl_module_init();
}
