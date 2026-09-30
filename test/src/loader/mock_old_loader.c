/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/* Mock pre-v6 loader: same rendezvous state (API 5, loader-present) but no shim or other
 * hooks. A TAP test preloads it to verify the current extension still loads. */

#include <postgres.h>
#include <fmgr.h>

#include "extension_constants.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Last loader API version without the ProcessUtility shim. */
static int32 mock_old_loader_api_version = 5;
static bool mock_loader_present = true;

/* Must match RENDEZVOUS_LOADER_PRESENT_NAME in extension_utils.c. */
#define MOCK_OLD_LOADER_PRESENT_NAME MAKE_EXTOPTION("loader_present")

void
_PG_init(void)
{
	void **versionptr = find_rendezvous_variable(RENDEZVOUS_BGW_LOADER_API_VERSION);
	void **presentptr = find_rendezvous_variable(MOCK_OLD_LOADER_PRESENT_NAME);

	*versionptr = &mock_old_loader_api_version;
	*presentptr = &mock_loader_present;
}
