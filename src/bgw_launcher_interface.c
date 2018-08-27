#include <postgres.h>

#include <fmgr.h>

#include "extension.h"
#include "bgw_launcher_interface.h"


extern bool
bgw_worker_reserve(void)
{
	PGFunction	reserve = load_external_function(EXTENSION_NAME, "bgw_worker_reserve", true, NULL);

	return DatumGetBool(DirectFunctionCall1(reserve, BoolGetDatum(false))); /* no function call zero */
}

extern void
bgw_worker_release(void)
{
	PGFunction	release = load_external_function(EXTENSION_NAME, "bgw_worker_release", true, NULL);

	DirectFunctionCall1(release, BoolGetDatum(false));	/* no function call zero */
}

extern int
bgw_num_unreserved(void)
{
	PGFunction	unreserved = load_external_function(EXTENSION_NAME, "bgw_num_unreserved", true, NULL);

	return DatumGetInt32(DirectFunctionCall1(unreserved, BoolGetDatum(false))); /* no function call zero */
}
