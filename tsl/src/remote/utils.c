/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#include <postgres.h>
#include <miscadmin.h>
#include <catalog/pg_type.h>
#include <access/reloptions.h>
#include <access/htup_details.h>
#include <utils/syscache.h>
#include <utils/guc.h>
#include <utils/builtins.h>
#include <utils/float.h>

#include "utils.h"
#include "compat/compat.h"
#include "guc.h"

/*
 * Force assorted GUC parameters to settings that ensure that we'll output
 * data values in a form that is unambiguous to the data node.
 *
 * This is rather expensive and annoying to do once per row, but there's
 * little choice if we want to be sure values are transmitted accurately;
 * we can't leave the settings in place between rows for fear of affecting
 * user-visible computations.
 *
 * We use the equivalent of a function SET option to allow the settings to
 * persist only until the caller calls reset_transmission_modes().  If an
 * error is thrown in between, guc.c will take care of undoing the settings.
 *
 * The return value is the nestlevel that must be passed to
 * reset_transmission_modes() to undo things.
 */
int
set_transmission_modes(void)
{
	int nestlevel = NewGUCNestLevel();

	/*
	 * The values set here should match what pg_dump does.  See also
	 * configure_remote_session in connection.c.
	 */
	if (DateStyle != USE_ISO_DATES)
		(void) set_config_option("datestyle",
								 "ISO",
								 PGC_USERSET,
								 PGC_S_SESSION,
								 GUC_ACTION_SAVE,
								 true,
								 0,
								 false);
	if (IntervalStyle != INTSTYLE_POSTGRES)
		(void) set_config_option("intervalstyle",
								 "postgres",
								 PGC_USERSET,
								 PGC_S_SESSION,
								 GUC_ACTION_SAVE,
								 true,
								 0,
								 false);
	if (extra_float_digits < 3)
		(void) set_config_option("extra_float_digits",
								 "3",
								 PGC_USERSET,
								 PGC_S_SESSION,
								 GUC_ACTION_SAVE,
								 true,
								 0,
								 false);

	return nestlevel;
}

/*
 * Undo the effects of set_transmission_modes().
 */
void
reset_transmission_modes(int nestlevel)
{
	AtEOXact_GUC(true, nestlevel);
}
