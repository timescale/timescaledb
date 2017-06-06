#include <postgres.h>
#include <utils/guc.h>

#include "guc.h"

bool		guc_disable_optimizations = false;
bool		guc_optimize_non_hypertables = false;
bool		guc_restoring = false;
bool        guc_originating_node = false;
bool        guc_ignore_ddl = false;


void
_guc_init(void)
{
	/* Main database to connect to. */
	DefineCustomBoolVariable("timescaledb.disable_optimizations", "Disable all timescale query optimizations",
							 NULL,
							 &guc_disable_optimizations,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);
	DefineCustomBoolVariable("timescaledb.optimize_non_hypertables", "Apply timescale query optimization to plain tables",
							 "Apply timescale query optimization to plain tables in addition to hypertables",
							 &guc_optimize_non_hypertables,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb.restoring", "Install timescale in restoring mode",
							 "Used for running pg_restore",
							 &guc_restoring,
							 false,
							 PGC_SUSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb_internal.originating_node", "Determines whether this is the originating node",
							 "Internal use only",
							 &guc_originating_node,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("timescaledb_internal.ignore_ddl", "Determines whether we should be ignoring ddl commands",
							 "Internal use only",
							 &guc_ignore_ddl,
							 false,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

}

void
_guc_fini(void)
{
}
