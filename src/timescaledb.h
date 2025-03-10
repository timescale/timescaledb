/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * File with definitions intended for plugins that want to interact with TimescaleDB.
 *
 * WARNING! Do not add variables, functions, macros, or other stuff that is
 * not intended for an external user, e.g., a separate plugin.
 */
#pragma once

#include <postgres.h>
#include <fmgr.h>

/*
 * Plugin structure with callbacks.
 *
 * Other plugins can use this to get information about the running version of
 * TimescaleDB as well as callbacks on particular events.
 *
 * IMPORTANT: If you extend this structure, take care to not remove any fields
 * and add new fields last. If you do not do this, you might break plugins
 * that are using the wrong version and read some field that you moved or
 * deleted.
 */
typedef struct TimescaleDBPlugin
{
	size_t size;		   /* Size of this structure */
	Pg_magic_struct magic; /* Pointer to PostgreSQL magic */
	int pg_version;		   /* PostgreSQL version as a number */
	int ts_version;		   /* TimescaleDB version as a number */

	/*
	 * Plugins can fill this in and get callbacks on particular events.
	 */
	void (*bgw_job_starting)(Oid db_id, int32 job_id, Oid user_oid);
	void (*bgw_job_exiting)(Oid db_id, int job_id, int result);
} TimescaleDBPlugin;

#define TIMESCALEDB_PLUGIN_NAME "TimescaleDBPlugin"

/*
 * Convenience macro for plugin callback.
 *
 * The expands to a check if: the plugin pointer is non-zero, the offset of
 * the function pointer is inside the structure, and the function pointer is
 * non-zero.
 *
 * If either is false, this is effectively a no-op.
 *
 * The check that the pointer is inside the size of the structure allows you
 * to add new functions to the structure without risking a call to a random
 * location if the structure is too "old" for the call.
 */
#define TS_PLUGIN_CALLBACK(PTR, FUNC, ...)                                                         \
	do                                                                                             \
	{                                                                                              \
		if ((PTR) && offsetof(TimescaleDBPlugin, FUNC) < (PTR)->size && (PTR)->FUNC)               \
			(*(PTR)->FUNC)(__VA_ARGS__);                                                           \
	} while (0)
