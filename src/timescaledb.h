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

#include <nodes/pg_list.h>

/*
 * Plugin structure with callbacks.
 *
 * Other plugins can use this to get information about the running version of
 * TimescaleDB as well as callbacks on particular events.
 *
 * Note that the header fields are not filled in at _PG_init, but will be
 * filled in before each callback, so these fields can be relied on inside the
 * callback.
 *
 * The plugin should fill in the size and the callbacks (at least the ones
 * that it wants to use). The size will be used to decide what TimescaleDB can
 * rely on, which means that it is only possible to add callbacks to this
 * structure.
 *
 * IMPORTANT: If you extend this structure, take care to not remove any fields
 * and add new fields last. Also avoid using structures that can change
 * between TimescaleDB versions. Instead, add any such structure that you want
 * to pass to callbacks to this header file and copy the information there as
 * necessary.
 */
typedef struct TimescaleDBCallbacks
{
	const Pg_magic_struct *magic; /* Pointer to PostgreSQL magic */
	int pg_version;				  /* PostgreSQL version as a number */
	int ts_version;				  /* TimescaleDB version as a number */

	/*
	 * Plugins can fill the following fields in and get callbacks on
	 * particular events.
	 */

	/* Full size of the structure, as seen by the plugin. */
	size_t size;

	/*
	 * Job callbacks.
	 *
	 * Procedures are passed in as a List-of-String to allow plugin to use
	 * func_get_detail() or similar support functions in PostgreSQL.
	 */
	void (*bgw_job_init)(int job_id, unsigned int user_oid);
	void (*bgw_job_start)(int job_id, Oid owner, const char *application_name, List *proc);
	void (*bgw_job_exit)(int job_id, int exit_status);

	/* Scheduler callbacks */
	void (*bgw_scheduler_start)(void);
	void (*bgw_scheduler_exit)(int exit_status);
} TimescaleDBCallbacks;

#define TIMESCALEDB_PLUGIN_NAME "TimescaleDBCallbacks"
