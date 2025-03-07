/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>

#include "planner/planner.h"
#include "timescaledb.h"
#include <c.h>

/*
 * This file contains function and support for dealing with the ABI. See the
 * header file for more information.
 */

/* Structure to go from old definition in mem_guard to the TimescaleDB plugin
   structure defined in the header file. */
static TimescaleDBPlugin plugin_callbacks = {
	.magic = PG_MODULE_MAGIC_DATA,
	.pg_version = PG_VERSION_NUM,
	.ts_version = TS_VERSION_NUM,
};
