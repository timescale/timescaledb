/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>

#include "export.h"
#include "extension_constants.h"
#include "timescaledb.h"

extern void ts_extension_invalidate(void);
extern TSDLLEXPORT bool ts_extension_is_loaded(void);
extern bool ts_extension_is_loaded_and_not_upgrading(void);
extern void ts_extension_check_version(const char *so_version);
extern void ts_extension_check_server_version(void);
extern TSDLLEXPORT Oid ts_extension_schema_oid(void);
extern TSDLLEXPORT char *ts_extension_schema_name(void);
extern const char *ts_experimental_schema_name(void);
extern const char *ts_extension_get_so_name(void);
extern bool ts_extension_is_proxy_table_relid(Oid relid);
extern TSDLLEXPORT void ts_setup_timescaledb_plugin_header(void);

extern TimescaleDBCallbacks **timescaledb_plugin_ptr;

/*
 * Convenience macro for calling a plugin callback from the server side.
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
 *
 * Note that the *plugin* decides the size of the structure. Hence it can only
 * contain callbacks into the plugin.
 */
#define TS_PLUGIN_CALLBACK(FUNC, ...)                                                              \
	do                                                                                             \
	{                                                                                              \
		TimescaleDBCallbacks *ptr = *timescaledb_plugin_ptr;                                       \
		if (ptr && ptr->magic == NULL)                                                             \
			ts_setup_timescaledb_plugin_header();                                                  \
		if (ptr && offsetof(TimescaleDBCallbacks, FUNC) < ptr->size && ptr->FUNC)                  \
			(*ptr->FUNC)(__VA_ARGS__);                                                             \
	} while (0)
