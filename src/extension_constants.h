/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

/* No function definitions here, only potentially globally available defines as this is used by the
 * loader*/

#define EXTENSION_NAME "timescaledb"	  /* Name of the actual extension */
#define EXTENSION_NAMESPACE "timescaledb" /* Namespace for extension objects */
#define EXTENSION_FDW_NAME "timescaledb_fdw"
#define TSL_LIBRARY_NAME "timescaledb-tsl"
#define TS_LIBDIR "$libdir/"
#define EXTENSION_SO TS_LIBDIR "" EXTENSION_NAME
#define EXTENSION_TSL_SO TS_LIBDIR TSL_LIBRARY_NAME "-" TIMESCALEDB_VERSION_MOD
#define TS_HYPERCORE_TAM_NAME "hypercore"

#define MAKE_EXTOPTION(NAME) (EXTENSION_NAMESPACE "." NAME)

#define MAX_VERSION_LEN (NAMEDATALEN + 1)
#define MAX_SO_NAME_LEN                                                                            \
	(8 + NAMEDATALEN + 1 + MAX_VERSION_LEN) /* "$libdir/"+extname+"-"+version                      \
											 * */

typedef enum TsExtensionSchemas
{
	TS_CATALOG_SCHEMA,
	TS_FUNCTIONS_SCHEMA,
	TS_INTERNAL_SCHEMA,
	TS_CACHE_SCHEMA,
	TS_CONFIG_SCHEMA,
	TS_EXPERIMENTAL_SCHEMA,
	TS_INFORMATION_SCHEMA,
	_TS_MAX_SCHEMA,
} TsExtensionSchemas;

#define NUM_TIMESCALEDB_SCHEMAS _TS_MAX_SCHEMA

#define CATALOG_SCHEMA_NAME "_timescaledb_catalog"
#define FUNCTIONS_SCHEMA_NAME "_timescaledb_functions"
#define INTERNAL_SCHEMA_NAME "_timescaledb_internal"
#define CACHE_SCHEMA_NAME "_timescaledb_cache"
#define CONFIG_SCHEMA_NAME "_timescaledb_config"
#define EXPERIMENTAL_SCHEMA_NAME "timescaledb_experimental"
#define INFORMATION_SCHEMA_NAME "timescaledb_information"

extern const char *const ts_extension_schema_names[];

#define RENDEZVOUS_BGW_LOADER_API_VERSION MAKE_EXTOPTION("bgw_loader_api_version")
