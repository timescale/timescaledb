/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_EXTENSION_CONSTANTS_H
#define TIMESCALEDB_EXTENSION_CONSTANTS_H

/* No function definitions here, only potentially globally available defines as this is used by the
 * loader*/

#define EXTENSION_NAME "timescaledb"
#define TSL_LIBRARY_NAME "timescaledb-tsl"
#define TS_LIBDIR "$libdir/"
#define EXTENSION_SO TS_LIBDIR "" EXTENSION_NAME
#define MAX_VERSION_LEN (NAMEDATALEN + 1)
#define MAX_SO_NAME_LEN                                                                            \
	(8 + NAMEDATALEN + 1 + MAX_VERSION_LEN) /* "$libdir/"+extname+"-"+version                      \
											 * */

#define CATALOG_SCHEMA_NAME "_timescaledb_catalog"
#define INTERNAL_SCHEMA_NAME "_timescaledb_internal"
#define CACHE_SCHEMA_NAME "_timescaledb_cache"
#define CONFIG_SCHEMA_NAME "_timescaledb_config"
#define RENDEZVOUS_BGW_LOADER_API_VERSION "timescaledb.bgw_loader_api_version"

enum
{
	CATALOG_SCHEMA_NUM = 0,
	INTERNAL_SCHEMA_NUM,
	CACHE_SCHEMA_NUM,
	CONFIG_SCHEMA_NUM,
	NUM_TIMESCALEDB_SCHEMAS
};

static const char *const timescaledb_schema_names[NUM_TIMESCALEDB_SCHEMAS] = {
	/* if we use the indexes for all the array values clang-format thinks this is an objective-C
	   file */
	[CATALOG_SCHEMA_NUM] = CATALOG_SCHEMA_NAME,
	INTERNAL_SCHEMA_NAME,
	CACHE_SCHEMA_NAME,
	CONFIG_SCHEMA_NAME,
};

#endif /* TIMESCALEDB_EXTENSION_CONSTANTS_H */
