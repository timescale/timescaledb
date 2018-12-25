/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_EXTENSION_CONSTANTS_H
#define TIMESCALEDB_EXTENSION_CONSTANTS_H

/* No function definitions here, only potentially globally available defines as this is used by the loader*/

#define EXTENSION_NAME "timescaledb"
#define TSL_LIBRARY_NAME "timescaledb-tsl"
#define TS_LIBDIR "$libdir/"
#define EXTENSION_SO TS_LIBDIR""EXTENSION_NAME
#define MAX_VERSION_LEN (NAMEDATALEN+1)
#define MAX_SO_NAME_LEN (8+NAMEDATALEN+1+MAX_VERSION_LEN)	/* "$libdir/"+extname+"-"+version
															 * */

#define CATALOG_SCHEMA_NAME "_timescaledb_catalog"
#define INTERNAL_SCHEMA_NAME "_timescaledb_internal"
#define CACHE_SCHEMA_NAME "_timescaledb_cache"
#define CONFIG_SCHEMA_NAME "_timescaledb_config"
#define RENDEZVOUS_BGW_LOADER_API_VERSION "timescaledb.bgw_loader_api_version"

static const char *const timescaledb_schema_names[] = {
	CATALOG_SCHEMA_NAME, INTERNAL_SCHEMA_NAME, CACHE_SCHEMA_NAME, CONFIG_SCHEMA_NAME
};

#define NUM_TIMESCALEDB_SCHEMAS	(sizeof(timescaledb_schema_names) / sizeof(char *))


#endif							/* TIMESCALEDB_EXTENSION_CONSTANTS_H */
