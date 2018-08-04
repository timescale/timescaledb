#ifndef TIMESCALEDB_EXTENSION_CONSTANTS_H
#define TIMESCALEDB_EXTENSION_CONSTANTS_H

/* No function definitions here, only potentially globally available defines as this is used by the loader*/

#define EXTENSION_NAME "timescaledb"
#define MAX_VERSION_LEN (NAMEDATALEN+1)
#define MAX_SO_NAME_LEN (NAMEDATALEN+1+MAX_VERSION_LEN) /* extname+"-"+version */

#define CATALOG_SCHEMA_NAME "_timescaledb_catalog"
#define INTERNAL_SCHEMA_NAME "_timescaledb_internal"
#define CACHE_SCHEMA_NAME "_timescaledb_cache"
#define CONFIG_SCHEMA_NAME "_timescaledb_config"


#endif							/* TIMESCALEDB_EXTENSION_CONSTANTS_H */
