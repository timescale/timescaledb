#ifndef TIMESCALEDB_EXTENSION_H
#define TIMESCALEDB_EXTENSION_H
#include <postgres.h>
#include "extension_constants.h"

bool		extension_invalidate(Oid relid);
bool		extension_is_loaded(void);
void		extension_check_version(const char *so_version);
void		extension_check_server_version(void);
Oid			extension_schema_oid(void);

char	   *extension_get_so_name(void);

#endif							/* TIMESCALEDB_EXTENSION_H */
