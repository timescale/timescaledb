/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_SERVER_H
#define TIMESCALEDB_TSL_SERVER_H

#include "catalog.h"

extern Datum server_add(PG_FUNCTION_ARGS);
extern Datum server_delete(PG_FUNCTION_ARGS);
extern Datum server_attach(PG_FUNCTION_ARGS);
extern List *server_get_servername_list(void);

#endif /* TIMESCALEDB_TSL_SERVER_H */
