/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#ifndef _TIMESCALEDB_SERVER_H
#define _TIMESCALEDB_SERVER_H

#include "catalog.h"

extern Datum server_add(PG_FUNCTION_ARGS);
extern Datum server_delete(PG_FUNCTION_ARGS);
extern List *server_get_servername_list(void);

#endif /* _TIMESCALEDB_SERVER_H */
