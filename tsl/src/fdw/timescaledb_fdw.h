/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#ifndef _TIMESCALEDB_TIMESCALEDB_FDW_H
#define _TIMESCALEDB_TIMESCALEDB_FDW_H

#include "postgres.h"
#include "fmgr.h"

extern Datum timescaledb_fdw_handler(PG_FUNCTION_ARGS);
extern Datum timescaledb_fdw_validator(PG_FUNCTION_ARGS);

#endif /* _TIMESCALEDB_TIMESCALEDB_FDW_H */