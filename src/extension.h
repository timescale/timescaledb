/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_EXTENSION_H
#define TIMESCALEDB_EXTENSION_H
#include <postgres.h>
#include "extension_constants.h"
#include "export.h"

extern bool ts_extension_invalidate(Oid relid);
extern TSDLLEXPORT bool ts_extension_is_loaded(void);
extern void ts_extension_check_version(const char *so_version);
extern void ts_extension_check_server_version(void);
extern Oid	ts_extension_schema_oid(void);

extern char *ts_extension_get_so_name(void);

#endif							/* TIMESCALEDB_EXTENSION_H */
