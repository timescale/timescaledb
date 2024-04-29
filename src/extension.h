/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <nodes/parsenodes.h>

#include "extension_constants.h"
#include "export.h"

extern void ts_extension_invalidate(void);
extern TSDLLEXPORT bool ts_extension_is_loaded(void);
extern void ts_extension_check_version(const char *so_version);
extern void ts_extension_check_server_version(void);
extern TSDLLEXPORT Oid ts_extension_schema_oid(void);
extern TSDLLEXPORT char *ts_extension_schema_name(void);
extern const char *ts_experimental_schema_name(void);
extern const char *ts_extension_get_so_name(void);
extern bool ts_extension_is_proxy_table_relid(Oid relid);
