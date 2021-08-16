/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_SECLABEL_H
#define TIMESCALEDB_SECLABEL_H

#include <postgres.h>
#include <fmgr.h>
#include <utils/builtins.h>
#include <utils/uuid.h>
#include <commands/seclabel.h>
#include <catalog/pg_database_d.h>
#include "compat/compat.h"
#include "export.h"

#define SECLABEL_DIST_PROVIDER "timescaledb"

#define SECLABEL_DIST_TAG_SEPARATOR ':'
#define SECLABEL_DIST_TAG_SEPARATOR_LEN 1
#define SECLABEL_DIST_TAG "dist_uuid"

extern bool ts_seclabel_get_dist_uuid(Oid dbid, char **uuid);
extern void ts_seclabel_init(void);

#endif /* TIMESCALEDB_SECLABEL_H */
