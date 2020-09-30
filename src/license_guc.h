/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_LICENSE_GUC_H
#define TIMESCALEDB_LICENSE_GUC_H

#include <postgres.h>
#include <fmgr.h>
#include <utils/guc.h>

#include <export.h>
#include <guc.h>

#define TS_LICENSE_APACHE "apache"
#define TS_LICENSE_TIMESCALE "timescale"

/*
 * If compiled with APACHE_ONLY, default to using only Apache code.
 */
#ifdef APACHE_ONLY
#define TS_LICENSE_DEFAULT TS_LICENSE_APACHE
#else
#define TS_LICENSE_DEFAULT TS_LICENSE_TIMESCALE
#endif

extern bool ts_license_guc_check_hook(char **newval, void **extra, GucSource source);
extern void ts_license_guc_assign_hook(const char *newval, void *extra);

extern TSDLLEXPORT void ts_license_enable_module_loading(void);
extern bool ts_license_is_apache(void);

#endif /* TIMESCALEDB_LICENSE_GUC_H */
