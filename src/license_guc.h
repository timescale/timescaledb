/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_LICENSE_GUC_H
#define TIMESCALEDB_LICENSE_GUC_H

#include <postgres.h>
#include <fmgr.h>
#include <utils/guc.h>

#include <export.h>
#include <guc.h>

/*
 * The license for using only Apache features is 'ApacheOnly'
 *
 * For documentation on other license types,
 * and information on the license format,
 * see tsl/src/license.c
 * (NB: This requires accepting LICENSE-TIMESCALE)
 */
typedef enum LicenseType
{
	LICENSE_TYPE_APACHE_ONLY = 'A',
	LICENSE_TYPE_COMMUNITY = 'C',
	LICENSE_TYPE_ENTERPRISE = 'E',
}			LicenseType;

#define TS_APACHE_ONLY_LICENSE "ApacheOnly"
#define TS_COMMUNITY_LICENSE "CommunityLicense"

/*
 * If compiled with APACHE_ONLY, default to using only Apache code.
 */
#ifdef APACHE_ONLY
#define TS_DEFAULT_LICENSE TS_APACHE_ONLY_LICENSE
#else
#define TS_DEFAULT_LICENSE TS_COMMUNITY_LICENSE
#endif

#define TS_LICENSE_TYPE_IS_VALID(license) \
    (license[0] == LICENSE_TYPE_APACHE_ONLY || \
        license[0] == LICENSE_TYPE_COMMUNITY || \
        license[0] == LICENSE_TYPE_ENTERPRISE)

#define TS_LICENSE_IS_APACHE_ONLY(license) \
    (license[0] == LICENSE_TYPE_APACHE_ONLY)

#define TS_CURRENT_LICENSE_IS_APACHE_ONLY() \
    TS_LICENSE_IS_APACHE_ONLY(ts_guc_license_key)
/*
 * guc updating happens in two parts:
 *   1. The new guc value is validated, and any fallible code is run, but no
 *      externally-visible changes are performed
 *   2. The guc is set, and the externally-visible changes are performed
 *
 * This means that `ts_license_update_check` should not actually change anything
 * except for its `extra` parameter. (We cheat a little since in that we might
 * load a dynamic library during the `ts_license_update_check`, but we don't
 * consider that change to be visible.)
 */

/* Each of these functions takes a LicenseUpdateExtra for their extra param */
extern bool ts_license_update_check(char **newval, void **extra, GucSource source);
extern void ts_license_on_assign(const char *newval, void *extra);

extern void ts_license_enable_module_loading(void);

#endif							/* LICENSE_GUC */
