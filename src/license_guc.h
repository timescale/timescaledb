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

/*
 * If compiled with ApacheOnly, default to using only Apache code.
 * For tests we generally want to test all the code, but we don't want
 * tsl startup messages spamming the log, so we default to Apache there too.
 * In a later PR we can decide if we want to default to Community for everything
 * and change all the tests.
 */
#ifdef ApacheOnly
#define TS_DEFAULT_LICENSE "ApacheOnly"
#elif defined(TS_DEBUG)
#define TS_DEFAULT_LICENSE "ApacheOnly"
#else
#define TS_DEFAULT_LICENSE "CommunityLicense"
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
bool		ts_license_update_check(char **newval, void **extra, GucSource source);
void		ts_license_on_assign(const char *newval, void *extra);

#endif							/* LICENSE_GUC */
