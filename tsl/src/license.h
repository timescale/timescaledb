/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#include <c.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>

typedef struct LicenseInfo LicenseInfo;

extern Datum tsl_license_update_check(PG_FUNCTION_ARGS);
extern void tsl_license_on_assign(const char *newval, const void *license);
extern void license_switch_to(const LicenseInfo *license);

bool		license_is_expired(void);
bool		license_enterprise_enabled(void);
