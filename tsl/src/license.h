/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#ifndef TIMESCALEDB_TSL_LICENSE_H
#define TIMESCALEDB_TSL_LICENSE_H

#include <postgres.h>
#include <c.h>
#include <export.h>
#include <utils/jsonb.h>
#include <utils/timestamp.h>

typedef struct LicenseInfo LicenseInfo;

extern PGDLLEXPORT Datum tsl_license_update_check(PG_FUNCTION_ARGS);
extern void tsl_license_on_assign(const char *newval, const void *license);
extern void license_switch_to(const LicenseInfo *license);

bool		license_is_expired(void);
bool		license_enterprise_enabled(void);
char	   *license_kind_str(void);
char	   *license_id_str(void);
TimestampTz license_start_time(void);
TimestampTz license_end_time(void);
void		license_enforce_enterprise_enabled(void);

#endif							/* TIMESCALEDB_TSL_LICENSE_H */
