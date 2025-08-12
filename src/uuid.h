/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#pragma once

#include <postgres.h>
#include <utils/uuid.h>

extern pg_uuid_t *ts_uuid_create(void);
extern TSDLLEXPORT pg_uuid_t *ts_create_uuid_v7_from_timestamptz(TimestampTz ts, bool zero);
