/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_TELEMETRY_UUID_H
#define TIMESCALEDB_TELEMETRY_UUID_H

#include <postgres.h>
#include <utils/uuid.h>

extern pg_uuid_t *uuid_create(void);

#endif							/* TIMESCALEDB_TELEMETRY_UUID_H */
