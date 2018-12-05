/*
 * Copyright (c) 2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Timescale License,
 * see LICENSE-TIMESCALE at the top of the tsl directory.
 */
#ifndef TIMESCALEDB_TSL_TELEMETRY_H
#define TIMESCALEDB_TSL_TELEMETRY_H

#include <postgres.h>
#include <utils/jsonb.h>
void		tsl_telemetry_add_license_info(JsonbParseState *parseState);

#endif							/* TIMESCALEDB_TSL_TELEMETRY_H */
