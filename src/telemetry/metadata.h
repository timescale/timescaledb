/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_TELEMETRY_METADATA_H
#define TIMESCALEDB_TELEMETRY_METADATA_H

#include <postgres.h>

extern Datum ts_metadata_get_uuid(void);
extern Datum ts_metadata_get_exported_uuid(void);
extern Datum ts_metadata_get_install_timestamp(void);

#endif							/* TIMESCALEDB_TELEMETRY_METADATA_H */
