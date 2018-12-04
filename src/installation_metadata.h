/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_INSTALLATION_METADATA_H
#define TIMESCALEDB_INSTALLATION_METADATA_H

#include <postgres.h>

extern Datum ts_installation_metadata_get_value(Datum metadata_key, Oid key_type, Oid value_type, bool *isnull);
extern Datum ts_installation_metadata_insert(Datum metadata_key, Oid key_type, Datum metadata_value, Oid value_type);

#endif							/* TIMESCALEDB_INSTALLATION_METADATA_H */
