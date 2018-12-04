/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_BGW_INTERFACE_H
#define TIMESCALEDB_BGW_INTERFACE_H

#include <postgres.h>

extern void ts_bgw_interface_register_api_version(void);
extern const int32 ts_bgw_loader_api_version;

#endif
