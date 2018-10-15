/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_PROCESS_UTILITY_H
#define TIMESCALEDB_PROCESS_UTILITY_H

#include <postgres.h>
#include <nodes/plannodes.h>

extern void process_utility_set_expect_chunk_modification(bool expect);

#endif							/* TIMESCALEDB_PROCESS_UTILITY_H */
