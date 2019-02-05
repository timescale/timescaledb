/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PROCESS_UTILITY_H
#define TIMESCALEDB_PROCESS_UTILITY_H

#include <postgres.h>
#include <nodes/plannodes.h>

extern void ts_process_utility_set_expect_chunk_modification(bool expect);

#endif /* TIMESCALEDB_PROCESS_UTILITY_H */
