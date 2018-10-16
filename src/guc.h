/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */
#ifndef TIMESCALEDB_GUC_H
#define TIMESCALEDB_GUC_H

#include <postgres.h>
#include "export.h"

extern bool ts_telemetry_on(void);

extern bool ts_guc_disable_optimizations;
extern bool ts_guc_optimize_non_hypertables;
extern bool ts_guc_constraint_aware_append;
extern bool ts_guc_restoring;
extern int	ts_guc_max_open_chunks_per_insert;
extern int	ts_guc_max_cached_chunks_per_hypertable;
extern int	ts_guc_telemetry_level;
extern TSDLLEXPORT char *ts_guc_license_key;

void		_guc_init(void);
void		_guc_fini(void);

#endif							/* TIMESCALEDB_GUC_H */
