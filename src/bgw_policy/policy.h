/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */

#ifndef TIMESCALEDB_BGW_POLICY_POLICY_H
#define TIMESCALEDB_BGW_POLICY_POLICY_H

#include "scanner.h"
#include "catalog.h"

ScanTupleResult ts_bgw_policy_delete_row_only_tuple_found(TupleInfo *ti, void *const data);

void		ts_bgw_policy_delete_by_hypertable_id(int32 hypertable_id);

#endif							/* TIMESCALEDB_BGW_POLICY_POLICY_H */
