/*
 * Copyright (c) 2016-2018  Timescale, Inc. All Rights Reserved.
 *
 * This file is licensed under the Apache License,
 * see LICENSE-APACHE at the top level directory.
 */

#ifndef TIMESCALEDB_BGW_COUNTER_H
#define TIMESCALEDB_BGW_COUNTER_H

#include <postgres.h>



extern int	guc_max_background_workers;

extern void bgw_counter_shmem_alloc(void);
extern void bgw_counter_shmem_startup(void);

extern void bgw_counter_setup_gucs(void);

extern void bgw_counter_reinit(void);
extern bool bgw_total_workers_increment(void);
extern void bgw_total_workers_decrement(void);
extern int	bgw_total_workers_get(void);
extern bool bgw_total_workers_increment_by(int increment_by);
extern void bgw_total_workers_decrement_by(int decrement_by);


#endif							/* TIMESCALEDB_BGW_COUNTER_H */
