/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_BGW_COUNTER_H
#define TIMESCALEDB_BGW_COUNTER_H

#include <postgres.h>

extern int ts_guc_max_background_workers;

extern void ts_bgw_counter_shmem_alloc(void);
extern void ts_bgw_counter_shmem_startup(void);

extern void ts_bgw_counter_setup_gucs(void);

extern void ts_bgw_counter_reinit(void);
extern bool ts_bgw_total_workers_increment(void);
extern void ts_bgw_total_workers_decrement(void);
extern int ts_bgw_total_workers_get(void);
extern bool ts_bgw_total_workers_increment_by(int increment_by);
extern void ts_bgw_total_workers_decrement_by(int decrement_by);

#endif /* TIMESCALEDB_BGW_COUNTER_H */
