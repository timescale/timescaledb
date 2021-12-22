/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#ifndef TIMESCALEDB_TSL_BGW_POLICY_UTILS_H
#define TIMESCALEDB_TSL_BGW_POLICY_UTILS_H

#include <postgres.h>
bool policy_config_check_hypertable_lag_equality(Jsonb *config, const char *json_label,
												 Oid dim_type, Oid lag_type, Datum lag_datum);
int64 subtract_integer_from_now_internal(int64 interval, Oid time_dim_type, Oid now_func,
										 bool *overflow);
Datum subtract_interval_from_now(Interval *interval, Oid time_dim_type);
const Dimension *get_open_dimension_for_hypertable(const Hypertable *ht);
#endif /* TIMESCALEDB_TSL_BGW_POLICY_UTILS_H */
