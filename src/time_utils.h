/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_TIME_UTILS_H
#define TIMESCALEDB_TIME_UTILS_H

#include <postgres.h>

#include "export.h"

extern TSDLLEXPORT int64 ts_time_value_from_arg(Datum arg, Oid argtype, Oid timetype);

#endif /* TIMESCALEDB_TIME_UTILS_H */
