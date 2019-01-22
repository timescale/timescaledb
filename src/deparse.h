/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_DEPARSE_H
#define TIMESCALEDB_DEPARSE_H

#include <postgres.h>

extern const char *deparse_get_tabledef(Oid relid);

#endif
