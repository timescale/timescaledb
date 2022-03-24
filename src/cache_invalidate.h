/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CACHE_INVALIDATE_H
#define TIMESCALEDB_CACHE_INVALIDATE_H

#include <postgres.h>

extern void ts_cache_invalidate_set_proxy_tables(Oid hypertable_proxy_oid, Oid bgw_proxy_oid);

#endif /* TIMESCALEDB_CACHE_INVALIDATE_H */
