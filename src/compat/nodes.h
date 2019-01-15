/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_COMPAT_NODES_H
#define TIMESCALEDB_COMPAT_NODES_H

#if PG12_LT

#define T_SubscriptingRef T_ArrayRef
typedef ArrayRef SubscriptingRef;

#endif

#endif /* TIMESCALEDB_COMPAT_NODES_H */
