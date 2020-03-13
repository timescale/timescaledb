/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#ifndef TIMESCALEDB_FKEYLIST_H
#define TIMESCALEDB_FKEYLIST_H

#include "compat.h"

#if PG11_LT
extern List *ts_relation_get_fk_list(Relation relation);
#endif

#endif /* TIMESCALEDB_FKEYLIST_H */
