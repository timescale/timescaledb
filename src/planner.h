/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_PLANNER_H
#define TIMESCALEDB_PLANNER_H

#include <postgres.h>
#include <nodes/pg_list.h>
#include <nodes/parsenodes.h>

#include "compat.h"

#if PG12_GE
#include <nodes/pathnodes.h>
#else
#include <nodes/relation.h>
#endif

typedef struct TimescaleDBPrivate
{
	bool appends_ordered;
	/* attno of the time dimension in the parent table if appends are ordered */
	int order_attno;
	List *nested_oids;
	bool compressed;
} TimescaleDBPrivate;

extern bool ts_rte_is_hypertable(const RangeTblEntry *rte);

static inline TimescaleDBPrivate *
ts_create_private_reloptinfo(RelOptInfo *rel)
{
	Assert(rel->fdw_private == NULL);
	rel->fdw_private = palloc0(sizeof(TimescaleDBPrivate));
	return rel->fdw_private;
}

static inline TimescaleDBPrivate *
ts_get_private_reloptinfo(const RelOptInfo *rel)
{
	return rel->fdw_private;
}

#endif /* TIMESCALEDB_PLANNER_H */
