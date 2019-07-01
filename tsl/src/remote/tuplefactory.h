/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef TIMESCALEDB_TSL_REMOTE_TUPLEFACTORY_H
#define TIMESCALEDB_TSL_REMOTE_TUPLEFACTORY_H

#include <postgres.h>
#include <funcapi.h>
#include <libpq-fe.h>
#include <nodes/relation.h>
#include <nodes/execnodes.h>
#include <utils/palloc.h>

#include "data_format.h"

typedef struct TupleFactory TupleFactory;

extern TupleFactory *tuplefactory_create_for_rel(Relation rel, List *retrieved_attrs);
extern TupleFactory *tuplefactory_create_for_scan(ScanState *ss, List *retrieved_attrs);
extern HeapTuple tuplefactory_make_tuple(TupleFactory *tf, PGresult *res, int row);
extern bool tuplefactory_is_binary(TupleFactory *tf);

#endif /* TIMESCALEDB_TSL_REMOTE_TUPLEFACTORY_H */
