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
#include <nodes/execnodes.h>
#include <nodes/pathnodes.h>
#include <utils/palloc.h>

#include "data_format.h"

typedef struct TupleFactory TupleFactory;
typedef struct TupleDescData *TupleDesc;

extern TupleFactory *tuplefactory_create_for_tupdesc(TupleDesc tupdesc, bool force_text);
extern TupleFactory *tuplefactory_create_for_rel(Relation rel, List *retrieved_attrs);
extern TupleFactory *tuplefactory_create_for_scan(ScanState *ss, List *retrieved_attrs);
extern HeapTuple tuplefactory_make_tuple(TupleFactory *tf, PGresult *res, int row, int format);
extern ItemPointer tuplefactory_make_virtual_tuple(TupleFactory *tf, PGresult *res, int row,
												   int format, Datum *values, bool *nulls);
extern bool tuplefactory_is_binary(TupleFactory *tf);
extern void tuplefactory_set_per_tuple_mctx_reset(TupleFactory *tf, bool reset);
extern void tuplefactory_reset_mctx(TupleFactory *tf);
extern struct AttConvInMetadata *tuplefactory_get_attconv(TupleFactory *tf);
extern TupleDesc tuplefactory_get_tupdesc(TupleFactory *tf);
extern List *tuplefactory_get_retrieved_attrs(TupleFactory *tf);
extern int tuplefactory_get_nattrs(TupleFactory *tf);

#endif /* TIMESCALEDB_TSL_REMOTE_TUPLEFACTORY_H */
