/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TIMESCALEDB_CONSTRAINT_H
#define TIMESCALEDB_CONSTRAINT_H

#include <postgres.h>
#include <access/htup.h>

#include "export.h"

/*
 * Return status for constraint processsing function.
 *
 * PROCESSED - count the constraint as processed
 * IGNORED - the constraint wasn't processed
 * DONE - stop processing constraints
 */
typedef enum ConstraintProcessStatus
{
	CONSTR_PROCESSED,
	CONSTR_PROCESSED_DONE,
	CONSTR_IGNORED,
	CONSTR_IGNORED_DONE,
} ConstraintProcessStatus;

typedef ConstraintProcessStatus (*constraint_func)(HeapTuple constraint_tuple, void *ctx);
extern TSDLLEXPORT int ts_constraint_process(Oid relid, constraint_func process_func, void *ctx);

#endif /* TIMESCALEDB_CONSTRAINT_H */
