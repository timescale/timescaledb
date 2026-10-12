/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#pragma once

#include <postgres.h>
#include <nodes/primnodes.h>
#include <optimizer/optimizer.h>

#include "compat/compat.h"

#if PG18_LT
extern ScalarArrayOpExpr *make_SAOP_expr(Oid oper, Node *leftexpr, Oid coltype, Oid arraycollid,
										 Oid inputcollid, List *exprs, bool haveNonConst);
#endif
