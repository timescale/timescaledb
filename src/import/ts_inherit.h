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
#include <optimizer/planner.h>
#include <parser/parsetree.h>
#include <utils/rel.h>

extern void ts_expand_single_inheritance_child(PlannerInfo *root, RangeTblEntry *parentrte,
													Index parentRTindex, Relation parentrel,
													PlanRowMark *top_parentrc, Relation childrel,
													RangeTblEntry **childrte_p,
													Index *childRTindex_p);
