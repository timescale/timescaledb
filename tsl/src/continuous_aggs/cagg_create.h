/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#ifndef CONT_CAGG_H
#define CONT_CAGG_H

bool tsl_process_continuous_agg_viewstmt(ViewStmt *stmt, const char *query_string, void *pstmt);
#endif
