/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#ifndef TDIGEST_API_H
#define TDIGEST_API_H

#include "postgres.h"

#include "tdigest.h"
#include "fmgr.h"
#include "export.h"

#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/bytea.h"
#include "utils/datum.h"
#include "utils/typcache.h"
#include "compat.h"

#define TDIGEST_TYPENAME "tdigest"

/*
 * Transition state for a TDigest-based aggregate
 */
typedef struct TDigestTState
{
	TDigest *td;			  /* the TDigest being updated and queried */
	List *unmerged_centroids; /* new data points buffered but not added to the TDigest yet */
} TDigestTState;

extern TSDLLEXPORT Datum ts_tdigest_send_sql(PG_FUNCTION_ARGS);
extern TSDLLEXPORT Datum ts_tdigest_recv_sql(PG_FUNCTION_ARGS);

#endif /* TDIGEST_API_H */
