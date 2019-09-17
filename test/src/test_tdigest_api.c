/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "postgres.h"

#include "tdigest.h"
#include "tdigest_api.h"
#include "fmgr.h"
#include "export.h"

#include "lib/stringinfo.h"
#include "utils/bytea.h"
#include "utils/datum.h"

/*
 * Ensures the TDigest send/recv functions are perfect inverses of each other
 */
TS_FUNCTION_INFO_V1(ts_tdigest_test_sendrecv);
Datum
ts_tdigest_test_sendrecv(PG_FUNCTION_ARGS)
{
	TDigest *tBefore, *tAfter;
	bytea *sent;
	StringInfoData transmission;

	/* call send, coerce results into StringInfo, then send to recv */
	tBefore = (TDigest *) PG_GETARG_VARLENA_P(0);
	sent = (bytea *) DirectFunctionCall1(ts_tdigest_send_sql, (Datum) tBefore);
	transmission = (StringInfoData){
		.data = VARDATA(sent),
		.len = VARSIZE(sent),
		.maxlen = VARSIZE(sent),
	};
	tAfter = (TDigest *) DirectFunctionCall1(ts_tdigest_recv_sql, (Datum) &transmission);

	/* send/recv are inverses, so tdigest before and after should be equal */
	PG_RETURN_BOOL(ts_tdigest_equal(tBefore, tAfter));
}
