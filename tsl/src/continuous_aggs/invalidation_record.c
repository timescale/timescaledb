/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <funcapi.h>

#include "invalidation_record.h"

#include <access/tupdesc.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>

#include "compat/compat.h"
#include "export.h"
#include <utils/elog.h>

TS_FUNCTION_INFO_V1(ts_invalidation_read_record);

/*
 * Helper macros to always use the current version when encoding the entry.
 */
#define _field(v) CppConcat(ver, v)
#define _ver _field(INVALIDATION_MESSAGE_CURRENT_VERSION)

/*
 * Read an encoded invalidation record coming from the plugin.
 *
 * Records from the plugin is coming in logical replication format, so decode
 * it and produce a single composite value in the same format as the
 * invalidation table.
 */
Datum
ts_invalidation_read_record(PG_FUNCTION_ARGS)
{
	bytea *raw_record = PG_GETARG_BYTEA_P(0);

	TupleDesc tupdesc;
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	tupdesc = BlessTupleDesc(tupdesc);

	StringInfoData info;
	initReadOnlyStringInfo(&info, VARDATA_ANY(raw_record), VARSIZE_ANY_EXHDR(raw_record));

	InvalidationMessage msg;
	ts_invalidation_record_decode(&info, &msg);

	Datum values[] = {
		[0] = ObjectIdGetDatum(msg._ver.hypertable_relid),
		[1] = Int64GetDatum(msg._ver.lowest_modified),
		[2] = Int64GetDatum(msg._ver.highest_modified),
	};
	bool nulls[3] = { 0 };

	Assert(tupdesc->natts == sizeof(values) / sizeof(*values));

	HeapTuple htup = heap_form_tuple(tupdesc, values, nulls);

	PG_RETURN_DATUM(HeapTupleGetDatum(htup));
}

/*
 * Decode invalidation record in byte array format.
 *
 * This needs to handle all supported version formats, not just the current
 * one. This function should be used on the receiving side of the invalidation
 * plugin and be used to decode records arriving from the plugin.
 */
void
ts_invalidation_record_decode(StringInfo record, InvalidationMessage *msg)
{
	/*
	 * The header is the same for all versions of the invalidation record.
	 */
	msg->hdr.version = pq_getmsgint(record, 2);
	msg->hdr.padding = pq_getmsgint(record, 2);

	switch (msg->hdr.version)
	{
		case 1:
			msg->ver1.hypertable_relid = pq_getmsgint32(record);
			msg->ver1.lowest_modified = pq_getmsgint64(record);
			msg->ver1.highest_modified = pq_getmsgint64(record);
			break;
		default:
			ereport(ERROR,
					errcode(ERRCODE_PROTOCOL_VIOLATION),
					errmsg("unrecognized invalidation record version %d", msg->hdr.version),
					errdetail(
						"Invalidation record from invalidation plugin is using an unrecognized "
						"version, suggesting that it is newer than the extension."),
					errhint("Make sure that invalidation plugin is not from a newer build than the "
							"extension."));
			break;
	}
}

/*
 * Encode an invalidation tuple in byte array format.
 */
void
ts_invalidation_record_encode(InvalidationMessage *msg, StringInfo record)
{
	/*
	 * Header is always the same for all versions. We deliberately set the
	 * version here to avoid bugs in calling this function.
	 */
	msg->hdr.version = INVALIDATION_MESSAGE_CURRENT_VERSION;
	msg->hdr.padding = 0;

	pq_sendint16(record, msg->hdr.version);
	pq_sendint16(record, msg->hdr.padding);

	pq_sendint32(record, msg->_ver.hypertable_relid);
	pq_sendint64(record, msg->_ver.lowest_modified);
	pq_sendint64(record, msg->_ver.highest_modified);
}
