/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "ts_observ_agg.h"
#include "ts_observ_defs.h"
#include "ts_observ_keys.h"
#include "ts_observ_segment.h"

#include <postgres.h>
#include <fmgr.h>
#include <funcapi.h>
#include <miscadmin.h>
#include <storage/lwlock.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/jsonb.h>
#include <utils/numeric.h>
#include <utils/timestamp.h>

/*
 * This file implements the set returning functions (SRFs) that expose the contents of the
 * observabnility data to SQL. The general theme of these functions is that they take a snapshot of
 * the current contents and operate on that snapshot without holding locks to minimize contention.
 * Many SRF calls take a `since` argument that allows them to skip old data in the snapshot.
 *
 * The individual events contain multiple tuples (Key-Value pairs) that are emitted together.
 * Some of these pairs are identifier data, like the metric name, the hypertable, etc., and some are
 * actual metric values. The identifier data is used for filtering and grouping. To filter events
 * (set of K-Vs) by a set of identifier K-Vs, the SRFs can take a JSONB filter argument. The JSONB
 * filter contains K-V pairs of identifier data, and an event matches the filter if it contains
 * all of those pairs. For instance, if the filter is {"metric": "compressed_bytes", "hypertable":
 * "OID"}, then an event matches if it contains K-V pairs for "metric" and "hypertable" with values
 * "compressed_bytes" and "OID", respectively. The filter is applied at the event level, so all
 * K-V pairs of an event are scanned for matches against the filter, and the event is included
 * in the output if all filter K-Vs are matched by any of the event's K-Vs.
 *
 * SRFs that return the event data, do so in a JSONB format as K-V pairs.
 */

/*
 * Copy the circular buffer into a palloc'd local array under LW_SHARED,
 * then release the lock. If `since` is non-zero, only copy the tail
 * starting from the estimated position.
 */
typedef struct TsObservSnapshot
{
	TsObservTuple *tuples;
	uint64 start_seqno;
	uint64 end_seqno; /* exclusive */
	uint32 capacity_mask;
	TimestampTz base_timestamp;
} TsObservSnapshot;

static TsObservSnapshot
ts_observ_take_snapshot(TsObservSegment *seg, TimestampTz since)
{
	TsObservSnapshot snap = { 0 };

	LWLockAcquire(&seg->lock, LW_SHARED);

	uint64 next = pg_atomic_read_u64(&seg->next_seqno);
	uint64 first = next > seg->log.capacity ? next - seg->log.capacity : 0;
	uint32 mask = seg->log.capacity_mask;

	uint64 copy_start = first;
	if (since != 0 && since > seg->base_timestamp)
	{
		/* Skip tuples whose wall-clock time is before `since`. */
		uint64 since_us = (uint64) (since - seg->base_timestamp);
		uint64 since_vts = since_us << TS_OBSERV_SUB_US_BITS;

		copy_start = next;
		for (uint64 s = first; s < next; s++)
		{
			uint32 pos = (uint32) (s & mask);
			uint64 vts = ts_observ_get_virtual_ts(ts_observ_log_entry(seg, pos)->id);
			if (vts >= since_vts)
			{
				copy_start = s;
				break;
			}
		}
	}

	uint64 copy_count = next - copy_start;
	snap.tuples = palloc(copy_count * sizeof(TsObservTuple));
	snap.start_seqno = copy_start;
	snap.end_seqno = next;
	snap.capacity_mask = mask;
	snap.base_timestamp = seg->base_timestamp;

	uint32 start_pos = (uint32) (copy_start & mask);
	if (start_pos + copy_count <= seg->log.capacity)
	{
		memcpy(snap.tuples,
			   ts_observ_log_entry(seg, start_pos),
			   copy_count * sizeof(TsObservTuple));
	}
	else
	{
		uint32 tail = seg->log.capacity - start_pos;
		memcpy(snap.tuples, ts_observ_log_entry(seg, start_pos), tail * sizeof(TsObservTuple));
		memcpy(snap.tuples + tail,
			   ts_observ_log_entry(seg, 0),
			   (copy_count - tail) * sizeof(TsObservTuple));
	}

	LWLockRelease(&seg->lock);
	return snap;
}

/* An event is a contiguous run of tuples sharing the same VTS. */
typedef struct TsObservEventSpan
{
	uint64 vts; /* virtual timestamp (event identity) */
	int start;	/* first tuple index in snapshot */
	int count;	/* number of tuples in this event */
} TsObservEventSpan;

/*
 * Advance to the next event starting at position `from` in the snapshot.
 * Returns false when no more events remain. An event's tuples are
 * contiguous because ts_observ_emit() reserves consecutive slots.
 */
static bool
ts_observ_next_event(const TsObservSnapshot *snap, int from, TsObservEventSpan *span)
{
	if (from >= (int) (snap->end_seqno - snap->start_seqno))
		return false;

	uint64 vts = ts_observ_get_virtual_ts(snap->tuples[from].id);
	int end = from + 1;
	int limit = (int) (snap->end_seqno - snap->start_seqno);
	while (end < limit && ts_observ_get_virtual_ts(snap->tuples[end].id) == vts)
		end++;

	span->vts = vts;
	span->start = from;
	span->count = end - from;
	return true;
}

/* Parse JSONB filter {"key_name": value, ...} into parallel arrays. */
typedef struct TsObservFilter
{
	uint16 key_ids[TS_OBSERV_MAX_AGG_DESCRIPTORS];
	double values[TS_OBSERV_MAX_AGG_DESCRIPTORS];
	int count;
} TsObservFilter;

static void
ts_observ_parse_filter(Jsonb *filter, TsObservFilter *out)
{
	out->count = 0;
	if (filter == NULL)
		return;

	JsonbIterator *it = JsonbIteratorInit(&filter->root);
	JsonbValue v;
	JsonbIteratorToken tok;
	JsonbValue pending_key = { 0 };
	bool have_key = false;

	while ((tok = JsonbIteratorNext(&it, &v, false)) != WJB_DONE)
	{
		if (tok == WJB_KEY)
		{
			pending_key = v;
			have_key = true;
		}
		else if (tok == WJB_VALUE && have_key)
		{
			uint16 kid =
				ts_observ_key_name_to_id(pending_key.val.string.val, pending_key.val.string.len);
			if (kid != TS_KEY_UNUSED && out->count < TS_OBSERV_MAX_AGG_DESCRIPTORS)
			{
				out->key_ids[out->count] = kid;
				out->values[out->count] =
					(v.type == jbvNumeric) ?
						DatumGetFloat8(
							DirectFunctionCall1(numeric_float8, NumericGetDatum(v.val.numeric))) :
						0.0;
				out->count++;
			}
			have_key = false;
		}
	}
}

/* True if every filter predicate matches a tuple in the event. */
static bool
ts_observ_event_matches(const TsObservSnapshot *snap, const TsObservEventSpan *span,
						const TsObservFilter *filter)
{
	for (int f = 0; f < filter->count; f++)
	{
		bool matched = false;
		for (int i = 0; i < span->count; i++)
		{
			const TsObservTuple *t = &snap->tuples[span->start + i];
			if (ts_observ_get_key_id(t->id) == filter->key_ids[f] && t->value == filter->values[f])
			{
				matched = true;
				break;
			}
		}
		if (!matched)
			return false;
	}
	return true;
}

/* VTS to wall-clock TimestampTz. */
static inline TimestampTz
ts_observ_vts_to_timestamp(uint64 vts, TimestampTz base)
{
	return base + (int64) ts_observ_virtual_ts_to_us(vts);
}

/* observ_keys(since) */

typedef struct KeyStats
{
	int64 count;
	uint64 first_vts; /* UINT64_MAX if never seen */
	uint64 last_vts;
} KeyStats;

PG_FUNCTION_INFO_V1(ts_observ_keys);

Datum
ts_observ_keys(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TimestampTz since = PG_ARGISNULL(0) ? 0 : PG_GETARG_TIMESTAMPTZ(0);

	InitMaterializedSRF(fcinfo, 0);
	TupleDesc tupdesc = rsinfo->setDesc;
	Tuplestorestate *tupstore = rsinfo->setResult;

	TsObservSegment *seg = ts_observ_get_segment();
	if (seg == NULL)
		PG_RETURN_VOID();

	/* Per-key_id count and VTS range. */
	KeyStats stats[TS_OBSERV_MAX_KEYS];
	for (int i = 0; i < TS_OBSERV_MAX_KEYS; i++)
		stats[i] = (KeyStats){ .count = 0, .first_vts = UINT64_MAX, .last_vts = 0 };

	TsObservSnapshot snap = ts_observ_take_snapshot(seg, since);
	int limit = (int) (snap.end_seqno - snap.start_seqno);
	for (int i = 0; i < limit; i++)
	{
		uint16 kid = ts_observ_get_key_id(snap.tuples[i].id);
		uint64 vts = ts_observ_get_virtual_ts(snap.tuples[i].id);
		stats[kid].count++;
		if (vts < stats[kid].first_vts)
			stats[kid].first_vts = vts;
		if (vts > stats[kid].last_vts)
			stats[kid].last_vts = vts;
	}
	if (snap.tuples)
		pfree(snap.tuples);

	/* Emit one row per known key. */
	int ncols = tupdesc->natts;
	Datum values[6];
	bool nulls[6];
	int n = ts_observ_key_count();
	for (int i = 0; i < n; i++)
	{
		const TsObservKeyDef *def = ts_observ_key_by_id((uint16) i);
		if (def == NULL)
			continue;
		memset(nulls, false, sizeof(nulls));
		values[0] = Int16GetDatum((int16) def->key_id);
		values[1] = CStringGetTextDatum(def->name);
		values[2] = CStringGetTextDatum(def->description);
		values[3] = Int64GetDatum(stats[def->key_id].count);
		if (stats[def->key_id].count == 0)
		{
			nulls[4] = nulls[5] = true;
			values[4] = values[5] = (Datum) 0;
		}
		else
		{
			values[4] = TimestampTzGetDatum(
				ts_observ_vts_to_timestamp(stats[def->key_id].first_vts, snap.base_timestamp));
			values[5] = TimestampTzGetDatum(
				ts_observ_vts_to_timestamp(stats[def->key_id].last_vts, snap.base_timestamp));
		}
		(void) ncols;
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ts_observ_log);

Datum
ts_observ_log(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Jsonb *filter_arg = PG_ARGISNULL(0) ? NULL : PG_GETARG_JSONB_P(0);
	TimestampTz since = PG_ARGISNULL(1) ? 0 : PG_GETARG_TIMESTAMPTZ(1);
	ArrayType *keys_arr = PG_ARGISNULL(2) ? NULL : PG_GETARG_ARRAYTYPE_P(2);

	InitMaterializedSRF(fcinfo, 0);
	TupleDesc tupdesc = rsinfo->setDesc;
	Tuplestorestate *tupstore = rsinfo->setResult;

	TsObservSegment *seg = ts_observ_get_segment();
	if (seg == NULL)
		PG_RETURN_VOID();

	/* Build a key_id bitset from the optional keys[] projection. */
	bool key_filter_active = (keys_arr != NULL);
	bool allowed[TS_OBSERV_MAX_KEYS] = { false };
	if (key_filter_active)
	{
		Datum *kd;
		bool *kn;
		int kc;
		deconstruct_array(keys_arr, TEXTOID, -1, false, 'i', &kd, &kn, &kc);
		for (int i = 0; i < kc; i++)
		{
			if (kn[i])
				continue;
			text *t = DatumGetTextP(kd[i]);
			uint16 kid = ts_observ_key_name_to_id(VARDATA_ANY(t), VARSIZE_ANY_EXHDR(t));
			if (kid != TS_KEY_UNUSED)
				allowed[kid] = true;
		}
	}

	TsObservFilter filter;
	ts_observ_parse_filter(filter_arg, &filter);

	TsObservSnapshot snap = ts_observ_take_snapshot(seg, since);

	Datum values[4];
	bool nulls[4] = { false };
	TsObservEventSpan span;
	int from = 0;
	while (ts_observ_next_event(&snap, from, &span))
	{
		from = span.start + span.count;

		if (filter.count > 0 && !ts_observ_event_matches(&snap, &span, &filter))
			continue;

		TimestampTz event_time = ts_observ_vts_to_timestamp(span.vts, snap.base_timestamp);

		for (int i = 0; i < span.count; i++)
		{
			const TsObservTuple *t = &snap.tuples[span.start + i];
			uint16 kid = ts_observ_get_key_id(t->id);
			if (key_filter_active && !allowed[kid])
				continue;

			const TsObservKeyDef *def = ts_observ_key_by_id(kid);
			if (def == NULL)
				continue;

			values[0] = Int64GetDatum((int64) span.vts);
			values[1] = TimestampTzGetDatum(event_time);
			values[2] = CStringGetTextDatum(def->name);
			values[3] = Float8GetDatum(t->value);
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	if (snap.tuples)
		pfree(snap.tuples);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ts_observ_key_values);

Datum
ts_observ_key_values(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	text *key_arg = PG_GETARG_TEXT_PP(0);
	Jsonb *filter_arg = PG_ARGISNULL(1) ? NULL : PG_GETARG_JSONB_P(1);
	TimestampTz since = PG_ARGISNULL(2) ? 0 : PG_GETARG_TIMESTAMPTZ(2);

	InitMaterializedSRF(fcinfo, 0);
	TupleDesc tupdesc = rsinfo->setDesc;
	Tuplestorestate *tupstore = rsinfo->setResult;

	uint16 target_key = ts_observ_key_name_to_id(VARDATA_ANY(key_arg), VARSIZE_ANY_EXHDR(key_arg));
	if (target_key == TS_KEY_UNUSED)
		PG_RETURN_VOID();

	TsObservSegment *seg = ts_observ_get_segment();
	if (seg == NULL)
		PG_RETURN_VOID();

	TsObservFilter filter;
	ts_observ_parse_filter(filter_arg, &filter);

	TsObservSnapshot snap = ts_observ_take_snapshot(seg, since);

	Datum values[2];
	bool nulls[2] = { false };
	TsObservEventSpan span;
	int from = 0;
	while (ts_observ_next_event(&snap, from, &span))
	{
		from = span.start + span.count;

		if (filter.count > 0 && !ts_observ_event_matches(&snap, &span, &filter))
			continue;

		/* Find the target key in this event. */
		for (int i = 0; i < span.count; i++)
		{
			const TsObservTuple *t = &snap.tuples[span.start + i];
			if (ts_observ_get_key_id(t->id) != target_key)
				continue;

			values[0] =
				TimestampTzGetDatum(ts_observ_vts_to_timestamp(span.vts, snap.base_timestamp));
			values[1] = Float8GetDatum(t->value);
			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			break; /* one value per event */
		}
	}

	if (snap.tuples)
		pfree(snap.tuples);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ts_observ_log_events);

Datum
ts_observ_log_events(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Jsonb *filter_arg = PG_ARGISNULL(0) ? NULL : PG_GETARG_JSONB_P(0);
	TimestampTz since = PG_ARGISNULL(1) ? 0 : PG_GETARG_TIMESTAMPTZ(1);

	InitMaterializedSRF(fcinfo, 0);
	TupleDesc tupdesc = rsinfo->setDesc;
	Tuplestorestate *tupstore = rsinfo->setResult;

	TsObservSegment *seg = ts_observ_get_segment();
	if (seg == NULL)
		PG_RETURN_VOID();

	TsObservFilter filter;
	ts_observ_parse_filter(filter_arg, &filter);

	TsObservSnapshot snap = ts_observ_take_snapshot(seg, since);

	Datum values[4];
	bool nulls[4] = { false };
	TsObservEventSpan span;
	int from = 0;
	while (ts_observ_next_event(&snap, from, &span))
	{
		from = span.start + span.count;

		if (filter.count > 0 && !ts_observ_event_matches(&snap, &span, &filter))
			continue;

		/* Assemble the JSONB payload. */
		JsonbParseState *state = NULL;
		pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

		int32 event_type_id = -1;
		for (int i = 0; i < span.count; i++)
		{
			const TsObservTuple *t = &snap.tuples[span.start + i];
			uint16 kid = ts_observ_get_key_id(t->id);
			const TsObservKeyDef *def = ts_observ_key_by_id(kid);
			if (def == NULL)
				continue;

			if (kid == TS_KEY_EVENT_TYPE)
				event_type_id = (int32) t->value;

			JsonbValue k, v;
			k.type = jbvString;
			k.val.string.val = (char *) def->name;
			k.val.string.len = strlen(def->name);
			pushJsonbValue(&state, WJB_KEY, &k);

			v.type = jbvNumeric;
			v.val.numeric =
				DatumGetNumeric(DirectFunctionCall1(float8_numeric, Float8GetDatum(t->value)));
			pushJsonbValue(&state, WJB_VALUE, &v);
		}
		JsonbValue *res = pushJsonbValue(&state, WJB_END_OBJECT, NULL);
		Jsonb *payload = JsonbValueToJsonb(res);

		values[0] = Int64GetDatum((int64) span.vts);
		values[1] = TimestampTzGetDatum(ts_observ_vts_to_timestamp(span.vts, snap.base_timestamp));
		/* Emit event_type as numeric text; a wrapper view can resolve names. */
		if (event_type_id >= 0)
		{
			char buf[16];
			snprintf(buf, sizeof(buf), "%d", event_type_id);
			values[2] = CStringGetTextDatum(buf);
		}
		else
		{
			nulls[2] = true;
			values[2] = (Datum) 0;
		}
		values[3] = JsonbPGetDatum(payload);
		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		nulls[2] = false;
	}

	if (snap.tuples)
		pfree(snap.tuples);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(ts_observ_reset);

Datum
ts_observ_reset(PG_FUNCTION_ARGS)
{
	TsObservSegment *seg = ts_observ_get_segment();
	if (seg == NULL)
		PG_RETURN_VOID();

	LWLockAcquire(&seg->lock, LW_EXCLUSIVE);
	pg_atomic_write_u64(&seg->next_seqno, 0);
	pg_atomic_write_u64(&seg->last_event_ts, 0);
	LWLockRelease(&seg->lock);

	PG_RETURN_VOID();
}
