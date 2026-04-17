/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include "ts_observ_agg.h"
#include "ts_observ_write.h"

#include <postgres.h>
#include <float.h>
#include <math.h>
#include <miscadmin.h>
#include <storage/ipc.h>
#include <utils/memutils.h>
#include <utils/timestamp.h>

/*
 * State is a tagged union. Scalar types use the inline double — no allocation.
 * Complex types heap-allocate a buffer via init_fn / free_fn.
 */
typedef union TsObservAggState
{
	double scalar;
	void *extended;
} TsObservAggState;

/*
 * Aggregate operations. For scalar types, the free_fn is not needed and may be NULL.
 */
typedef void (*TsObservAggInitFn)(TsObservAggState *state);
typedef void (*TsObservAggUpdateFn)(TsObservAggState *state, double value);
typedef double (*TsObservAggFinalizeFn)(const TsObservAggState *state, int64 count);
typedef void (*TsObservAggFreeFn)(TsObservAggState *state);

typedef struct TsObservAggOps
{
	TsObservAggInitFn init_fn;
	TsObservAggUpdateFn update_fn;
	TsObservAggFinalizeFn finalize_fn;
	TsObservAggFreeFn free_fn; /* NULL for scalar types */
} TsObservAggOps;

typedef struct TsObservAggDescriptor
{
	uint16 key_id;
	TsObservAggType agg_type;
	TsObservAggState state;
} TsObservAggDescriptor;

typedef struct TsObservAggSlot
{
	bool active;
	int64 add_count;
	TimestampTz start_time;
	TimestampTz last_add_time;

	TsObservKV identity[TS_OBSERV_MAX_AGG_DESCRIPTORS];
	int num_identity;

	TsObservAggDescriptor descriptors[TS_OBSERV_MAX_AGG_DESCRIPTORS];
	int num_descriptors;
} TsObservAggSlot;

static TsObservAggSlot agg_slots[TS_OBSERV_MAX_AGG_SLOTS];
static uint32 agg_next_id = 0;

/* Scalar aggregate implementations */

static void
agg_init_sum(TsObservAggState *s)
{
	s->scalar = 0.0;
}
static void
agg_update_sum(TsObservAggState *s, double v)
{
	s->scalar += v;
}
static double
agg_finalize_sum(const TsObservAggState *s, int64 c)
{
	return s->scalar;
}

static void
agg_init_min(TsObservAggState *s)
{
	s->scalar = DBL_MAX;
}
static void
agg_update_min(TsObservAggState *s, double v)
{
	if (v < s->scalar)
		s->scalar = v;
}
static double
agg_finalize_min(const TsObservAggState *s, int64 c)
{
	return s->scalar;
}

static void
agg_init_max(TsObservAggState *s)
{
	s->scalar = -DBL_MAX;
}
static void
agg_update_max(TsObservAggState *s, double v)
{
	if (v > s->scalar)
		s->scalar = v;
}
static double
agg_finalize_max(const TsObservAggState *s, int64 c)
{
	return s->scalar;
}

static void
agg_init_sumsq(TsObservAggState *s)
{
	s->scalar = 0.0;
}
static void
agg_update_sumsq(TsObservAggState *s, double v)
{
	s->scalar += v * v;
}
static double
agg_finalize_sumsq(const TsObservAggState *s, int64 c)
{
	return s->scalar;
}

static void
agg_init_last(TsObservAggState *s)
{
	s->scalar = 0.0;
}
static void
agg_update_last(TsObservAggState *s, double v)
{
	s->scalar = v;
}
static double
agg_finalize_last(const TsObservAggState *s, int64 c)
{
	return s->scalar;
}

static void
agg_init_first(TsObservAggState *s)
{
	s->scalar = 0.0;
}
static void
agg_update_first(TsObservAggState *s, double v)
{ /* captured in first add() */
}
static double
agg_finalize_first(const TsObservAggState *s, int64 c)
{
	return s->scalar;
}

static void
agg_init_avg(TsObservAggState *s)
{
	s->scalar = 0.0;
}
static void
agg_update_avg(TsObservAggState *s, double v)
{
	s->scalar += v;
}
static double
agg_finalize_avg(const TsObservAggState *s, int64 c)
{
	return c > 0 ? s->scalar / c : 0.0;
}

/* STDDEV (extended state: sum + sum_sq) */
typedef struct AggStateStddev
{
	double sum;
	double sum_sq;
} AggStateStddev;

static void
agg_init_stddev(TsObservAggState *s)
{
	AggStateStddev *st = palloc(sizeof(AggStateStddev));
	st->sum = 0.0;
	st->sum_sq = 0.0;
	s->extended = st;
}

static void
agg_update_stddev(TsObservAggState *s, double v)
{
	AggStateStddev *st = (AggStateStddev *) s->extended;
	st->sum += v;
	st->sum_sq += v * v;
}

static double
agg_finalize_stddev(const TsObservAggState *s, int64 count)
{
	if (count < 2)
		return 0.0;
	AggStateStddev *st = (AggStateStddev *) s->extended;
	double mean = st->sum / count;
	double variance = st->sum_sq / count - mean * mean;
	return sqrt(Max(variance, 0.0)); /* guard against fp noise */
}

static void
agg_free_stddev(TsObservAggState *s)
{
	if (s->extended)
		pfree(s->extended);
	s->extended = NULL;
}

/* Ops lookup table */

static const TsObservAggOps agg_ops[] = {
	[TS_OBSERV_AGG_SUM] = { agg_init_sum, agg_update_sum, agg_finalize_sum, NULL },
	[TS_OBSERV_AGG_MIN] = { agg_init_min, agg_update_min, agg_finalize_min, NULL },
	[TS_OBSERV_AGG_MAX] = { agg_init_max, agg_update_max, agg_finalize_max, NULL },
	[TS_OBSERV_AGG_SUMSQ] = { agg_init_sumsq, agg_update_sumsq, agg_finalize_sumsq, NULL },
	[TS_OBSERV_AGG_LAST] = { agg_init_last, agg_update_last, agg_finalize_last, NULL },
	[TS_OBSERV_AGG_FIRST] = { agg_init_first, agg_update_first, agg_finalize_first, NULL },
	[TS_OBSERV_AGG_AVG] = { agg_init_avg, agg_update_avg, agg_finalize_avg, NULL },
	[TS_OBSERV_AGG_STDDEV] = { agg_init_stddev,
							   agg_update_stddev,
							   agg_finalize_stddev,
							   agg_free_stddev },
};

/* Slot lifecycle helpers */

static void
ts_observ_agg_free_slot(TsObservAggSlot *slot)
{
	for (int i = 0; i < slot->num_descriptors; i++)
	{
		TsObservAggDescriptor *desc = &slot->descriptors[i];
		const TsObservAggOps *ops = &agg_ops[desc->agg_type];
		if (ops->free_fn)
			ops->free_fn(&desc->state);
	}
	slot->active = false;
}

/* Public API */

TsObservAggID
ts_observ_agg_start(const TsObservKV *identity_kvs, int num_identity)
{
	uint32 id = agg_next_id++ % TS_OBSERV_MAX_AGG_SLOTS;
	uint32 max_id = (id + TS_OBSERV_MAX_AGG_SLOTS) % TS_OBSERV_MAX_AGG_SLOTS;

	while (agg_slots[id].active)
	{
		id = (id + 1) % TS_OBSERV_MAX_AGG_SLOTS;
		if (id == max_id)
		{
			elog(WARNING, "ts_observ: no free aggregate slots available");
			return TS_OBSERV_AGG_INVALID_ID;
		}
	}

	TsObservAggSlot *slot = &agg_slots[id];
	memset(slot, 0, sizeof(*slot));
	slot->active = true;
	slot->start_time = GetCurrentTimestamp();

	Assert(num_identity <= TS_OBSERV_MAX_AGG_DESCRIPTORS);
	if (num_identity > TS_OBSERV_MAX_AGG_DESCRIPTORS)
	{
		num_identity = TS_OBSERV_MAX_AGG_DESCRIPTORS;
		elog(WARNING, "ts_observ: too many identity keys (%d), some will be ignored", num_identity);
	}
	memcpy(slot->identity, identity_kvs, num_identity * sizeof(TsObservKV));
	slot->num_identity = num_identity;
	return (TsObservAggID){ .id = id };
}

void
ts_observ_agg_add_kv(const TsObservAggID agg_id, const TsObservKV kv)
{
	Assert(agg_id.id < TS_OBSERV_MAX_AGG_SLOTS);
	if (agg_id.id >= TS_OBSERV_MAX_AGG_SLOTS)
	{
		return;
	}

	TsObservAggSlot *slot = &agg_slots[agg_id.id];
	Assert(slot->active);

	/* This is not critical functionality, just return if the slot is inactive */
	if (!slot->active)
	{
		return;
	}

	int num_identity = slot->num_identity;

	Assert(num_identity + 1 <= TS_OBSERV_MAX_AGG_DESCRIPTORS);
	if (num_identity + 1 > TS_OBSERV_MAX_AGG_DESCRIPTORS)
	{
		elog(WARNING,
			 "ts_observ: too many identity keys (%d), some will be ignored",
			 num_identity + 1);
		return;
	}

	/* Update identity key if already exists. */
	for (int i = 0; i < num_identity; i++)
	{
		if (slot->identity[i].key_id == kv.key_id)
		{
			slot->identity[i].value = kv.value;
			return;
		}
	}

	slot->identity[num_identity] = kv;
	slot->num_identity++;
}

void
ts_observ_agg_describe(TsObservAggID agg_id, uint16 key_id, TsObservAggType agg_type)
{
	Assert(agg_id.id < TS_OBSERV_MAX_AGG_SLOTS);
	if (agg_id.id >= TS_OBSERV_MAX_AGG_SLOTS)
	{
		return;
	}

	TsObservAggSlot *slot = &agg_slots[agg_id.id];
	Assert(slot->active);
	Assert(slot->num_descriptors < TS_OBSERV_MAX_AGG_DESCRIPTORS);

	/* This is not critical functionality, just return if the slot is inactive or full */
	if (!slot->active || slot->num_descriptors >= TS_OBSERV_MAX_AGG_DESCRIPTORS)
	{
		return;
	}

	const TsObservAggOps *ops = &agg_ops[agg_type];
	TsObservAggDescriptor *desc = &slot->descriptors[slot->num_descriptors++];
	desc->key_id = key_id;
	desc->agg_type = agg_type;
	ops->init_fn(&desc->state);
}

void
ts_observ_agg_add(TsObservAggID agg_id, double value)
{
	TsObservAggSlot *slot = &agg_slots[agg_id.id];
	Assert(slot->active);

	/* This is not critical functionality, just return if the slot is inactive */
	if (!slot->active)
		return;

	/* FIRST captures its value on the first add. */
	if (slot->add_count == 0)
	{
		for (int i = 0; i < slot->num_descriptors; i++)
		{
			if (slot->descriptors[i].agg_type == TS_OBSERV_AGG_FIRST)
				slot->descriptors[i].state.scalar = value;
		}
	}

	slot->add_count++;
	slot->last_add_time = GetCurrentTimestamp();

	for (int i = 0; i < slot->num_descriptors; i++)
	{
		TsObservAggDescriptor *desc = &slot->descriptors[i];
		const TsObservAggOps *ops = &agg_ops[desc->agg_type];
		ops->update_fn(&desc->state, value);
	}
}

void
ts_observ_agg_finish(TsObservAggID agg_id)
{
	TsObservAggSlot *slot = &agg_slots[agg_id.id];
	Assert(slot->active);

	if (slot->add_count == 0)
	{
		ts_observ_agg_free_slot(slot);
		return;
	}

	int max_kvs = slot->num_identity + 3 + slot->num_descriptors;
	TsObservKV *kvs = palloc(max_kvs * sizeof(TsObservKV));
	int n = 0;

	/* 1. Identity keys (from start). */
	for (int i = 0; i < slot->num_identity; i++)
		kvs[n++] = slot->identity[i];

	/* 2. Auto-metadata. */
	kvs[n++] = (TsObservKV){ TS_KEY_AGG_COUNT, (double) slot->add_count };
	kvs[n++] = (TsObservKV){ TS_KEY_AGG_START, (double) slot->start_time };
	kvs[n++] = (TsObservKV){ TS_KEY_AGG_END, (double) slot->last_add_time };

	/* 3. Finalized aggregates. */
	for (int i = 0; i < slot->num_descriptors; i++)
	{
		TsObservAggDescriptor *desc = &slot->descriptors[i];
		const TsObservAggOps *ops = &agg_ops[desc->agg_type];
		double final_value = ops->finalize_fn(&desc->state, slot->add_count);
		kvs[n++] = (TsObservKV){ desc->key_id, final_value };
	}

	ts_observ_emit(kvs, n);
	pfree(kvs);

	ts_observ_agg_free_slot(slot);
}

void
ts_observ_agg_cancel(TsObservAggID agg_id)
{
	TsObservAggSlot *slot = &agg_slots[agg_id.id];
	ts_observ_agg_free_slot(slot);
}
