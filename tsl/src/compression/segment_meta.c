
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <utils/sortsupport.h>
#include <utils/typcache.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <libpq/pqformat.h>

#include "segment_meta.h"
#include "datum_serialize.h"

typedef struct SegmentMetaMinMaxBuilder
{
	Oid type_oid;
	bool empty;
	bool has_null;

	SortSupportData ssup;
	bool type_by_val;
	int16 type_len;
	Datum min;
	Datum max;
} SegmentMetaMinMaxBuilder;

typedef enum SegmentMetaMinMaxVersion
{
	/* Not a real version, if this does get used, it's a bug in the code */
	_INVALID_SEGMENT_MIN_MAX_VERSION = 0,

	SEGMENT_SEGMENT_MIN_MAX_V1,

	/* end of real values */
	_MAX_SEGMENT_MIN_MAX_VERSION = 128,
} SegmentMetaMinMaxVersion;

typedef enum SegmentMetaMinMaxFlags
{
	/* Has nulls allows us to optimize IS NULL quals */
	HAS_NULLS = (1 << 0),

	/* All Nulls should result in a NULL value for entire SegmentMetaMinMax */
	_MAX_FLAG = (1 << 8),
} SegmentMetaMinMaxFlags;

/* start must be aligned according to the alignment of the stored type */
typedef struct SegmentMetaMinMax
{
	char vl_len_[4];
	uint8 version; /*  SegmentMetaMinMaxVersion */
	uint8 flags;   /* SegmentMetaMinMaxFlags */
	char padding[2];
	Oid type;
	/* optional alignment padding for type */
	/*char data[FLEXIBLE_ARRAY_MEMBER]; bound values for two datums with alignment padding in
	 * between. First datum is min, second is max. Size determined by the datum type or the VARLENA
	 * header */
} SegmentMetaMinMax;

SegmentMetaMinMaxBuilder *
segment_meta_min_max_builder_create(Oid type_oid, Oid collation)
{
	SegmentMetaMinMaxBuilder *builder = palloc(sizeof(*builder));
	TypeCacheEntry *type = lookup_type_cache(type_oid, TYPECACHE_LT_OPR);

	if (!OidIsValid(type->lt_opr))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify an less-than operator for type %s",
						format_type_be(type_oid))));

	*builder = (SegmentMetaMinMaxBuilder){
		.type_oid = type_oid,
		.empty = true,
		.has_null = false,
		.type_by_val = type->typbyval,
		.type_len = type->typlen,
	};

	builder->ssup.ssup_cxt = CurrentMemoryContext;
	builder->ssup.ssup_collation = collation;
	builder->ssup.ssup_nulls_first = false;

	PrepareSortSupportFromOrderingOp(type->lt_opr, &builder->ssup);

	return builder;
}

void
segment_meta_min_max_builder_update_val(SegmentMetaMinMaxBuilder *builder, Datum val)
{
	int cmp;

	if (builder->empty)
	{
		builder->min = datumCopy(val, builder->type_by_val, builder->type_len);
		builder->max = datumCopy(val, builder->type_by_val, builder->type_len);
		builder->empty = false;
		return;
	}

	cmp = ApplySortComparator(builder->min, false, val, false, &builder->ssup);
	if (cmp > 0)
		builder->min = datumCopy(val, builder->type_by_val, builder->type_len);

	cmp = ApplySortComparator(builder->max, false, val, false, &builder->ssup);
	if (cmp < 0)
		builder->max = datumCopy(val, builder->type_by_val, builder->type_len);
}

void
segment_meta_min_max_builder_update_null(SegmentMetaMinMaxBuilder *builder)
{
	builder->has_null = true;
}

static void
segment_meta_min_max_builder_reset(SegmentMetaMinMaxBuilder *builder)
{
	if (!builder->empty)
	{
		if (!builder->type_by_val)
		{
			pfree(DatumGetPointer(builder->min));
			pfree(DatumGetPointer(builder->max));
		}
		builder->min = 0;
		builder->max = 0;
	}
	builder->empty = true;
	builder->has_null = false;
}

SegmentMetaMinMax *
segment_meta_min_max_builder_finish(SegmentMetaMinMaxBuilder *builder)
{
	SegmentMetaMinMax *res;
	uint8 flags = 0;
	Size total_size = sizeof(*res);
	DatumSerializer *serializer;
	char *data;

	if (builder->empty)
		return NULL;

	serializer = create_datum_serializer(builder->type_oid);

	if (builder->has_null)
		flags |= HAS_NULLS;

	if (builder->type_len == -1)
	{
		/* detoast if necessary. should never store toast pointers */
		builder->min = PointerGetDatum(PG_DETOAST_DATUM_PACKED(builder->min));
		builder->max = PointerGetDatum(PG_DETOAST_DATUM_PACKED(builder->max));
	}

	total_size = datum_get_bytes_size(serializer, total_size, builder->min);
	total_size = datum_get_bytes_size(serializer, total_size, builder->max);

	res = palloc0(total_size);
	*res = (SegmentMetaMinMax){
		.version = SEGMENT_SEGMENT_MIN_MAX_V1,
		.flags = flags,
		.type = builder->type_oid,
	};

	SET_VARSIZE(res, total_size);

	data = (char *) res + sizeof(*res);
	total_size -= sizeof(*res);

	data = datum_to_bytes_and_advance(serializer, data, &total_size, builder->min);
	data = datum_to_bytes_and_advance(serializer, data, &total_size, builder->max);

	Assert(total_size == 0);

	return res;
}

SegmentMetaMinMax *
segment_meta_min_max_builder_finish_and_reset(SegmentMetaMinMaxBuilder *builder)
{
	SegmentMetaMinMax *res = segment_meta_min_max_builder_finish(builder);
	segment_meta_min_max_builder_reset(builder);
	return res;
}

static void
segment_meta_min_max_get_deconstruct(SegmentMetaMinMax *meta, DatumDeserializer *deser, Datum *min,
									 Datum *max)
{
	char *data = (char *) meta + sizeof(*meta);
	/* skip the min */
	*min = bytes_to_datum_and_advance(deser, &data);
	*max = bytes_to_datum_and_advance(deser, &data);
}

bytea *
segment_meta_min_max_to_binary_string(SegmentMetaMinMax *meta)
{
	StringInfoData buf;
	DatumDeserializer *deser = create_datum_deserializer(meta->type);
	DatumSerializer *ser = create_datum_serializer(meta->type);
	Datum min, max;

	segment_meta_min_max_get_deconstruct(meta, deser, &min, &max);
	pq_begintypsend(&buf);
	pq_sendbyte(&buf, meta->version);
	pq_sendbyte(&buf, meta->flags);
	type_append_to_binary_string(meta->type, &buf);

	datum_append_to_binary_string(ser, &buf, min);
	datum_append_to_binary_string(ser, &buf, max);

	return pq_endtypsend(&buf);
}

SegmentMetaMinMax *
segment_meta_min_max_from_binary_string(StringInfo buf)
{
	uint8 version = pq_getmsgbyte(buf);

	if (version == SEGMENT_SEGMENT_MIN_MAX_V1)
	{
		uint8 flags = pq_getmsgbyte(buf);
		Oid type_oid = binary_string_get_type(buf);
		DatumDeserializer *deser = create_datum_deserializer(type_oid);
		TypeCacheEntry *type = lookup_type_cache(type_oid, 0);
		SegmentMetaMinMaxBuilder builder = (SegmentMetaMinMaxBuilder){
			.type_oid = type_oid,
			.empty = false,
			.has_null = (flags & HAS_NULLS) != 0,
			.type_by_val = type->typbyval,
			.type_len = type->typlen,
			.min = binary_string_to_datum(deser, buf),
			.max = binary_string_to_datum(deser, buf),
		};

		return segment_meta_min_max_builder_finish(&builder);
	}
	else
		elog(ERROR, "Unknown version number for segment meta min max: %d", version);
}

Datum
tsl_segment_meta_min_max_get_min(Datum meta_datum, Oid type)
{
	SegmentMetaMinMax *meta = (SegmentMetaMinMax *) PG_DETOAST_DATUM(meta_datum);
	DatumDeserializer *deser;
	Datum min, max;

	if (type != meta->type)
		elog(ERROR, "wrong type requested from segment_meta_min_max");

	deser = create_datum_deserializer(meta->type);
	segment_meta_min_max_get_deconstruct(meta, deser, &min, &max);
	return min;
}

Datum
tsl_segment_meta_min_max_get_max(Datum meta_datum, Oid type)
{
	SegmentMetaMinMax *meta = (SegmentMetaMinMax *) PG_DETOAST_DATUM(meta_datum);
	DatumDeserializer *deser = create_datum_deserializer(meta->type);
	Datum min, max;

	if (type != meta->type)
		elog(ERROR, "wrong type requested from segment_meta_min_max");
	segment_meta_min_max_get_deconstruct(meta, deser, &min, &max);
	return max;
}

bool
tsl_segment_meta_min_max_has_null(Datum meta_datum)
{
	SegmentMetaMinMax *meta = (SegmentMetaMinMax *) PG_DETOAST_DATUM(meta_datum);
	return (meta->flags & HAS_NULLS) != 0;
}
