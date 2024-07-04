/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include <postgres.h>
#include <catalog/pg_am.h>
#include <parser/parse_coerce.h>
#include <parser/parse_relation.h>
#include <utils/typcache.h>

#include "compression.h"
#include "compression_dml.h"
#include "create.h"
#include "ts_catalog/array_utils.h"

static Oid deduce_filter_subtype(BatchFilter *filter, Oid att_typoid);

/*
 * Build scankeys for decompressed tuple to check if it is part of the batch.
 *
 * The key_columns are the columns of the uncompressed chunk.
 */
ScanKeyData *
build_scankeys_for_uncompressed(Oid ht_relid, CompressionSettings *settings, Relation out_rel,
								Bitmapset *key_columns, TupleTableSlot *slot, int *num_scankeys)
{
	ScanKeyData *scankeys = NULL;
	int key_index = 0;
	TupleDesc out_desc = RelationGetDescr(out_rel);

	if (bms_is_empty(key_columns))
	{
		*num_scankeys = key_index;
		return scankeys;
	}

	scankeys = palloc(sizeof(ScanKeyData) * bms_num_members(key_columns));

	int i = -1;
	while ((i = bms_next_member(key_columns, i)) > 0)
	{
		AttrNumber attno = i + FirstLowInvalidHeapAttributeNumber;
		bool isnull;

		/*
		 * slot has the physical layout of the hypertable, so we need to
		 * get the attribute number of the hypertable for the column.
		 */
		char *attname = get_attname(out_rel->rd_id, attno, false);

		/*
		 * We can skip any segmentby columns here since they have already been
		 * checked during batch filtering.
		 */
		if (ts_array_is_member(settings->fd.segmentby, attname))
		{
			continue;
		}

		AttrNumber ht_attno = get_attnum(ht_relid, attname);
		Datum value = slot_getattr(slot, ht_attno, &isnull);

		Oid atttypid = out_desc->attrs[AttrNumberGetAttrOffset(attno)].atttypid;
		TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_BTREE_OPFAMILY);

		/*
		 * Should never happen since the column is part of unique constraint
		 * and should therefore have the required opfamily
		 */
		if (!OidIsValid(tce->btree_opf))
			elog(ERROR, "no btree opfamily for type \"%s\"", format_type_be(atttypid));

		Oid opr = get_opfamily_member(tce->btree_opf, atttypid, atttypid, BTEqualStrategyNumber);

		/*
		 * Fall back to btree operator input type when it is binary compatible with
		 * the column type and no operator for column type could be found.
		 */
		if (!OidIsValid(opr) && IsBinaryCoercible(atttypid, tce->btree_opintype))
		{
			opr = get_opfamily_member(tce->btree_opf,
									  tce->btree_opintype,
									  tce->btree_opintype,
									  BTEqualStrategyNumber);
		}

		if (!OidIsValid(opr))
			elog(ERROR, "no operator found for type \"%s\"", format_type_be(atttypid));

		ScanKeyEntryInitialize(&scankeys[key_index++],
							   isnull ? SK_ISNULL | SK_SEARCHNULL : 0,
							   attno,
							   BTEqualStrategyNumber,
							   InvalidOid,
							   out_desc->attrs[AttrNumberGetAttrOffset(attno)].attcollation,
							   get_opcode(opr),
							   isnull ? 0 : value);
	}

	*num_scankeys = key_index;
	return scankeys;
}

/*
 * Build scankeys for decompression of specific batches. key_columns references the
 * columns of the uncompressed chunk.
 */
ScanKeyData *
build_heap_scankeys(Oid hypertable_relid, Relation in_rel, Relation out_rel,
					CompressionSettings *settings, Bitmapset *key_columns, Bitmapset **null_columns,
					TupleTableSlot *slot, int *num_scankeys)
{
	int key_index = 0;
	ScanKeyData *scankeys = NULL;

	if (!bms_is_empty(key_columns))
	{
		scankeys = palloc0(bms_num_members(key_columns) * 2 * sizeof(ScanKeyData));
		int i = -1;
		while ((i = bms_next_member(key_columns, i)) > 0)
		{
			AttrNumber attno = i + FirstLowInvalidHeapAttributeNumber;
			char *attname = get_attname(out_rel->rd_id, attno, false);
			bool isnull;
			AttrNumber ht_attno = get_attnum(hypertable_relid, attname);

			/*
			 * This is a not very precise but easy assertion to detect attno
			 * mismatch at least in some cases. The mismatch might happen if the
			 * hypertable and chunk layout are different because of dropped
			 * columns, and we're using a wrong slot type here.
			 */
			PG_USED_FOR_ASSERTS_ONLY Oid ht_atttype = get_atttype(hypertable_relid, ht_attno);
			PG_USED_FOR_ASSERTS_ONLY Oid slot_atttype =
				slot->tts_tupleDescriptor->attrs[AttrNumberGetAttrOffset(ht_attno)].atttypid;
			Assert(ht_atttype == slot_atttype);

			Datum value = slot_getattr(slot, ht_attno, &isnull);

			/*
			 * There are 3 possible scenarios we have to consider
			 * when dealing with columns which are part of unique
			 * constraints.
			 *
			 * 1. Column is segmentby-Column
			 * In this case we can add a single ScanKey with an
			 * equality check for the value.
			 * 2. Column is orderby-Column
			 * In this we can add 2 ScanKeys with range constraints
			 * utilizing batch metadata.
			 * 3. Column is neither segmentby nor orderby
			 * In this case we cannot utilize this column for
			 * batch filtering as the values are compressed and
			 * we have no metadata.
			 */
			if (ts_array_is_member(settings->fd.segmentby, attname))
			{
				key_index = create_segment_filter_scankey(in_rel,
														  attname,
														  BTEqualStrategyNumber,
														  InvalidOid,
														  scankeys,
														  key_index,
														  null_columns,
														  value,
														  isnull,
														  false);
			}
			if (ts_array_is_member(settings->fd.orderby, attname))
			{
				/* Cannot optimize orderby columns with NULL values since those
				 * are not visible in metadata
				 */
				if (isnull)
					continue;

				int16 index = ts_array_position(settings->fd.orderby, attname);

				key_index = create_segment_filter_scankey(in_rel,
														  column_segment_min_name(index),
														  BTLessEqualStrategyNumber,
														  InvalidOid,
														  scankeys,
														  key_index,
														  null_columns,
														  value,
														  false,
														  false); /* is_null_check */
				key_index = create_segment_filter_scankey(in_rel,
														  column_segment_max_name(index),
														  BTGreaterEqualStrategyNumber,
														  InvalidOid,
														  scankeys,
														  key_index,
														  null_columns,
														  value,
														  false,
														  false); /* is_null_check */
			}
		}
	}

	*num_scankeys = key_index;
	return scankeys;
}

/*
 * This method will build scan keys required to do index
 * scans on compressed chunks.
 */
ScanKeyData *
build_index_scankeys(Relation index_rel, List *index_filters, int *num_scankeys)
{
	ListCell *lc;
	BatchFilter *filter = NULL;
	*num_scankeys = list_length(index_filters);
	ScanKeyData *scankey = palloc0(sizeof(ScanKeyData) * (*num_scankeys));
	int idx = 0;
	int flags;

	/* Order scankeys based on index attribute order */
	for (int idx_attno = 1; idx_attno <= index_rel->rd_index->indnkeyatts && idx < *num_scankeys;
		 idx_attno++)
	{
		AttrNumber attno = index_rel->rd_index->indkey.values[AttrNumberGetAttrOffset(idx_attno)];
		char *attname = get_attname(index_rel->rd_index->indrelid, attno, false);
		Oid typoid = attnumTypeId(index_rel, idx_attno);
		foreach (lc, index_filters)
		{
			filter = lfirst(lc);
			if (!strcmp(attname, NameStr(filter->column_name)))
			{
				flags = 0;
				if (filter->is_null_check)
				{
					flags = SK_ISNULL | (filter->is_null ? SK_SEARCHNULL : SK_SEARCHNOTNULL);
				}
				if (filter->is_array_op)
				{
					flags |= SK_SEARCHARRAY;
				}

				ScanKeyEntryInitialize(&scankey[idx++],
									   flags,
									   idx_attno,
									   filter->strategy,
									   deduce_filter_subtype(filter, typoid), /* subtype */
									   filter->collation,
									   filter->opcode,
									   filter->value ? filter->value->constvalue : 0);
				break;
			}
		}
	}

	Assert(idx == *num_scankeys);
	return scankey;
}

/* This method is used to find matching index on compressed chunk
 * and build scan keys from the slot data
 */
ScanKeyData *
build_index_scankeys_using_slot(Oid hypertable_relid, Relation in_rel, Relation out_rel,
								Bitmapset *key_columns, TupleTableSlot *slot,
								Relation *result_index_rel, Bitmapset **index_columns,
								int *num_scan_keys)
{
	List *index_oids;
	ListCell *lc;
	ScanKeyData *scankeys = NULL;
	/* get list of indexes defined on compressed chunk */
	index_oids = RelationGetIndexList(in_rel);
	*num_scan_keys = 0;

	foreach (lc, index_oids)
	{
		Relation index_rel = index_open(lfirst_oid(lc), AccessShareLock);
		IndexInfo *index_info = BuildIndexInfo(index_rel);

		/* Can't use partial or expression indexes */
		if (index_info->ii_Predicate != NIL || index_info->ii_Expressions != NIL)
		{
			index_close(index_rel, AccessShareLock);
			continue;
		}

		/* Can only use Btree indexes */
		if (index_info->ii_Am != BTREE_AM_OID)
		{
			index_close(index_rel, AccessShareLock);
			continue;
		}

		/*
		 * Must have at least two attributes, index we are looking for contains
		 * at least one segmentby column and a sequence number.
		 */
		if (index_rel->rd_index->indnatts < 2)
		{
			index_close(index_rel, AccessShareLock);
			continue;
		}

		scankeys = palloc0((index_rel->rd_index->indnatts) * sizeof(ScanKeyData));

		/*
		 * 	Using only key attributes to exclude covering columns
		 * 	only interested in filtering here
		 */
		for (int i = 0; i < index_rel->rd_index->indnkeyatts; i++)
		{
			AttrNumber idx_attnum = AttrOffsetGetAttrNumber(i);
			AttrNumber in_attnum = index_rel->rd_index->indkey.values[i];
			const NameData *attname = attnumAttName(in_rel, in_attnum);

			/* Make sure we find columns in key columns in order to select the right index */
			if (!bms_is_member(get_attnum(out_rel->rd_id, NameStr(*attname)), key_columns))
			{
				break;
			}

			bool isnull;
			AttrNumber ht_attno = get_attnum(hypertable_relid, NameStr(*attname));
			Datum value = slot_getattr(slot, ht_attno, &isnull);

			if (isnull)
			{
				ScanKeyEntryInitialize(&scankeys[(*num_scan_keys)++],
									   SK_ISNULL | SK_SEARCHNULL,
									   idx_attnum,
									   InvalidStrategy, /* no strategy */
									   InvalidOid,		/* no strategy subtype */
									   InvalidOid,		/* no collation */
									   InvalidOid,		/* no reg proc for this */
									   (Datum) 0);		/* constant */
				continue;
			}

			Oid atttypid = attnumTypeId(index_rel, idx_attnum);

			TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_BTREE_OPFAMILY);
			if (!OidIsValid(tce->btree_opf))
				elog(ERROR, "no btree opfamily for type \"%s\"", format_type_be(atttypid));

			Oid opr =
				get_opfamily_member(tce->btree_opf, atttypid, atttypid, BTEqualStrategyNumber);

			/*
			 * Fall back to btree operator input type when it is binary compatible with
			 * the column type and no operator for column type could be found.
			 */
			if (!OidIsValid(opr) && IsBinaryCoercible(atttypid, tce->btree_opintype))
			{
				opr = get_opfamily_member(tce->btree_opf,
										  tce->btree_opintype,
										  tce->btree_opintype,
										  BTEqualStrategyNumber);
			}

			/* No operator could be found so we can't create the scankey. */
			if (!OidIsValid(opr))
				continue;

			Oid opcode = get_opcode(opr);
			Ensure(OidIsValid(opcode),
				   "no opcode found for column operator of a hypertable column");

			ScanKeyEntryInitialize(&scankeys[(*num_scan_keys)++],
								   0, /* flags */
								   idx_attnum,
								   BTEqualStrategyNumber,
								   InvalidOid, /* No strategy subtype. */
								   attnumCollationId(index_rel, idx_attnum),
								   opcode,
								   value);
		}

		if (*num_scan_keys > 0)
		{
			*result_index_rel = index_rel;
			break;
		}
		else
		{
			index_close(index_rel, AccessShareLock);
			pfree(scankeys);
			scankeys = NULL;
		}
	}

	return scankeys;
}

/*
 * This method will build scan keys for predicates including
 * SEGMENT BY column with attribute number from compressed chunk
 * if condition is like <segmentbycol> = <const value>, else
 * OUT param null_columns is saved with column attribute number.
 */
ScanKeyData *
build_update_delete_scankeys(Relation in_rel, List *heap_filters, int *num_scankeys,
							 Bitmapset **null_columns)
{
	ListCell *lc;
	BatchFilter *filter;
	int key_index = 0;

	ScanKeyData *scankeys = palloc0(heap_filters->length * sizeof(ScanKeyData));

	foreach (lc, heap_filters)
	{
		filter = lfirst(lc);
		AttrNumber attno = get_attnum(in_rel->rd_id, NameStr(filter->column_name));
		Oid typoid = get_atttype(in_rel->rd_id, attno);
		if (attno == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of relation \"%s\" does not exist",
							NameStr(filter->column_name),
							RelationGetRelationName(in_rel))));

		key_index = create_segment_filter_scankey(in_rel,
												  NameStr(filter->column_name),
												  filter->strategy,
												  deduce_filter_subtype(filter, typoid),
												  scankeys,
												  key_index,
												  null_columns,
												  filter->value ? filter->value->constvalue : 0,
												  filter->is_null_check,
												  filter->is_array_op);
	}
	*num_scankeys = key_index;
	return scankeys;
}

int
create_segment_filter_scankey(Relation in_rel, char *segment_filter_col_name,
							  StrategyNumber strategy, Oid subtype, ScanKeyData *scankeys,
							  int num_scankeys, Bitmapset **null_columns, Datum value,
							  bool is_null_check, bool is_array_op)
{
	AttrNumber cmp_attno = get_attnum(in_rel->rd_id, segment_filter_col_name);
	Assert(cmp_attno != InvalidAttrNumber);
	/* This should never happen but if it does happen, we can't generate a scan key for
	 * the filter column so just skip it */
	if (cmp_attno == InvalidAttrNumber)
		return num_scankeys;

	int flags = is_array_op ? SK_SEARCHARRAY : 0;

	/*
	 * In PG versions <= 14 NULL values are always considered distinct
	 * from other NULL values and therefore NULLABLE multi-columnn
	 * unique constraints might expose unexpected behaviour in the
	 * presence of NULL values.
	 * Since SK_SEARCHNULL is not supported by heap scans we cannot
	 * build a ScanKey for NOT NULL and instead have to do those
	 * checks manually.
	 */
	if (is_null_check)
	{
		*null_columns = bms_add_member(*null_columns, cmp_attno);
		return num_scankeys;
	}

	Oid atttypid = in_rel->rd_att->attrs[AttrNumberGetAttrOffset(cmp_attno)].atttypid;

	TypeCacheEntry *tce = lookup_type_cache(atttypid, TYPECACHE_BTREE_OPFAMILY);
	if (!OidIsValid(tce->btree_opf))
		elog(ERROR, "no btree opfamily for type \"%s\"", format_type_be(atttypid));

	Oid opr = get_opfamily_member(tce->btree_opf, atttypid, atttypid, strategy);

	/*
	 * Fall back to btree operator input type when it is binary compatible with
	 * the column type and no operator for column type could be found.
	 */
	if (!OidIsValid(opr) && IsBinaryCoercible(atttypid, tce->btree_opintype))
	{
		opr =
			get_opfamily_member(tce->btree_opf, tce->btree_opintype, tce->btree_opintype, strategy);
	}

	/* No operator could be found so we can't create the scankey. */
	if (!OidIsValid(opr))
		return num_scankeys;

	opr = get_opcode(opr);
	Assert(OidIsValid(opr));
	/* We should never end up here but: no opcode, no optimization */
	if (!OidIsValid(opr))
		return num_scankeys;

	ScanKeyEntryInitialize(&scankeys[num_scankeys++],
						   flags,
						   cmp_attno,
						   strategy,
						   subtype,
						   in_rel->rd_att->attrs[AttrNumberGetAttrOffset(cmp_attno)].attcollation,
						   opr,
						   value);

	return num_scankeys;
}

/*
 * Get the subtype for an indexscan from the provided filter. We also
 * need to handle array constants appropriately.
 */
static Oid
deduce_filter_subtype(BatchFilter *filter, Oid att_typoid)
{
	Oid subtype = InvalidOid;

	if (!filter->value)
		return InvalidOid;

	/*
	 * Check if the filter type is different from the att type. If yes, the
	 * subtype needs to be set appropriately.
	 */
	if (att_typoid != filter->value->consttype)
	{
		/* For an array type get its element type */
		if (filter->is_array_op)
			subtype = get_element_type(filter->value->consttype);
		else
			subtype = filter->value->consttype;
	}

	return subtype;
}
