/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <access/heapam.h>
#include <access/reloptions.h>
#include <access/tupdesc.h>
#include <access/xact.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/objectaccess.h>
#include <catalog/pg_constraint_d.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_type.h>
#include <catalog/toasting.h>
#include <commands/alter.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <parser/parse_type.h>
#include <storage/lmgr.h>
#include <tcop/utility.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "ts_catalog/catalog.h"
#include "create.h"
#include "chunk.h"
#include "chunk_index.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/compression_settings.h"
#include "ts_catalog/continuous_agg.h"
#include "compression_with_clause.h"
#include "compression.h"
#include "compression/compression_storage.h"
#include "hypertable_cache.h"
#include "custom_type_cache.h"
#include "trigger.h"
#include "utils.h"
#include "guc.h"

static void validate_hypertable_for_compression(Hypertable *ht);
static List *build_columndefs(CompressionSettings *settings, Oid src_relid);
static ColumnDef *build_columndef_singlecolumn(const char *colname, Oid typid);

static char *
compression_column_segment_metadata_name(int16 column_index, const char *type)
{
	char *buf = palloc(sizeof(char) * NAMEDATALEN);
	int ret;

	Assert(column_index > 0);
	ret =
		snprintf(buf, NAMEDATALEN, COMPRESSION_COLUMN_METADATA_PREFIX "%s_%d", type, column_index);
	if (ret < 0 || ret > NAMEDATALEN)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("bad segment metadata column name")));
	}
	return buf;
}

char *
column_segment_min_name(int16 column_index)
{
	return compression_column_segment_metadata_name(column_index,
													COMPRESSION_COLUMN_METADATA_MIN_COLUMN_NAME);
}

char *
column_segment_max_name(int16 column_index)
{
	return compression_column_segment_metadata_name(column_index,
													COMPRESSION_COLUMN_METADATA_MAX_COLUMN_NAME);
}

static void
check_segmentby(Oid relid, List *segmentby_cols)
{
	ListCell *lc;
	ArrayType *segmentby = NULL;

	foreach (lc, segmentby_cols)
	{
		char *colname = lfirst(lc);
		AttrNumber col_attno = get_attnum(relid, colname);
		if (col_attno == InvalidAttrNumber)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column \"%s\" does not exist", colname),
					 errhint("The timescaledb.compress_segmentby option must reference a valid "
							 "column.")));
		}

		const char *col_attname = get_attname(relid, col_attno, false);

		/* check if segmentby columns are distinct. */
		if (ts_array_is_member(segmentby, col_attname))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("duplicate column name \"%s\"", colname),
					 errhint("The timescaledb.compress_segmentby option must reference distinct "
							 "column.")));
		segmentby = ts_array_add_element_text(segmentby, col_attname);
	}
}

static void
check_orderby(Oid relid, List *orderby_cols, ArrayType *segmentby)
{
	ArrayType *orderby = NULL;
	ListCell *lc;

	foreach (lc, orderby_cols)
	{
		CompressedParsedCol *col = (CompressedParsedCol *) lfirst(lc);
		AttrNumber col_attno = get_attnum(relid, NameStr(col->colname));

		if (col_attno == InvalidAttrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column \"%s\" does not exist", NameStr(col->colname)),
					 errhint("The timescaledb.compress_orderby option must reference a valid "
							 "column.")));

		Oid col_type = get_atttype(relid, col_attno);
		TypeCacheEntry *type = lookup_type_cache(col_type, TYPECACHE_LT_OPR);

		if (!OidIsValid(type->lt_opr))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("invalid ordering column type %s", format_type_be(col_type)),
					 errdetail("Could not identify a less-than operator for the type.")));

		const char *col_attname = get_attname(relid, col_attno, false);

		/* check if orderby columns are distinct. */
		if (ts_array_is_member(orderby, col_attname))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("duplicate column name \"%s\"", NameStr(col->colname)),
					 errhint("The timescaledb.compress_orderby option must reference distinct "
							 "column.")));

		/* check if orderby_cols and segmentby_cols are distinct */
		if (ts_array_is_member(segmentby, col_attname))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cannot use column \"%s\" for both ordering and segmenting",
							col_attname),
					 errhint("Use separate columns for the timescaledb.compress_orderby and"
							 " timescaledb.compress_segmentby options.")));

		orderby = ts_array_add_element_text(orderby, col_attname);
	}
}

/*
 * return the columndef list for compressed hypertable.
 * we do this by getting the source hypertable's attrs,
 * 1.  validate the segmentby cols and orderby cols exists in this list and
 * 2. create the columndefs for the new compressed hypertable
 *     segmentby_cols have same datatype as the original table
 *     all other cols have COMPRESSEDDATA_TYPE type
 */
static List *
build_columndefs(CompressionSettings *settings, Oid src_relid)
{
	Oid compresseddata_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;
	ArrayType *segmentby = settings->fd.segmentby;
	List *column_defs = NIL;

	Relation rel = table_open(src_relid, AccessShareLock);
	TupleDesc tupdesc = rel->rd_att;

	for (int attno = 0; attno < tupdesc->natts; attno++)
	{
		Oid attroid = InvalidOid;
		int32 typmod = -1;
		Oid collid = 0;

		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno);
		ColumnDef *coldef;
		if (attr->attisdropped)
			continue;
		if (strncmp(NameStr(attr->attname),
					COMPRESSION_COLUMN_METADATA_PREFIX,
					strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0)
			elog(ERROR,
				 "cannot compress tables with reserved column prefix '%s'",
				 COMPRESSION_COLUMN_METADATA_PREFIX);

		bool is_segmentby = ts_array_is_member(segmentby, NameStr(attr->attname));

		if (is_segmentby)
		{
			attroid = attr->atttypid; /*segment by columns have original type */
			typmod = attr->atttypmod;
			collid = attr->attcollation;
		}
		if (!OidIsValid(attroid))
		{
			attroid = compresseddata_oid; /* default type for column */
		}
		coldef = makeColumnDef(NameStr(attr->attname), attroid, typmod, collid);
		column_defs = lappend(column_defs, coldef);
	}

	table_close(rel, AccessShareLock);

	/* additional metadata columns. */

	/* count of the number of uncompressed rows */
	column_defs = lappend(column_defs,
						  makeColumnDef(COMPRESSION_COLUMN_METADATA_COUNT_NAME,
										INT4OID,
										-1 /* typemod */,
										0 /*collation*/));
	/* sequence_num column */
	column_defs = lappend(column_defs,
						  makeColumnDef(COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME,
										INT4OID,
										-1 /* typemod */,
										0 /*collation*/));

	if (settings->fd.orderby)
	{
		Datum datum;
		bool isnull;
		int16 index = 1;
		ArrayIterator it = array_create_iterator(settings->fd.orderby, 0, NULL);
		while (array_iterate(it, &datum, &isnull))
		{
			AttrNumber col_attno = get_attnum(settings->fd.relid, TextDatumGetCString(datum));
			Oid col_type = get_atttype(settings->fd.relid, col_attno);
			TypeCacheEntry *type = lookup_type_cache(col_type, TYPECACHE_LT_OPR);

			if (!OidIsValid(type->lt_opr))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("invalid ordering column type %s", format_type_be(col_type)),
						 errdetail("Could not identify a less-than operator for the type.")));

			/* segment_meta min and max columns */
			column_defs = lappend(column_defs,
								  makeColumnDef(column_segment_min_name(index),
												col_type,
												-1 /* typemod */,
												0 /*collation*/));
			column_defs = lappend(column_defs,
								  makeColumnDef(column_segment_max_name(index),
												col_type,
												-1 /* typemod */,
												0 /*collation*/));
			index++;
		}
	}
	return column_defs;
}

/* use this api for the case when you add a single column to a table that already has
 * compression setup
 * such as ALTER TABLE xyz ADD COLUMN .....
 */
static ColumnDef *
build_columndef_singlecolumn(const char *colname, Oid typid)
{
	Oid compresseddata_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;

	if (strncmp(colname,
				COMPRESSION_COLUMN_METADATA_PREFIX,
				strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0)
		elog(ERROR,
			 "cannot compress tables with reserved column prefix '%s'",
			 COMPRESSION_COLUMN_METADATA_PREFIX);

	return makeColumnDef(colname, compresseddata_oid, -1 /*typmod*/, 0 /*collation*/);
}

/*
 * Create compress chunk for specific table.
 *
 * If table_id is InvalidOid, create a new table.
 *
 * Constraints and triggers are not created on the PG chunk table.
 * Caller is expected to do this explicitly.
 */
Chunk *
create_compress_chunk(Hypertable *compress_ht, Chunk *src_chunk, Oid table_id)
{
	Catalog *catalog = ts_catalog_get();
	CatalogSecurityContext sec_ctx;
	Chunk *compress_chunk;
	int namelen;
	Oid tablespace_oid;

	Assert(compress_ht->space->num_dimensions == 0);

	/* Create a new catalog entry for chunk based on the hypercube */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	compress_chunk =
		ts_chunk_create_base(ts_catalog_table_next_seq_id(catalog, CHUNK), 0, RELKIND_RELATION);
	ts_catalog_restore_user(&sec_ctx);

	compress_chunk->fd.hypertable_id = compress_ht->fd.id;
	compress_chunk->cube = src_chunk->cube;
	compress_chunk->hypertable_relid = compress_ht->main_table_relid;
	compress_chunk->constraints = ts_chunk_constraints_alloc(1, CurrentMemoryContext);
	namestrcpy(&compress_chunk->fd.schema_name, INTERNAL_SCHEMA_NAME);

	if (OidIsValid(table_id))
	{
		Relation table_rel = table_open(table_id, AccessShareLock);
		strncpy(NameStr(compress_chunk->fd.table_name),
				RelationGetRelationName(table_rel),
				NAMEDATALEN);
		table_close(table_rel, AccessShareLock);
	}
	else
	{
		/* Fail if we overflow the name limit */
		namelen = snprintf(NameStr(compress_chunk->fd.table_name),
						   NAMEDATALEN,
						   "compress%s_%d_chunk",
						   NameStr(compress_ht->fd.associated_table_prefix),
						   compress_chunk->fd.id);

		if (namelen >= NAMEDATALEN)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("invalid name \"%s\" for compressed chunk",
							NameStr(compress_chunk->fd.table_name)),
					 errdetail("The associated table prefix is too long.")));
	}

	/* Insert chunk */
	ts_chunk_insert_lock(compress_chunk, RowExclusiveLock);

	/* only add inheritable constraints. no dimension constraints */
	ts_chunk_constraints_add_inheritable_constraints(compress_chunk->constraints,
													 compress_chunk->fd.id,
													 compress_chunk->relkind,
													 compress_chunk->hypertable_relid);

	ts_chunk_constraints_insert_metadata(compress_chunk->constraints);

	/* Create the actual table relation for the chunk
	 * Note that we have to pick the tablespace here as the compressed ht doesn't have dimensions
	 * on which to base this decision. We simply pick the same tablespace as the uncompressed chunk
	 * for now.
	 */
	tablespace_oid = get_rel_tablespace(src_chunk->table_id);

	if (OidIsValid(table_id))
		compress_chunk->table_id = table_id;
	else
	{
		CompressionSettings *settings = ts_compression_settings_get(src_chunk->hypertable_relid);
		List *column_defs = build_columndefs(settings, src_chunk->table_id);
		compress_chunk->table_id =
			compression_chunk_create(src_chunk, compress_chunk, column_defs, tablespace_oid);
	}

	if (!OidIsValid(compress_chunk->table_id))
		elog(ERROR, "could not create compressed chunk table");

	/* Materialize current compression settings for this chunk */
	ts_compression_settings_materialize(src_chunk->hypertable_relid, compress_chunk->table_id);

	/* if the src chunk is not in the default tablespace, the compressed indexes
	 * should also be in a non-default tablespace. IN the usual case, this is inferred
	 * from the hypertable's and chunk's tablespace info. We do not propagate
	 * attach_tablespace settings to the compressed hypertable. So we have to explicitly
	 * pass the tablespace information here
	 */
	ts_chunk_index_create_all(compress_chunk->fd.hypertable_id,
							  compress_chunk->hypertable_relid,
							  compress_chunk->fd.id,
							  compress_chunk->table_id,
							  tablespace_oid);

	return compress_chunk;
}

/* Add  the hypertable time column to the end of the orderby list if
 * it's not already in the orderby or segmentby. */
static List *
add_time_to_order_by_if_not_included(List *orderby_cols, List *segmentby_cols, Hypertable *ht)
{
	ListCell *lc;
	const Dimension *time_dim;
	const char *time_col_name;
	bool found = false;

	time_dim = hyperspace_get_open_dimension(ht->space, 0);
	time_col_name = get_attname(ht->main_table_relid, time_dim->column_attno, false);

	foreach (lc, orderby_cols)
	{
		CompressedParsedCol *col = (CompressedParsedCol *) lfirst(lc);
		if (namestrcmp(&col->colname, time_col_name) == 0)
			found = true;
	}
	foreach (lc, segmentby_cols)
	{
		if (strncmp(lfirst(lc), time_col_name, NAMEDATALEN) == 0)
			found = true;
	}

	if (!found)
	{
		/* Add time DESC NULLS FIRST to order by list */
		CompressedParsedCol *col = palloc(sizeof(*col));
		*col = (CompressedParsedCol){
			.desc = true,
			.nullsfirst = true,
		};
		namestrcpy(&col->colname, time_col_name);
		orderby_cols = lappend(orderby_cols, col);
	}
	return orderby_cols;
}

/* returns list of constraints that need to be cloned on the compressed hypertable
 * This is limited to foreign key constraints now
 */
static List *
validate_existing_constraints(Hypertable *ht, CompressionSettings *settings)
{
	Relation pg_constr;
	SysScanDesc scan;
	ScanKeyData scankey;
	HeapTuple tuple;
	List *conlist = NIL;

	ArrayType *arr;

	pg_constr = table_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scankey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(settings->fd.relid));

	scan = systable_beginscan(pg_constr, ConstraintRelidTypidNameIndexId, true, NULL, 1, &scankey);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_constraint form = (Form_pg_constraint) GETSTRUCT(tuple);

		/*
		 * We check primary, unique, and exclusion constraints.  Move foreign
		 * key constraints over to compression table ignore triggers
		 */
		if (form->contype == CONSTRAINT_CHECK || form->contype == CONSTRAINT_TRIGGER)
			continue;
		else if (form->contype == CONSTRAINT_EXCLUSION)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("constraint %s is not supported for compression",
							NameStr(form->conname)),
					 errhint("Exclusion constraints are not supported on hypertables that are "
							 "compressed.")));
		}
		else
		{
			int j, numkeys;
			int16 *attnums;
			bool is_null;

			/* Extract the conkey array, ie, attnums of PK's columns */
			Datum adatum = heap_getattr(tuple,
										Anum_pg_constraint_conkey,
										RelationGetDescr(pg_constr),
										&is_null);
			if (is_null)
			{
				Oid oid = heap_getattr(tuple,
									   Anum_pg_constraint_oid,
									   RelationGetDescr(pg_constr),
									   &is_null);
				elog(ERROR, "null conkey for constraint %u", oid);
			}

			arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
			numkeys = ts_array_length(arr);
			if (ARR_NDIM(arr) != 1 || numkeys < 0 || ARR_HASNULL(arr) ||
				ARR_ELEMTYPE(arr) != INT2OID)
				elog(ERROR, "conkey is not a 1-D smallint array");
			attnums = (int16 *) ARR_DATA_PTR(arr);
			for (j = 0; j < numkeys; j++)
			{
				const char *attname = get_attname(settings->fd.relid, attnums[j], false);

				if (form->contype == CONSTRAINT_FOREIGN)
				{
					/* is this a segment-by column */
					if (!ts_array_is_member(settings->fd.segmentby, attname))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("column \"%s\" must be used for segmenting", attname),
								 errdetail("The foreign key constraint \"%s\" cannot be"
										   " enforced with the given compression configuration.",
										   NameStr(form->conname))));
				}
				/* is colno a segment-by or order_by column */
				else if (!form->conindid && !ts_array_is_member(settings->fd.segmentby, attname) &&
						 !ts_array_is_member(settings->fd.orderby, attname))
					ereport(WARNING,
							(errmsg("column \"%s\" should be used for segmenting or ordering",
									attname)));
			}

			if (form->contype == CONSTRAINT_FOREIGN)
			{
				Name conname = palloc0(NAMEDATALEN);
				namestrcpy(conname, NameStr(form->conname));
				conlist = lappend(conlist, conname);
			}
		}
	}

	systable_endscan(scan);
	table_close(pg_constr, AccessShareLock);

	return conlist;
}

/*
 * Validate existing indexes on the hypertable. Note that there can be indexes
 * that do not have a corresponding constraint.
 *
 * We pass in a list of indexes that we should ignore since these are checked
 * by the constraint checking above.
 */
static void
validate_existing_indexes(Hypertable *ht, CompressionSettings *settings)
{
	Relation pg_index;
	HeapTuple htup;
	ScanKeyData skey;
	SysScanDesc indscan;

	ScanKeyInit(&skey,
				Anum_pg_index_indrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(ht->main_table_relid));

	pg_index = table_open(IndexRelationId, AccessShareLock);
	indscan = systable_beginscan(pg_index, IndexIndrelidIndexId, true, NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(indscan)))
	{
		Form_pg_index index = (Form_pg_index) GETSTRUCT(htup);

		/* We can ignore indexes that are being dropped, invalid indexes,
		 * exclusion indexes, and any indexes checked by the constraint
		 * checking. We can also skip checks below if the index is not a
		 * unique index. */
		if (!index->indislive || !index->indisvalid || index->indisexclusion || !index->indisunique)
			continue;

		/* Now we check that all columns of the unique index are part of the
		 * segmentby columns. */
		for (int i = 0; i < index->indnkeyatts; i++)
		{
			int attno = index->indkey.values[i];
			if (attno == 0)
				continue; /* skip check for expression column */
			const char *attname = get_attname(ht->main_table_relid, attno, false);
			if (!ts_array_is_member(settings->fd.segmentby, attname) &&
				!ts_array_is_member(settings->fd.orderby, attname))
				ereport(WARNING,
						(errmsg("column \"%s\" should be used for segmenting or ordering",
								attname)));
		}
	}
	systable_endscan(indscan);
	table_close(pg_index, AccessShareLock);
}

static void
check_modify_compression_options(Hypertable *ht, CompressionSettings *settings,
								 WithClauseResult *with_clause_options, List *parsed_orderby_cols)
{
	bool compress_enable = DatumGetBool(with_clause_options[CompressEnabled].parsed);
	bool compressed_chunks_exist;
	bool compression_already_enabled = TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht);
	compressed_chunks_exist =
		compression_already_enabled && ts_chunk_exists_with_compression(ht->fd.id);

	if (compressed_chunks_exist)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change configuration on already compressed chunks"),
				 errdetail("There are compressed chunks that prevent changing"
						   " the existing compression configuration.")));

	/* Require both order by and segment by when altering if they were previously set because
	 * otherwise it's not clear what the default value means: does it mean leave as-is or is it an
	 * empty list.
	 * In the case where orderby is already set to the default value (time DESC),
	 * both interpretations should lead to orderby being set to the default value,
	 * so we allow skipping orderby if the default is the already set value. */
	if (compress_enable && compression_already_enabled)
	{
		if (with_clause_options[CompressOrderBy].is_default && settings->fd.orderby)
		{
			bool orderby_time_default_matches = false;
			const char *colname = NULL;
			CompressedParsedCol *parsed = NULL;
			/* If the orderby that's already set is only the time column DESC (which is the
			 default), and we pass the default again, then no need to give an error */
			if (list_length(parsed_orderby_cols) == 1)
			{
				colname = ts_array_get_element_text(settings->fd.orderby, 1);
				bool orderby_desc = ts_array_get_element_bool(settings->fd.orderby_desc, 1);
				parsed = lfirst(list_nth_cell(parsed_orderby_cols, 0));
				orderby_time_default_matches = (orderby_desc == parsed->desc);
			}

			// this is okay only if the orderby that's already set is only the time column
			// check for the same attribute name and same order (desc)
			if (!(ts_array_length(settings->fd.orderby) == 1 &&
				  list_length(parsed_orderby_cols) == 1 && parsed &&
				  (namestrcmp(&parsed->colname, colname) == 0) && orderby_time_default_matches))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("must specify a column to order by"),
						 errdetail("The timescaledb.compress_orderby option was"
								   " previously set and must also be specified"
								   " in the updated configuration.")));
		}
		if (with_clause_options[CompressSegmentBy].is_default && settings->fd.segmentby)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("must specify a column to segment by"),
					 errdetail("The timescaledb.compress_segmentby option was"
							   " previously set and must also be specified"
							   " in the updated configuration.")));
	}
}

static void
drop_existing_compression_table(Hypertable *ht)
{
	Hypertable *compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
	if (compressed == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("compressed hypertable not found"),
				 errdetail("compression was enabled on \"%s\", but its internal"
						   " compressed hypertable could not be found.",
						   NameStr(ht->fd.table_name))));

	/* need to drop the old compressed hypertable in case the segment by columns changed (and
	 * thus the column types of compressed hypertable need to change) */
	ts_hypertable_drop(compressed, DROP_RESTRICT);
	ts_hypertable_unset_compressed(ht);
}

static bool
disable_compression(Hypertable *ht, WithClauseResult *with_clause_options)
{
	bool compression_already_enabled = TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht);
	if (!with_clause_options[CompressOrderBy].is_default ||
		!with_clause_options[CompressSegmentBy].is_default)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("invalid compression configuration"),
				 errdetail("Cannot set additional compression options when"
						   " disabling compression.")));

	if (!compression_already_enabled)
		/* compression is not enabled, so just return */
		return false;

	CompressionSettings *settings = ts_compression_settings_get(ht->main_table_relid);

	/* compression is enabled. can we turn it off? */
	check_modify_compression_options(ht, settings, with_clause_options, NIL);

	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
		drop_existing_compression_table(ht);
	else
	{
		ts_hypertable_unset_compressed(ht);
	}

	ts_compression_settings_delete(ht->main_table_relid);

	return true;
}

/* Add column to internal compression table */
static void
add_column_to_compression_table(Oid relid, CompressionSettings *settings, ColumnDef *coldef)
{
	AlterTableCmd *addcol_cmd;

	/* create altertable stmt to add column to the compressed hypertable */
	// Assert(TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(compress_ht));
	addcol_cmd = makeNode(AlterTableCmd);
	addcol_cmd->subtype = AT_AddColumn;
	addcol_cmd->def = (Node *) coldef;
	addcol_cmd->missing_ok = false;

	/* alter the table and add column */
	ts_alter_table_with_event_trigger(relid, NULL, list_make1(addcol_cmd), true);
	modify_compressed_toast_table_storage(settings, list_make1(coldef), relid);
}

/* Drop column from internal compression table */
static void
drop_column_from_compression_table(Oid relid, char *name)
{
	AlterTableCmd *cmd;

	/* create altertable stmt to drop column from the compressed hypertable */
	cmd = makeNode(AlterTableCmd);
	cmd->subtype = AT_DropColumn;
	cmd->name = name;
	cmd->missing_ok = true;

	/* alter the table and drop column */
	ts_alter_table_with_event_trigger(relid, NULL, list_make1(cmd), true);
}

static bool
update_compress_chunk_time_interval(Hypertable *ht, WithClauseResult *with_clause_options)
{
	const Dimension *time_dim = hyperspace_get_open_dimension(ht->space, 0);
	Interval *compress_interval =
		ts_compress_hypertable_parse_chunk_time_interval(with_clause_options, ht);
	if (!compress_interval)
	{
		return false;
	}
	int64 compress_interval_usec =
		ts_interval_value_to_internal(IntervalPGetDatum(compress_interval), INTERVALOID);
	if (compress_interval_usec % time_dim->fd.interval_length > 0)
		elog(WARNING,
			 "compress chunk interval is not a multiple of chunk interval, you should use a "
			 "factor of chunk interval to merge as much as possible");
	return ts_hypertable_set_compress_interval(ht, compress_interval_usec);
}

static void compression_settings_update(CompressionSettings *settings,
										WithClauseResult *with_clause_options, List *segmentby_cols,
										List *orderby_cols);

/*
 * enables compression for the passed in table by
 * creating a compression hypertable with special properties
 * Note: caller should check security permissions
 *
 * Return true if compression was enabled, false otherwise.
 *
 * Steps:
 * 1. Check existing constraints on the table -> can we support them with compression?
 * 2. Create internal compression table + mark hypertable as compression enabled
 * 3. Add catalog entries to hypertable_compression to record compression settings.
 * 4. Copy constraints to internal compression table
 */
bool
tsl_process_compress_table(AlterTableCmd *cmd, Hypertable *ht,
						   WithClauseResult *with_clause_options)
{
	int32 compress_htid;
	bool compress_enable = DatumGetBool(with_clause_options[CompressEnabled].parsed);
	Oid ownerid;
	List *segmentby_cols;
	List *orderby_cols;
	CompressionSettings *settings;

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);

	validate_hypertable_for_compression(ht);

	/* Lock the uncompressed ht in exclusive mode and keep till end of txn */
	LockRelationOid(ht->main_table_relid, AccessExclusiveLock);

	/* reload info after lock */
	ht = ts_hypertable_get_by_id(ht->fd.id);

	settings = ts_compression_settings_get(ht->main_table_relid);
	if (!settings)
	{
		settings = ts_compression_settings_create(ht->main_table_relid, NULL, NULL, NULL, NULL);
	}

	/* If we are not enabling compression, we must be just altering compressed chunk interval. */
	if (with_clause_options[CompressEnabled].is_default)
	{
		return update_compress_chunk_time_interval(ht, with_clause_options);
	}
	if (!compress_enable)
	{
		return disable_compression(ht, with_clause_options);
	}
	ownerid = ts_rel_get_owner(ht->main_table_relid);
	segmentby_cols = ts_compress_hypertable_parse_segment_by(with_clause_options, ht);
	orderby_cols = ts_compress_hypertable_parse_order_by(with_clause_options, ht);
	orderby_cols = add_time_to_order_by_if_not_included(orderby_cols, segmentby_cols, ht);

	if (TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
		check_modify_compression_options(ht, settings, with_clause_options, orderby_cols);

	compression_settings_update(settings, with_clause_options, segmentby_cols, orderby_cols);

	check_segmentby(ht->main_table_relid, segmentby_cols);
	check_orderby(ht->main_table_relid, orderby_cols, settings->fd.segmentby);

	/* take explicit locks on catalog tables and keep them till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE), RowExclusiveLock);

	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		/* compression is enabled */
		drop_existing_compression_table(ht);
	}

	/* Check if we can create a compressed hypertable with existing
	 * constraints and indexes. */
	validate_existing_constraints(ht, settings);
	validate_existing_indexes(ht, settings);

	Oid tablespace_oid = get_rel_tablespace(ht->main_table_relid);
	compress_htid = compression_hypertable_create(ht, ownerid, tablespace_oid);
	ts_hypertable_set_compressed(ht, compress_htid);

	if (!with_clause_options[CompressChunkTimeInterval].is_default)
	{
		update_compress_chunk_time_interval(ht, with_clause_options);
	}

	/* do not release any locks, will get released by xact end */
	return true;
}

/*
 * Verify uncompressed hypertable is compatible with conpression
 */
static void
validate_hypertable_for_compression(Hypertable *ht)
{
	if (TS_HYPERTABLE_IS_INTERNAL_COMPRESSION_TABLE(ht))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot compress internal compression hypertable")));
	}

	/*check row security settings for the table */
	if (ts_has_row_security(ht->main_table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("compression cannot be used on table with row security")));

	Relation rel = table_open(ht->main_table_relid, AccessShareLock);
	TupleDesc tupdesc = RelationGetDescr(rel);

	/*
	 * This is only a rough estimate and the actual row size might be different.
	 * We use this only to show a warning when the row size is close to the
	 * maximum row size.
	 */
	Size row_size = MAXALIGN(SizeofHeapTupleHeader);
	row_size += 8;	/* sequence_num */
	row_size += 4;	/* count */
	row_size += 16; /* min/max */
	for (int attno = 0; attno < tupdesc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno);

		if (attr->attisdropped)
			continue;

		row_size += 18; /* assume 18 bytes for each compressed column (varlena) */

		if (strncmp(NameStr(attr->attname),
					COMPRESSION_COLUMN_METADATA_PREFIX,
					strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0)
			elog(ERROR,
				 "cannot compress tables with reserved column prefix '%s'",
				 COMPRESSION_COLUMN_METADATA_PREFIX);
	}
	table_close(rel, AccessShareLock);

	if (row_size > MaxHeapTupleSize)
	{
		ereport(WARNING,
				(errmsg("compressed row size might exceed maximum row size"),
				 errdetail("Estimated row size of compressed hypertable is %zu. This exceeds the "
						   "maximum size of %zu and can cause compression of chunks to fail.",
						   row_size,
						   MaxHeapTupleSize)));
	}
}

static void
compression_settings_update(CompressionSettings *settings, WithClauseResult *with_clause_options,
							List *segmentby_cols, List *orderby_cols)
{
	/* orderby arrays should always be in sync either all NULL or none */
	Assert(
		(settings->fd.orderby && settings->fd.orderby_desc && settings->fd.orderby_nullsfirst) ||
		(!settings->fd.orderby && !settings->fd.orderby_desc && !settings->fd.orderby_nullsfirst));

	if (!with_clause_options[CompressSegmentBy].is_default)
	{
		settings->fd.segmentby = ts_array_create_from_list_text(segmentby_cols);
	}

	if (!with_clause_options[CompressOrderBy].is_default || !settings->fd.orderby)
	{
		List *cols = NIL;
		List *desc = NIL;
		List *nullsfirst = NIL;
		ListCell *lc;

		foreach (lc, orderby_cols)
		{
			CompressedParsedCol *col = lfirst(lc);
			cols = lappend(cols, (void *) NameStr(col->colname));
			desc = lappend_int(desc, col->desc);
			nullsfirst = lappend_int(nullsfirst, col->nullsfirst);
		}

		settings->fd.orderby = ts_array_create_from_list_text(cols);
		settings->fd.orderby_desc = ts_array_create_from_list_bool(desc);
		settings->fd.orderby_nullsfirst = ts_array_create_from_list_bool(nullsfirst);
	}

	ts_compression_settings_update(settings);
}

/* Add a column to a table that has compression enabled
 * This function specifically adds the column to the internal compression table.
 */
void
tsl_process_compress_table_add_column(Hypertable *ht, ColumnDef *orig_def)
{
	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);
	if (!TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		return;
	}

	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.compressed_hypertable_id);
	ListCell *lc;
	Oid coloid = LookupTypeNameOid(NULL, orig_def->typeName, false);

	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		/* don't add column if it already exists */
		if (get_attnum(chunk->table_id, orig_def->colname) != InvalidAttrNumber)
		{
			return;
		}
		ColumnDef *coldef = build_columndef_singlecolumn(orig_def->colname, coloid);
		CompressionSettings *settings = ts_compression_settings_get(chunk->table_id);
		add_column_to_compression_table(chunk->table_id, settings, coldef);
	}
}

/* Drop a column from a table that has compression enabled
 * This function specifically removes it from the internal compression table
 * and removes it from metadata.
 * Removing orderby or segmentby columns is not supported.
 */
void
tsl_process_compress_table_drop_column(Hypertable *ht, char *name)
{
	Assert(TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht) || TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht));

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);

	CompressionSettings *settings = ts_compression_settings_get(ht->main_table_relid);

	if (ts_array_is_member(settings->fd.segmentby, name) ||
		ts_array_is_member(settings->fd.orderby, name))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop orderby or segmentby column from a hypertable with "
						"compression enabled")));

	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.compressed_hypertable_id);
		ListCell *lc;
		foreach (lc, chunks)
		{
			Chunk *chunk = lfirst(lc);
			drop_column_from_compression_table(chunk->table_id, name);
		}
	}
}

/* Rename a column on a hypertable that has compression enabled.
 *
 * This function renames the existing column in the internal compression table.
 * We assume that there is a 1-1 mapping between the original chunk and
 * compressed chunk column names and that the names are identical.
 * Also update any metadata associated with the column.
 */
void
tsl_process_compress_table_rename_column(Hypertable *ht, const RenameStmt *stmt)
{
	Assert(stmt->relationType == OBJECT_TABLE && stmt->renameType == OBJECT_COLUMN);
	Assert(TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht));

	if (strncmp(stmt->newname,
				COMPRESSION_COLUMN_METADATA_PREFIX,
				strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0)
		elog(ERROR,
			 "cannot compress tables with reserved column prefix '%s'",
			 COMPRESSION_COLUMN_METADATA_PREFIX);

	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.compressed_hypertable_id);
		ListCell *lc;
		foreach (lc, chunks)
		{
			Chunk *chunk = lfirst(lc);
			RenameStmt *compress_col_stmt = (RenameStmt *) copyObject(stmt);
			compress_col_stmt->relation =
				makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), -1);
			ExecRenameStmt(compress_col_stmt);
		}
	}
}
