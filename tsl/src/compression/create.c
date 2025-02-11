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
#include <catalog/pg_am_d.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_constraint_d.h>
#include <catalog/pg_type.h>
#include <catalog/toasting.h>
#include <commands/alter.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <common/md5.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <parser/parse_type.h>
#include <storage/lmgr.h>
#include <tcop/utility.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/datum.h>
#include <utils/guc.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "compat/compat.h"
#include "chunk.h"
#include "chunk_index.h"
#include "compression.h"
#include "compression/compression_storage.h"
#include "compression/sparse_index_bloom1.h"
#include "compression_with_clause.h"
#include "create.h"
#include "custom_type_cache.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "trigger.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_settings.h"
#include "ts_catalog/continuous_agg.h"
#include "utils.h"
#include <executor/spi.h>

static const char *sparse_index_types[] = { "min", "max", "bloom1" };

#ifdef USE_ASSERT_CHECKING
static bool
is_sparse_index_type(const char *type)
{
	for (size_t i = 0; i < sizeof(sparse_index_types) / sizeof(sparse_index_types[0]); i++)
	{
		if (strcmp(sparse_index_types[i], type) == 0)
		{
			return true;
		}
	}

	return false;
}
#endif

static void validate_hypertable_for_compression(Hypertable *ht);
static List *build_columndefs(CompressionSettings *settings, Oid src_relid);
static ColumnDef *build_columndef_singlecolumn(const char *colname, Oid typid);
static void compression_settings_update(Hypertable *ht, CompressionSettings *settings,
										WithClauseResult *with_clause_options);

static char *
compression_column_segment_metadata_name(const char *type, int16 column_index)
{
	Assert(is_sparse_index_type(type));

	char *buf = palloc(sizeof(char) * NAMEDATALEN);

	Assert(column_index > 0);
	int ret =
		snprintf(buf, NAMEDATALEN, COMPRESSION_COLUMN_METADATA_PATTERN_V1, type, column_index);
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
	return compression_column_segment_metadata_name("min", column_index);
}

char *
column_segment_max_name(int16 column_index)
{
	return compression_column_segment_metadata_name("max", column_index);
}

/*
 * Get metadata name for a given column name and metadata type, format version 2.
 * We can't reference the attribute numbers, because they can change after
 * drop/restore if we had any dropped columns.
 * We might have to truncate the column names to fit into the NAMEDATALEN here,
 * in this case we disambiguate them with their md5 hash.
 */
char *
compressed_column_metadata_name_v2(const char *metadata_type, const char *column_name)
{
	Assert(is_sparse_index_type(metadata_type));
	Assert(strlen(metadata_type) <= 6);

	const int len = strlen(column_name);
	Assert(len < NAMEDATALEN);

	/*
	 * We have to fit the name into NAMEDATALEN - 1 which is 63 bytes:
	 * 12 (_ts_meta_v2_) + 6 (metadata_type) + 1 (_) + x (column_name) + 1 (_) + 4 (hash) = 63;
	 * x = 63 - 24 = 39.
	 */
	char *result;
	if (len > 39)
	{
		const char *errstr = NULL;
		char hash[33];
		Ensure(pg_md5_hash_compat(column_name, len, hash, &errstr), "md5 computation failure");

		result = psprintf("_ts_meta_v2_%.6s_%.4s_%.39s", metadata_type, hash, column_name);
	}
	else
	{
		result = psprintf("_ts_meta_v2_%.6s_%.39s", metadata_type, column_name);
	}
	Assert(strlen(result) < NAMEDATALEN);
	return result;
}

int
compressed_column_metadata_attno(CompressionSettings *settings, Oid chunk_reloid,
								 AttrNumber chunk_attno, Oid compressed_reloid, char *metadata_type)
{
	Assert(is_sparse_index_type(metadata_type));

	char *attname = get_attname(chunk_reloid, chunk_attno, /* missing_ok = */ false);
	int16 orderby_pos = ts_array_position(settings->fd.orderby, attname);

	if (orderby_pos != 0 &&
		(strcmp(metadata_type, "min") == 0 || strcmp(metadata_type, "max") == 0))
	{
		char *metadata_name = compression_column_segment_metadata_name(metadata_type, orderby_pos);
		return get_attnum(compressed_reloid, metadata_name);
	}

	char *metadata_name = compressed_column_metadata_name_v2(metadata_type, attname);
	return get_attnum(compressed_reloid, metadata_name);
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
	List *compressed_column_defs = NIL;
	List *segmentby_column_defs = NIL;

	Relation rel = table_open(src_relid, AccessShareLock);

	Bitmapset *index_columns = NULL;
	if (ts_guc_auto_sparse_indexes)
	{
		/*
		 * Check which columns have btree indexes. We will create sparse minmax
		 * indexes for them in compressed chunk.
		 */
		ListCell *lc;
		List *index_oids = RelationGetIndexList(rel);
		foreach (lc, index_oids)
		{
			Oid index_oid = lfirst_oid(lc);
			Relation index_rel = index_open(index_oid, AccessShareLock);
			IndexInfo *index_info = BuildIndexInfo(index_rel);
			index_close(index_rel, NoLock);

			/*
			 * We want to create the sparse minmax index, if it can satisfy the same
			 * kinds of queries as the uncompressed index. The simplest case is btree
			 * which can satisfy equality and comparison tests, same as sparse minmax.
			 *
			 * If an uncompressed column has an index, we want to create a
			 * sparse index for it as well. A sparse index can't satisfy ordering
			 * queries, but at least we can use a bloom index to satisfy equality
			 * queries. Create it when we have uncompressed index types that can
			 * also satisfy equality.
			 */
			if (index_info->ii_Am != BTREE_AM_OID && index_info->ii_Am != HASH_AM_OID &&
				index_info->ii_Am != BRIN_AM_OID)
			{
				continue;
			}

			for (int i = 0; i < index_info->ii_NumIndexKeyAttrs; i++)
			{
				const AttrNumber attno = index_info->ii_IndexAttrNumbers[i];
				if (attno == InvalidAttrNumber)
				{
					continue;
				}

				index_columns = bms_add_member(index_columns, attno);
			}
		}
	}

	TupleDesc tupdesc = rel->rd_att;

	for (int attoffset = 0; attoffset < tupdesc->natts; attoffset++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attoffset);
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
			segmentby_column_defs = lappend(segmentby_column_defs,
											makeColumnDef(NameStr(attr->attname),
														  attr->atttypid,
														  attr->atttypmod,
														  attr->attcollation));
			continue;
		}

		/*
		 * This is either an orderby or a normal compressed column. We want to
		 * have metadata for some of them.  Put the metadata columns before the
		 * respective compressed column, because they are accessed before
		 * decompression.
		 */
		const bool is_orderby = ts_array_is_member(settings->fd.orderby, NameStr(attr->attname));
		if (is_orderby)
		{
			int index = ts_array_position(settings->fd.orderby, NameStr(attr->attname));
			TypeCacheEntry *type = lookup_type_cache(attr->atttypid, TYPECACHE_LT_OPR);

			/*
			 * We must be able to create the metadata for the orderby columns,
			 * because it is required for sorting.
			 */
			if (!OidIsValid(type->lt_opr))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("invalid ordering column type %s", format_type_be(attr->atttypid)),
						 errdetail("Could not identify a less-than operator for the type.")));

			/* segment_meta min and max columns */
			compressed_column_defs = lappend(compressed_column_defs,
											 makeColumnDef(column_segment_min_name(index),
														   attr->atttypid,
														   attr->atttypmod,
														   attr->attcollation));
			compressed_column_defs = lappend(compressed_column_defs,
											 makeColumnDef(column_segment_max_name(index),
														   attr->atttypid,
														   attr->atttypmod,
														   attr->attcollation));
		}
		else if (bms_is_member(attr->attnum, index_columns))
		{
			TypeCacheEntry *type =
				lookup_type_cache(attr->atttypid, TYPECACHE_LT_OPR | TYPECACHE_HASH_EXTENDED_PROC);

			/*
			 * We can have various unusual user-defined types which do not
			 * support comparison or hashing. The sparse indexes for the
			 * non-orderby columns are not required for correctness, so just
			 * don't create the sparse index if we lack the suitable operators.
			 */
			bool can_use_minmax = OidIsValid(type->lt_opr);

			/*
			 * For bloom1 indexes we currently use our custom hash functions
			 * which have better characteristics.
			 */
			bool can_use_bloom1 = bloom1_get_hash_function(attr->atttypid) != NULL;

			if (attr->atttypid == TIMESTAMPTZOID || attr->atttypid == TIMESTAMPOID ||
				attr->atttypid == TIMEOID || attr->atttypid == TIMETZOID ||
				attr->atttypid == DATEOID)
			{
				/*
				 * For time types, we expect:
				 * 1) range queries, not equality,
				 * 2) correlation with the orderby columns, e.g. creation time
				 *    correlates with the update time that is used as orderby.
				 * So bloom filters probably don't make sense.
				 */
				can_use_bloom1 = false;
			}

			if (can_use_bloom1)
			{
				/*
				 * Add bloom filter metadata for columns that are not a part of
				 * orderby, but a part of some other btree index.
				 */
				ColumnDef *bloom_column_def =
					makeColumnDef(compressed_column_metadata_name_v2("bloom1",
																	 NameStr(attr->attname)),
								  BYTEAOID,
								  /* typmod = */ -1,
								  /* collation = */ 0);

				/*
				 * We have our internal compression for bloom filters, and the
				 * result is almost incompressible with lz4 (~2%), so disable it.
				 */
				bloom_column_def->storage = TYPSTORAGE_EXTERNAL;

				compressed_column_defs = lappend(compressed_column_defs, bloom_column_def);
			}
			else if (can_use_minmax)
			{
				/*
				 * Here we create minmax metadata for the columns for which
				 * we have btree indexes and the bloom sparse index is not
				 * suitable.
				 * to have a btree index for a column and at the same time
				 * not have a "less" operator for it. Still, we can have
				 * various unusual user-defined types, and the minmax metadata
				 * for the rest of the columns are not required for correctness,
				 * so play it safe and just don't create the metadata if we don't
				 * have an operator.
				 */
				compressed_column_defs =
					lappend(compressed_column_defs,
							makeColumnDef(compressed_column_metadata_name_v2("min",
																			 NameStr(
																				 attr->attname)),
										  attr->atttypid,
										  attr->atttypmod,
										  attr->attcollation));
				compressed_column_defs =
					lappend(compressed_column_defs,
							makeColumnDef(compressed_column_metadata_name_v2("max",
																			 NameStr(
																				 attr->attname)),
										  attr->atttypid,
										  attr->atttypmod,
										  attr->attcollation));
			}
		}

		compressed_column_defs = lappend(compressed_column_defs,
										 makeColumnDef(NameStr(attr->attname),
													   compresseddata_oid,
													   /* typmod = */ -1,
													   /* collOid = */ InvalidOid));
	}

	/*
	 * Add the metadata columns. Count is always accessed, so put it first.
	 */
	List *all_column_defs = list_make1(makeColumnDef(COMPRESSION_COLUMN_METADATA_COUNT_NAME,
													 INT4OID,
													 -1 /* typemod */,
													 0 /*collation*/));

	/*
	 * Then, put all segmentby columns. They are likely to be used in filters
	 * before decompression.
	 */
	all_column_defs = list_concat(all_column_defs, segmentby_column_defs);

	/*
	 * Then, put all the compressed columns.
	 */
	all_column_defs = list_concat(all_column_defs, compressed_column_defs);

	table_close(rel, AccessShareLock);

	return all_column_defs;
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
static OrderBySettings
add_time_to_order_by_if_not_included(OrderBySettings obs, ArrayType *segmentby, Hypertable *ht)
{
	const Dimension *time_dim;
	const char *time_col_name;
	bool found = false;

	time_dim = hyperspace_get_open_dimension(ht->space, 0);
	if (!time_dim)
		return obs;

	time_col_name = get_attname(ht->main_table_relid, time_dim->column_attno, false);

	if (ts_array_is_member(obs.orderby, time_col_name))
		found = true;

	if (ts_array_is_member(segmentby, time_col_name))
		found = true;

	if (!found)
	{
		/* Add time DESC NULLS FIRST to order by settings */
		obs.orderby = ts_array_add_element_text(obs.orderby, pstrdup(time_col_name));
		obs.orderby_desc = ts_array_add_element_bool(obs.orderby_desc, true);
		obs.orderby_nullsfirst = ts_array_add_element_bool(obs.orderby_nullsfirst, true);
	}
	return obs;
}

/* returns list of constraints that need to be cloned on the compressed hypertable
 * This is limited to foreign key constraints now
 */
static void
validate_existing_constraints(Hypertable *ht, CompressionSettings *settings)
{
	Relation pg_constr;
	SysScanDesc scan;
	ScanKeyData scankey;
	HeapTuple tuple;

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
		 * We check primary, unique, and exclusion constraints.
		 */
		if (form->contype == CONSTRAINT_CHECK || form->contype == CONSTRAINT_TRIGGER
#if PG17_GE
			|| form->contype == CONSTRAINT_NOTNULL
		/* CONSTRAINT_NOTNULL introduced in PG17, see b0e96f311985 */
#endif
		)
		{
			continue;
		}
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
			attnums = (int16 *) ARR_DATA_PTR(arr);
			for (j = 0; j < numkeys; j++)
			{
				const char *attname = get_attname(settings->fd.relid, attnums[j], false);

				/* is colno a segment-by or order_by column */
				if (!form->conindid && !ts_array_is_member(settings->fd.segmentby, attname) &&
					!ts_array_is_member(settings->fd.orderby, attname))
					ereport(WARNING,
							(errmsg("column \"%s\" should be used for segmenting or ordering",
									attname)));
			}
		}
	}

	systable_endscan(scan);
	table_close(pg_constr, AccessShareLock);
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
drop_existing_compression_table(Hypertable *ht)
{
	if (ts_chunk_exists_with_compression(ht->fd.id))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop compression hypertable with compressed chunks")));

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
	if (!TS_HYPERTABLE_HAS_COMPRESSION_ENABLED(ht))
		/* compression is not enabled, so just return */
		return false;

	if (ts_chunk_exists_with_compression(ht->fd.id))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot disable compression on hypertable with compressed chunks")));

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
	if (!time_dim)
		return false;

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
	bool compress_disable = !with_clause_options[CompressEnabled].is_default &&
							!DatumGetBool(with_clause_options[CompressEnabled].parsed);
	CompressionSettings *settings;

	ts_feature_flag_check(FEATURE_HYPERTABLE_COMPRESSION);

	validate_hypertable_for_compression(ht);

	/* Lock the uncompressed ht in exclusive mode and keep till end of txn */
	LockRelationOid(ht->main_table_relid, AccessExclusiveLock);

	/* reload info after lock */
	ht = ts_hypertable_get_by_id(ht->fd.id);

	if (compress_disable)
	{
		return disable_compression(ht, with_clause_options);
	}

	settings = ts_compression_settings_get(ht->main_table_relid);
	if (!settings)
	{
		settings = ts_compression_settings_create(ht->main_table_relid, NULL, NULL, NULL, NULL);
	}

	compression_settings_update(ht, settings, with_clause_options);

	if (!TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		/* take explicit locks on catalog tables and keep them till end of txn */
		LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE), RowExclusiveLock);

		/* Check if we can create a compressed hypertable with existing
		 * constraints and indexes. */
		validate_existing_constraints(ht, settings);
		validate_existing_indexes(ht, settings);

		Oid ownerid = ts_rel_get_owner(ht->main_table_relid);
		Oid tablespace_oid = get_rel_tablespace(ht->main_table_relid);
		compress_htid = compression_hypertable_create(ht, ownerid, tablespace_oid);
		ts_hypertable_set_compressed(ht, compress_htid);
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

	if (row_size > MaxHeapTupleSize)
	{
		ereport(WARNING,
				(errmsg("compressed row size might exceed maximum row size"),
				 errdetail("Estimated row size of compressed hypertable is %zu. This exceeds the "
						   "maximum size of %zu and can cause compression of chunks to fail.",
						   row_size,
						   MaxHeapTupleSize)));
	}

	/*
	 * Check that all triggers are ok for compressed tables.
	 */
	Relation pg_trigger = table_open(TriggerRelationId, AccessShareLock);
	HeapTuple tuple;

	ScanKeyData key;
	ScanKeyInit(&key,
				Anum_pg_trigger_tgrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(ht->main_table_relid));

	SysScanDesc scan = systable_beginscan(pg_trigger, TriggerRelidNameIndexId, true, NULL, 1, &key);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		bool oldtable_isnull;
		Form_pg_trigger trigrec = (Form_pg_trigger) GETSTRUCT(tuple);

		/*
		 * We currently cannot support transition tables for DELETE triggers
		 * on compressed tables that are not using hypercore table access
		 * method since deleting a complete segment will not build a
		 * transition table for the delete.
		 */
		fastgetattr(tuple, Anum_pg_trigger_tgoldtable, pg_trigger->rd_att, &oldtable_isnull);
		if (!oldtable_isnull && !TRIGGER_FOR_ROW(trigrec->tgtype) &&
			TRIGGER_FOR_DELETE(trigrec->tgtype) && !ts_is_hypercore_am(ht->amoid))
			ereport(ERROR,
					errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("DELETE triggers with transition tables not supported"),
					errdetail(
						"Compressed hypertables not using \"hypercore\" access method are not "
						"supported if the trigger use transition tables."));
	}

	systable_endscan(scan);
	table_close(pg_trigger, AccessShareLock);
	table_close(rel, AccessShareLock);
}

/*
 * Get the default segment by value for a hypertable
 */
static ArrayType *
compression_setting_segmentby_get_default(const Hypertable *ht)
{
	StringInfoData command;
	StringInfoData result;
	int res;
	ArrayType *column_res = NULL;
	Datum datum;
	text *message;
	bool isnull;
	MemoryContext upper = CurrentMemoryContext;
	MemoryContext old;
	int32 confidence = -1;
	Oid default_segmentby_fn = ts_guc_default_segmentby_fn_oid();

	if (!OidIsValid(default_segmentby_fn))
	{
		elog(LOG_SERVER_ONLY,
			 "segment_by default: hypertable=\"%s\" columns=\"\" function: \"\" confidence=-1",
			 get_rel_name(ht->main_table_relid));
		return NULL;
	}

	/* Lock down search_path */
	int save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	initStringInfo(&command);
	appendStringInfo(&command,
					 "SELECT "
					 " (SELECT array_agg(x)  "
					 " FROM jsonb_array_elements_text(seg_by->'columns') t(x))::text[], "
					 " seg_by->>'message', "
					 " (seg_by->>'confidence')::int "
					 "FROM %s.%s(%d) seg_by",
					 quote_identifier(get_namespace_name(get_func_namespace(default_segmentby_fn))),
					 quote_identifier(get_func_name(default_segmentby_fn)),
					 ht->main_table_relid);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI");

	res = SPI_execute(command.data, true /* read_only */, 0 /*count*/);

	if (res < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 (errmsg("could not get the default segment by for a hypertable \"%s\"",
						 get_rel_name(ht->main_table_relid)))));

	old = MemoryContextSwitchTo(upper);
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);
	if (!isnull)
		column_res = DatumGetArrayTypePCopy(datum);
	MemoryContextSwitchTo(old);

	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);

	if (!isnull)
	{
		message = DatumGetTextPP(datum);
		elog(WARNING,
			 "there was some uncertainty picking the default segment by for the hypertable: %s",
			 text_to_cstring(message));
	}

	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull);
	if (!isnull)
	{
		confidence = DatumGetInt32(datum);
	}

	pfree(command.data);

	/* Reset search path since this can be executed as part of a larger transaction */
	AtEOXact_GUC(false, save_nestlevel);

	res = SPI_finish();
	if (res != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(res));

	initStringInfo(&result);
	ts_array_append_stringinfo(column_res, &result);
	elog(NOTICE,
		 "default segment by for hypertable \"%s\" is set to \"%s\"",
		 get_rel_name(ht->main_table_relid),
		 result.data);

	elog(LOG_SERVER_ONLY,
		 "segment_by default: hypertable=\"%s\" columns=\"%s\" function: \"%s.%s\" confidence=%d",
		 get_rel_name(ht->main_table_relid),
		 result.data,
		 get_namespace_name(get_func_namespace(default_segmentby_fn)),
		 get_func_name(default_segmentby_fn),
		 confidence);
	pfree(result.data);
	return column_res;
}

/*
 * Get the default segment by value for a hypertable
 */
static OrderBySettings
compression_setting_orderby_get_default(Hypertable *ht, ArrayType *segmentby)
{
	StringInfoData command;
	int res;
	text *column_res = NULL;
	Datum datum;
	text *message;
	bool isnull;
	MemoryContext upper = CurrentMemoryContext;
	MemoryContext old;
	char *orderby;
	int32 confidence = -1;

	Oid types[] = { TEXTARRAYOID };
	Datum values[] = { PointerGetDatum(segmentby) };
	char nulls[] = { segmentby == NULL ? 'n' : 'v' };
	Oid orderby_fn = ts_guc_default_orderby_fn_oid();

	if (!OidIsValid(orderby_fn))
	{
		/* fallback to original logic */
		OrderBySettings obs = (OrderBySettings){ 0 };
		obs = add_time_to_order_by_if_not_included(obs, segmentby, ht);
		elog(LOG_SERVER_ONLY,
			 "order_by default: hypertable=\"%s\" function=\"\" confidence=-1",
			 get_rel_name(ht->main_table_relid));
		return obs;
	}

	/* Lock down search_path */
	int save_nestlevel = NewGUCNestLevel();
	RestrictSearchPath();

	initStringInfo(&command);
	appendStringInfo(&command,
					 "SELECT "
					 " (SELECT string_agg(x, ', ') FROM "
					 "jsonb_array_elements_text(seg_by->'clauses') "
					 "t(x))::text, "
					 " seg_by->>'message', "
					 " (seg_by->>'confidence')::int "
					 "FROM %s.%s(%d, coalesce($1, array[]::text[])) seg_by",
					 quote_identifier(get_namespace_name(get_func_namespace(orderby_fn))),
					 quote_identifier(get_func_name(orderby_fn)),
					 ht->main_table_relid);

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "could not connect to SPI");

	res = SPI_execute_with_args(command.data,
								1,
								types,
								values,
								nulls,
								true /* read_only */,
								0 /*count*/);
	if (res < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 (errmsg("could not get the default order by for a hypertable \"%s\"",
						 get_rel_name(ht->main_table_relid)))));

	old = MemoryContextSwitchTo(upper);
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isnull);

	if (!isnull)
		column_res = DatumGetTextPCopy(datum);
	MemoryContextSwitchTo(old);

	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 2, &isnull);

	if (!isnull)
	{
		message = DatumGetTextPP(datum);
		elog(WARNING,
			 "there was some uncertainty picking the default order by for the hypertable: %s",
			 text_to_cstring(message));
	}
	datum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 3, &isnull);
	if (!isnull)
	{
		confidence = DatumGetInt32(datum);
	}

	/* Reset search path since this can be executed as part of a larger transaction */
	AtEOXact_GUC(false, save_nestlevel);

	pfree(command.data);

	res = SPI_finish();
	if (res != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed: %s", SPI_result_code_string(res));

	if (column_res != NULL)
		orderby = TextDatumGetCString(PointerGetDatum(column_res));
	else
		orderby = "";

	elog(NOTICE,
		 "default order by for hypertable \"%s\" is set to \"%s\"",
		 get_rel_name(ht->main_table_relid),
		 orderby);

	elog(LOG_SERVER_ONLY,
		 "order_by default: hypertable=\"%s\" clauses=\"%s\" function=\"%s.%s\" confidence=%d",
		 get_rel_name(ht->main_table_relid),
		 orderby,
		 get_namespace_name(get_func_namespace(orderby_fn)),
		 get_func_name(orderby_fn),
		 confidence);
	return ts_compress_parse_order_collist(orderby, ht);
}

static void
compression_settings_update(Hypertable *ht, CompressionSettings *settings,
							WithClauseResult *with_clause_options)
{
	/* orderby arrays should always be in sync either all NULL or none */
	Assert(
		(settings->fd.orderby && settings->fd.orderby_desc && settings->fd.orderby_nullsfirst) ||
		(!settings->fd.orderby && !settings->fd.orderby_desc && !settings->fd.orderby_nullsfirst));

	if (!with_clause_options[CompressChunkTimeInterval].is_default)
	{
		update_compress_chunk_time_interval(ht, with_clause_options);
	}

	if (!with_clause_options[CompressSegmentBy].is_default)
	{
		settings->fd.segmentby = ts_compress_hypertable_parse_segment_by(with_clause_options, ht);
	}
	else if (!settings->fd.segmentby)
	{
		settings->fd.segmentby = compression_setting_segmentby_get_default(ht);
	}

	if (!with_clause_options[CompressOrderBy].is_default || !settings->fd.orderby)
	{
		OrderBySettings obs;
		if (with_clause_options[CompressOrderBy].is_default)
		{
			obs = compression_setting_orderby_get_default(ht, settings->fd.segmentby);
		}
		else
		{
			obs = ts_compress_hypertable_parse_order_by(with_clause_options, ht);
			obs = add_time_to_order_by_if_not_included(obs, settings->fd.segmentby, ht);
		}
		settings->fd.orderby = obs.orderby;
		settings->fd.orderby_desc = obs.orderby_desc;
		settings->fd.orderby_nullsfirst = obs.orderby_nullsfirst;
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

	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.compressed_hypertable_id);
	ListCell *lc;
	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		CompressionSettings *settings = ts_compression_settings_get(chunk->table_id);
		if (ts_array_is_member(settings->fd.segmentby, name) ||
			ts_array_is_member(settings->fd.orderby, name))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot drop orderby or segmentby column from a chunk with "
							"compression enabled")));
	}

	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
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

	if (!TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		return;
	}

	RenameStmt *compressed_col_stmt = (RenameStmt *) copyObject(stmt);
	RenameStmt *compressed_index_stmt = (RenameStmt *) copyObject(stmt);
	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.compressed_hypertable_id);
	ListCell *lc;
	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		compressed_col_stmt->relation =
			makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), -1);
		ExecRenameStmt(compressed_col_stmt);

		compressed_index_stmt->relation = compressed_col_stmt->relation;
		for (size_t i = 0; i < sizeof(sparse_index_types) / sizeof(sparse_index_types[0]); i++)
		{
			char *old_index_name =
				compressed_column_metadata_name_v2(sparse_index_types[i], stmt->subname);
			if (get_attnum(chunk->table_id, old_index_name) == InvalidAttrNumber)
			{
				continue;
			}

			char *new_index_name =
				compressed_column_metadata_name_v2(sparse_index_types[i], stmt->newname);
			compressed_index_stmt->subname = old_index_name;
			compressed_index_stmt->newname = new_index_name;
			ExecRenameStmt(compressed_index_stmt);
		}
	}
}
