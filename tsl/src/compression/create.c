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
#include <executor/spi.h>
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
#include "bgw_policy/policies_v2.h"
#include "chunk.h"
#include "chunk_index.h"
#include "compression.h"
#include "compression/compression_storage.h"
#include "compression/sparse_index_bloom1.h"
#include "create.h"
#include "custom_type_cache.h"
#include "dimension.h"
#include "foreach_ptr.h"
#include "guc.h"
#include "hypertable_cache.h"
#include "jsonb_utils.h"
#include "trigger.h"
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_settings.h"
#include "ts_catalog/continuous_agg.h"
#include "utils.h"
#include "with_clause/alter_table_with_clause.h"
#include "with_clause/create_table_with_clause.h"

#include "bgw_policy/compression_api.h"

#ifdef USE_ASSERT_CHECKING
static const char *sparse_index_types[] = { "min", "max" };

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

	if (strcmp(bloom1_column_prefix, type) == 0)
	{
		return true;
	}

	if (ts_guc_read_legacy_bloom1_v1 && strcmp("bloom1", type) == 0)
	{
		return true;
	}

	return false;
}
#endif

static void validate_hypertable_for_compression(Hypertable *ht);
static List *build_columndefs(CompressionSettings *settings, Oid src_reloid);
static ColumnDef *build_columndef_singlecolumn(const char *colname, Oid typid);
static void compression_settings_set_manually_for_create(Hypertable *ht,
														 CompressionSettings *settings,
														 WithClauseResult *with_clause_options);
static void compression_settings_set_manually_for_alter(Hypertable *ht,
														CompressionSettings *settings,
														WithClauseResult *with_clause_options);
static void create_composite_bloom(IndexInfo *index_info, Hypertable *ht,
								   CompressionSettings *settings, JsonbParseState *parse_state,
								   TsBmsList *sparse_index_columns, bool *has_object);

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

/*
 * Validate that compression settings don't exceed PostgreSQL's INDEX_MAX_KEYS limit.
 *
 * Compression creates an implicit index on the compressed chunk with:
 * - 1 index key per segmentby column
 * - 2 index keys per orderby column (for min/max metadata)
 */
static void
validate_compression_index_key_limit(CompressionSettings *settings)
{
	int num_segmentby_keys = ts_array_length(settings->fd.segmentby);
	int num_orderby_keys = 2 * ts_array_length(settings->fd.orderby);
	if ((num_segmentby_keys + num_orderby_keys) > INDEX_MAX_KEYS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("too many segmentby and orderby columns"),
				 errdetail("Combined segmentby keys (%d) and orderby keys (%d) cannot exceed %d",
						   num_segmentby_keys,
						   num_orderby_keys,
						   INDEX_MAX_KEYS)));
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
compressed_column_metadata_name_v2(const char *metadata_type, const char **column_names,
								   int num_columns)
{
	Assert(is_sparse_index_type(metadata_type));
	Assert(strlen(metadata_type) <= 6);
	Assert(column_names != NULL);
	Assert(num_columns > 0);
	Assert(num_columns <= MAX_BLOOM_FILTER_COLUMNS);

	int len = 0;
	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	for (int i = 0; i < num_columns; i++)
	{
		Assert(column_names[i] != NULL);
#ifdef USE_ASSERT_CHECKING
		int col_len = strlen(column_names[i]);
#endif
		Assert(col_len > 0 && col_len < NAMEDATALEN);
		if (i > 0)
			appendStringInfoChar(&buf, '_');
		appendStringInfo(&buf, "%s", column_names[i]);
	}

	len = buf.len;

	/*
	 * We have to fit the name into NAMEDATALEN - 1 which is 63 bytes:
	 * 12 (_ts_meta_v2_) + 6 (metadata_type) + [1 (_) + x (column_name)]x num_columns  + 1 (_) + 4
	 * (hash) = 63; x = 63 - 24 = 39.
	 */

	char *result;
	if (len > 39)
	{
		const char *errstr = NULL;
		char hash[33];
		Ensure(pg_md5_hash(buf.data, len, hash, &errstr), "md5 computation failure");
		result = psprintf("_ts_meta_v2_%.6s_%.4s_%.39s", metadata_type, hash, buf.data);
	}
	else
	{
		result = psprintf("_ts_meta_v2_%.6s_%.39s", metadata_type, buf.data);
	}
	Assert(strlen(result) < NAMEDATALEN);
	return result;
}

char *
compressed_column_metadata_name_list_v2(const char *metadata_type, List *column_names_list)
{
	int num_column_names = list_length(column_names_list);
	Ensure(num_column_names > 0, "list of column names must be non-empty");
	Ensure(num_column_names <= MAX_BLOOM_FILTER_COLUMNS,
		   "list of column names must be less than or equal to %d, got %d",
		   MAX_BLOOM_FILTER_COLUMNS,
		   num_column_names);

	const char *column_names[MAX_BLOOM_FILTER_COLUMNS];
	ListCell *cell = NULL;
	int i = 0;
	foreach (cell, column_names_list)
	{
		column_names[i] = (const char *) lfirst(cell);
		i++;
	}

	return compressed_column_metadata_name_v2(metadata_type, column_names, num_column_names);
}

int
compressed_column_metadata_attno(const CompressionSettings *settings, Oid chunk_reloid,
								 AttrNumber chunk_attno, Oid compressed_reloid,
								 char const *metadata_type)
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

	char *metadata_name =
		compressed_column_metadata_name_v2(metadata_type, (const char **) &attname, 1);
	return get_attnum(compressed_reloid, metadata_name);
}

/*
 * The heuristic for whether we should use the bloom filter sparse index.
 */
static bool
should_create_bloom_sparse_index(Oid atttypid, TypeCacheEntry *type, Oid src_reloid)
{
	/*
	 * The index must be enabled by the GUC.
	 */
	if (!ts_guc_enable_sparse_index_bloom)
	{
		return false;
	}

	/*
	 * The type must be hashable. For some types we use our own hash functions
	 * which have better characteristics.
	 */
	FmgrInfo *finfo = NULL;
	if (bloom1_get_hash_function(atttypid, &finfo) == NULL)
	{
		return false;
	}

	/*
	 * For time types, we expect:
	 * 1) range queries, not equality,
	 * 2) correlation with the orderby columns, e.g. creation time correlates
	 *    with the update time that is used as orderby.
	 * This makes minmax indexes more suitable than bloom filters.
	 */
	if (atttypid == TIMESTAMPTZOID || atttypid == TIMESTAMPOID || atttypid == TIMEOID ||
		atttypid == TIMETZOID || atttypid == DATEOID)
	{
		return false;
	}

	/*
	 * For fractional arithmetic types, equality queries are unlikely.
	 */
	if (atttypid == FLOAT4OID || atttypid == FLOAT8OID || atttypid == NUMERICOID)
	{
		return false;
	}

	/*
	 * Bloom filters for 1k elements with 2% false positive rate require about
	 * one byte per element, so there's no point in using them for smaller data
	 * types that typically compress to less than that.
	 */
	if (type->typlen > 0 && type->typlen < 4)
	{
		return false;
	}

	return true;
}

/*
 * Create a column definition for a sparse index column. The attributes passed is a
 * List of Form_pg_attribute elements. Min and max indices only use
 * the first element. Bloom filters may use multiple columns.
 */
static ColumnDef *
create_sparse_index_column_def(List *attributes, const char *metadata_type)
{
	Assert(is_sparse_index_type(metadata_type));
	ColumnDef *column_def = NULL;
	List *column_names = NIL;

	/* At least one valid attribute must be present */
	Assert(attributes != NULL);
	Assert(list_length(attributes) > 0);
	Assert(list_length(attributes) <= MAX_BLOOM_FILTER_COLUMNS);

	const bool is_bloom = strcmp(metadata_type, bloom1_column_prefix) == 0;

	{
		/* Populate the column names array */
		ListCell *cell = NULL;
		int i = 0;
		foreach (cell, attributes)
		{
			Form_pg_attribute attr = (Form_pg_attribute) lfirst(cell);
			Ensure(i < MAX_BLOOM_FILTER_COLUMNS,
				   "too many columns for bloom filter, got %d, max %d, name: %s",
				   i + 1,
				   MAX_BLOOM_FILTER_COLUMNS,
				   NameStr(attr->attname));
			column_names = lappend(column_names, NameStr(attr->attname));
			i++;
		}
	}

	if (is_bloom)
	{
		/*
		 * The types must be hashable. For some types we use our own hash functions
		 * which have better characteristics.
		 */
		ListCell *cell = NULL;
		foreach (cell, attributes)
		{
			Form_pg_attribute attr = (Form_pg_attribute) lfirst(cell);
			FmgrInfo *finfo = NULL;
			if (bloom1_get_hash_function(attr->atttypid, &finfo) == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("invalid bloom filter column type %s, name: %s",
								format_type_be(attr->atttypid),
								NameStr(attr->attname)),
						 errdetail("Could not identify a hashing function for the type.")));
		}

		column_def =
			makeColumnDef(compressed_column_metadata_name_list_v2(metadata_type, column_names),
						  ts_custom_type_cache_get(CUSTOM_TYPE_BLOOM1)->type_oid,
						  /* typmod = */ -1,
						  /* collation = */ 0);

		/*
		 * We have our custom compression for bloom filters, and the
		 * result is almost incompressible with lz4 (~2%), so disable it.
		 */
		column_def->storage = TYPSTORAGE_EXTERNAL;
	}
	else /* either min or max */
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(list_head(attributes));
		TypeCacheEntry *type = lookup_type_cache(attr->atttypid, TYPECACHE_LT_OPR);

		/*
		 * a comparison operator if required for min max operations
		 */
		if (!OidIsValid(type->lt_opr))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_FUNCTION),
					 errmsg("invalid minmax column type %s", format_type_be(attr->atttypid)),
					 errdetail("Could not identify a less-than operator for the type.")));

		column_def =
			makeColumnDef(compressed_column_metadata_name_list_v2(metadata_type, column_names),
						  attr->atttypid,
						  attr->atttypmod,
						  attr->attcollation);
		if (attr->attstorage != TYPSTORAGE_PLAIN)
		{
			column_def->storage = TYPSTORAGE_MAIN;
		}
	}

	return column_def;
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
build_columndefs(CompressionSettings *settings, Oid src_reloid)
{
	Oid compresseddata_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;
	ArrayType *segmentby = settings->fd.segmentby;
	List *compressed_column_defs = NIL;
	List *segmentby_column_defs = NIL;
	Jsonb *sparse_cfg = settings->fd.index;
	SparseIndexSettings *parsed_settings =
		sparse_cfg ? ts_convert_to_sparse_index_settings(sparse_cfg) : NULL;
	Bitmapset *all_composite_bloom_obj_ids = NULL;
	List *per_column_settings = ts_get_per_column_compression_settings(parsed_settings);

	Relation rel = table_open(src_reloid, AccessShareLock);

	TupleDesc tupdesc = rel->rd_att;

	int num_sparse_index_objects =
		parsed_settings != NULL ? list_length(parsed_settings->objects) : 0;
	List **composite_attr_lists = NULL;
	if (num_sparse_index_objects > 0)
	{
		/* Allocate an array of Lists that contain Form_pg_attribute elements for each sparse index
		 * configuration object. Minmax and single bloom filter configuration objects will have a
		 * single element list.
		 */
		composite_attr_lists = palloc0(sizeof(List *) * num_sparse_index_objects);
	}

	for (int attoffset = 0; attoffset < tupdesc->natts; attoffset++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attoffset);
		if (attr->attisdropped)
			continue;
		if (strncmp(NameStr(attr->attname),
					COMPRESSION_COLUMN_METADATA_PREFIX,
					strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_RESERVED_NAME),
					 errmsg("cannot convert tables with reserved column prefix '%s'",
							COMPRESSION_COLUMN_METADATA_PREFIX)));

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

		PerColumnCompressionSettings *per_column_setting =
			per_column_settings ?
				ts_get_per_column_compression_settings_by_column_name(per_column_settings,
																	  NameStr(attr->attname)) :
				NULL;

		if (per_column_setting != NULL && composite_attr_lists != NULL)
		{
			if (per_column_setting->minmax_obj_id != -1 &&
				per_column_setting->minmax_obj_id < num_sparse_index_objects)
			{
				/* Minmax index configuration objects will have a single element list */
				Assert(list_length(composite_attr_lists[per_column_setting->minmax_obj_id]) == 0);
				composite_attr_lists[per_column_setting->minmax_obj_id] =
					lappend(composite_attr_lists[per_column_setting->minmax_obj_id], attr);
			}

			if (per_column_setting->single_bloom_obj_id != -1 &&
				per_column_setting->single_bloom_obj_id < num_sparse_index_objects)
			{
				/* Single bloom filter configuration objects will have a single element list */
				Assert(list_length(composite_attr_lists[per_column_setting->single_bloom_obj_id]) ==
					   0);
				composite_attr_lists[per_column_setting->single_bloom_obj_id] =
					lappend(composite_attr_lists[per_column_setting->single_bloom_obj_id], attr);
			}

			if (per_column_setting->composite_bloom_index_obj_ids != NULL)
			{
				/* The bitmapset tells which sparse index configuration objects the current
				 * column participates in. Iterate over the bitmapset and add an entry
				 * to the composite_attr_lists. */
				int i = -1;
				while ((i = bms_next_member(per_column_setting->composite_bloom_index_obj_ids,
											i)) >= 0)
				{
					composite_attr_lists[i] = lappend(composite_attr_lists[i], attr);
				}

				/* capture all composite bloom index objects */
				all_composite_bloom_obj_ids =
					bms_union(all_composite_bloom_obj_ids,
							  per_column_setting->composite_bloom_index_obj_ids);
			}
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
			ColumnDef *def = makeColumnDef(column_segment_min_name(index),
										   attr->atttypid,
										   attr->atttypmod,
										   attr->attcollation);
			def->storage = TYPSTORAGE_PLAIN;
			compressed_column_defs = lappend(compressed_column_defs, def);
			def = makeColumnDef(column_segment_max_name(index),
								attr->atttypid,
								attr->atttypmod,
								attr->attcollation);
			def->storage = TYPSTORAGE_PLAIN;
			compressed_column_defs = lappend(compressed_column_defs, def);
		}
		else if (per_column_setting != NULL && composite_attr_lists != NULL)
		{
			/* check sparse index columndefs is applicable */
			bool is_bloom = per_column_setting->single_bloom_obj_id != -1;
			bool is_minmax = per_column_setting->minmax_obj_id != -1;

			/*
			 * We allow only one sparse index per column. Columns used in the ORDER BY
			 * clause implicitly have a minmax index and adding a bloom filter on them is not
			 * allowed.
			 *
			 * The parser is expected to enforce this constraint earlier, but we check again
			 * here as a safeguard.
			 */
			Ensure((!is_bloom || !is_minmax),
				   "Should not create bloom filter for minmax column \"%s\"",
				   NameStr(attr->attname));

			/* build sparse index columndefs if applicable */
			if (is_bloom)
			{
				if (!ts_guc_enable_sparse_index_bloom)
				{
					ereport(WARNING,
							(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							 errmsg("Creating bloom sparse index is disabled"),
							 errhint("Either set \"enable_sparse_index_bloom\" to true or remove "
									 "the bloom filter indexes from \"sparse_index\" configuration "
									 "of the hypertable.")));
				}
				/*
				 * Add bloom filter sparse index for this column.
				 */
				ColumnDef *bloom_column_def =
					create_sparse_index_column_def(composite_attr_lists[per_column_setting
																			->single_bloom_obj_id],
												   bloom1_column_prefix);

				compressed_column_defs = lappend(compressed_column_defs, bloom_column_def);
			}
			else if (is_minmax)
			{
				/*
				 * Add minmax sparse index for this column.
				 */
				ColumnDef *def =
					create_sparse_index_column_def(composite_attr_lists[per_column_setting
																			->minmax_obj_id],
												   "min");
				compressed_column_defs = lappend(compressed_column_defs, def);

				def = create_sparse_index_column_def(composite_attr_lists[per_column_setting
																			  ->minmax_obj_id],
													 "max");
				compressed_column_defs = lappend(compressed_column_defs, def);
			}
		}
		compressed_column_defs = lappend(compressed_column_defs,
										 makeColumnDef(NameStr(attr->attname),
													   compresseddata_oid,
													   /* typmod = */ -1,
													   /* collOid = */ InvalidOid));
	}

	/* add the composite bloom columns */
	if (composite_attr_lists != NULL && per_column_settings != NULL)
	{
		/* iterate over the all_composite_bloom_obj_ids bitmapset */
		int i = -1;
		while ((i = bms_next_member(all_composite_bloom_obj_ids, i)) >= 0)
		{
			Assert(i < num_sparse_index_objects);
			Assert(composite_attr_lists[i] != NULL);
			List *attr_list = composite_attr_lists[i];
			if (attr_list != NULL)
			{
				ColumnDef *def = create_sparse_index_column_def(attr_list, bloom1_column_prefix);
				compressed_column_defs = lappend(compressed_column_defs, def);
			}
		}
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
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("cannot convert tables with reserved column prefix '%s'",
						COMPRESSION_COLUMN_METADATA_PREFIX)));

	return makeColumnDef(colname, compresseddata_oid, -1 /*typmod*/, 0 /*collation*/);
}

/*
 * Create compress chunk for specific table.
 *
 * If table_id is InvalidOid, create a new table.
 *
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

	/* Create a new catalog entry for chunk based on uncompressed chunk */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	compress_chunk =
		ts_chunk_create_base(ts_catalog_table_next_seq_id(catalog, CHUNK), 0, RELKIND_RELATION);
	ts_catalog_restore_user(&sec_ctx);

	compress_chunk->fd.hypertable_id = compress_ht->fd.id;
	compress_chunk->hypertable_relid = compress_ht->main_table_relid;
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

	/* Create the actual table relation for the chunk
	 * Note that we have to pick the tablespace here as the compressed ht doesn't have dimensions
	 * on which to base this decision. We simply pick the same tablespace as the uncompressed chunk
	 * for now.
	 */
	tablespace_oid = get_rel_tablespace(src_chunk->table_id);
	CompressionSettings *settings = ts_compression_settings_get(src_chunk->hypertable_relid);

	/*
	 * On hypertables created with CREATE TABLE ... WITH we enable compression
	 * by default but do not create CompressionSettings immediately assuming
	 * that we have more information available when the first compression
	 * is actually triggered allowing us to generate better compression
	 * settings.
	 */

	if (!settings)
	{
		settings = ts_compression_settings_create(src_chunk->hypertable_relid,
												  InvalidOid,
												  NULL,
												  NULL,
												  NULL,
												  NULL,
												  NULL);
	}

	Hypertable *ht = ts_hypertable_get_by_id(src_chunk->fd.hypertable_id);
	compression_settings_set_defaults(ht, settings, ts_alter_table_with_clause_parse(NIL));

	if (OidIsValid(table_id))
		compress_chunk->table_id = table_id;
	else
	{
		List *column_defs = build_columndefs(settings, src_chunk->table_id);
		compress_chunk->table_id = compression_chunk_create(src_chunk,
															compress_chunk,
															column_defs,
															tablespace_oid,
															settings);
	}

	if (!OidIsValid(compress_chunk->table_id))
		elog(ERROR, "could not create columnstore chunk table");

	/* Materialize current compression settings for this chunk */
	ts_compression_settings_materialize(settings, src_chunk->table_id, compress_chunk->table_id);

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

	Assert(ht->main_table_relid == settings->fd.relid);
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
					 errmsg("constraint %s is not supported for converting to columnstore",
							NameStr(form->conname)),
					 errhint("Exclusion constraints are not supported on hypertables that are "
							 "converted to columnstore.")));
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
				if (!form->conindid && (settings->fd.segmentby && settings->fd.orderby) &&
					!ts_array_is_member(settings->fd.segmentby, attname) &&
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
			if ((settings->fd.segmentby && settings->fd.orderby) &&
				!ts_array_is_member(settings->fd.segmentby, attname) &&
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
				 errmsg("cannot drop columnstore-enabled hypertable with columnstore chunks")));

	Hypertable *compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
	if (compressed == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("columnstore-enabled hypertable not found"),
				 errdetail("columnstore was enabled on \"%s\", but its internal"
						   " columnstore hypertable could not be found.",
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
				 errmsg("cannot disable columnstore on hypertable with columnstore chunks")));

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

/* Drop column from internal compression table, drop the bloom filter columns as well and
 * update the compression settings for the chunk */
static void
drop_column_from_compression_table(CompressionSettings *comp_settings, char *name)
{
	Oid relid = comp_settings->fd.compress_relid;
	AlterTableCmd *cmd;
	List *cmds = NIL;
	Jsonb *jb = comp_settings->fd.index;

	/* create altertable stmt to drop column from the compressed hypertable */
	cmd = makeNode(AlterTableCmd);
	cmd->subtype = AT_DropColumn;
	cmd->name = name;
	cmd->missing_ok = true;
	cmds = list_make1(cmd);

	if (jb)
	{
		SparseIndexSettings *parsed_settings = ts_convert_to_sparse_index_settings(jb);
		if (parsed_settings)
		{
			bool removed_any = false;

			ListCell *obj_cell = NULL;
			foreach (obj_cell, parsed_settings->objects)
			{
				bool removed = false;
				const char *bloom_column_name = NULL;
				SparseIndexSettingsObject *obj = (SparseIndexSettingsObject *) lfirst(obj_cell);
				foreach_ptr(SparseIndexSettingsPair, pair, obj->pairs)
				{
					if (strcmp(pair->key, ts_sparse_index_common_keys[SparseIndexKeyCol]) != 0)
					{
						continue;
					}
					foreach_ptr(const char, value, pair->values)
					{
						if (strcmp(value, name) == 0)
						{
							removed = true;
							Assert(list_length(pair->values) <= MAX_BLOOM_FILTER_COLUMNS);

							bloom_column_name =
								compressed_column_metadata_name_list_v2(bloom1_column_prefix,
																		pair->values);
							Assert(bloom_column_name != NULL);
							break;
						}
					}
					if (removed)
					{
						break;
					}
				}
				/* if the column was removed, we need to remove the object from the list */
				if (removed)
				{
					removed_any = true;
					if (bloom_column_name)
					{
						cmd = makeNode(AlterTableCmd);
						cmd->subtype = AT_DropColumn;
						cmd->name = pstrdup(bloom_column_name);
						cmd->missing_ok = true;
						cmds = lappend(cmds, cmd);
					}
					parsed_settings->objects =
						foreach_delete_current(parsed_settings->objects, obj_cell);
				}
			}

			if (removed_any)
			{
				jb = ts_convert_from_sparse_index_settings(parsed_settings);
				comp_settings->fd.index = jb;
				ts_compression_settings_update(comp_settings);
			}
			ts_free_sparse_index_settings(parsed_settings);
		}
	}

	/* alter the table and drop column */
	ts_alter_table_with_event_trigger(relid, NULL, cmds, true);
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
tsl_process_compress_table(Hypertable *ht, WithClauseResult *with_clause_options)
{
	int32 compress_htid;
	bool compress_disable = !with_clause_options[AlterTableFlagColumnstore].is_default &&
							!DatumGetBool(with_clause_options[AlterTableFlagColumnstore].parsed);
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

	if (!with_clause_options[AlterTableFlagCompressChunkTimeInterval].is_default)
	{
		update_compress_chunk_time_interval(ht, with_clause_options);
	}

	settings = ts_compression_settings_get(ht->main_table_relid);
	if (!settings)
	{
		settings = ts_compression_settings_create(ht->main_table_relid,
												  InvalidOid,
												  NULL,
												  NULL,
												  NULL,
												  NULL,
												  NULL);
	}

	compression_settings_set_manually_for_alter(ht, settings, with_clause_options);

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

	/*
	 * Check for suboptimal compressed chunk merging configuration
	 *
	 * When compress_chunk_time_interval is configured to merge chunks during compression the
	 * primary dimension should be the first compress_orderby column otherwise chunk merging will
	 * require decompression.
	 */
	Dimension *dim = ts_hyperspace_get_mutable_dimension(ht->space, DIMENSION_TYPE_OPEN, 0);
	if (dim && dim->fd.compress_interval_length &&
		ts_array_position(settings->fd.orderby, NameStr(dim->fd.column_name)) != 1)
	{
		ereport(WARNING,
				(errcode(ERRCODE_WARNING),
				 errmsg("compress_chunk_time_interval configured and primary dimension not "
						"first column in compress_orderby"),
				 errhint("consider setting \"%s\" as first compress_orderby column",
						 NameStr(dim->fd.column_name))));
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
				 errmsg("cannot compress internal columnstore hypertable")));
	}

	/*check row security settings for the table */
	if (ts_has_row_security(ht->main_table_relid))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("columnstore cannot be used on table with row security")));

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
			ereport(ERROR,
					(errcode(ERRCODE_RESERVED_NAME),
					 errmsg("cannot convert tables with reserved column prefix '%s' to columnstore",
							COMPRESSION_COLUMN_METADATA_PREFIX)));
	}

	if (row_size > MaxHeapTupleSize)
	{
		ereport(WARNING,
				(errmsg("compressed row size might exceed maximum row size"),
				 errdetail("Estimated row size of columnstore-enabled hypertable is %zu. This "
						   "exceeds the "
						   "maximum size of %zu and can cause conversion of chunks to columnstore "
						   "to fail.",
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
		 * We currently don't support transition tables for DELETE triggers
		 * on compressed tables because deleting a complete segment will not build a
		 * transition table for the delete.
		 */
		fastgetattr(tuple, Anum_pg_trigger_tgoldtable, pg_trigger->rd_att, &oldtable_isnull);
		if (!oldtable_isnull && !TRIGGER_FOR_ROW(trigrec->tgtype) &&
			TRIGGER_FOR_DELETE(trigrec->tgtype))
			ereport(ERROR,
					errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("DELETE triggers with transition tables not supported"));
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
		elog(LOG_SERVER_ONLY,
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
		elog(LOG_SERVER_ONLY,
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

	elog(LOG_SERVER_ONLY,
		 "order_by default: hypertable=\"%s\" clauses=\"%s\" function=\"%s.%s\" confidence=%d",
		 get_rel_name(ht->main_table_relid),
		 orderby,
		 get_namespace_name(get_func_namespace(orderby_fn)),
		 get_func_name(orderby_fn),
		 confidence);

	if (*orderby == '\0')
	{
		return (OrderBySettings){ 0 };
	}

	return ts_compress_parse_order_collist(orderby, ht);
}

/* sparse indexes will only be set by default if there was no configuration */
static bool
can_set_default_sparse_index(CompressionSettings *settings)
{
	return (settings->fd.index == NULL) ||
		   !ts_jsonb_has_key_value_str_field(settings->fd.index,
											 ts_sparse_index_common_keys[SparseIndexKeySource],
											 ts_sparse_index_source_names
												 [_SparseIndexSourceEnumConfig]);
}

static void
create_composite_bloom(IndexInfo *index_info, Hypertable *ht, CompressionSettings *settings,
					   JsonbParseState *parse_state, TsBmsList *sparse_index_columns,
					   bool *has_object)
{
	int num_cols = index_info->ii_NumIndexKeyAttrs;

	/* Allocate bloom config for the number of columns in the index */
	BloomFilterConfig bloom_config;
	bloom_config.base.type = _SparseIndexTypeEnumBloom;
	bloom_config.base.source = _SparseIndexSourceEnumDefault;
	bloom_config.columns = palloc0(num_cols * sizeof(SparseIndexColumn));

	/* Extract columns, filtering out segmentby columns.
	 * Note: orderby columns are not filtered out here because they can
	 * be in composite bloom filters.
	 */
	int valid_columns = 0;

	/*
	 * The index must be enabled by the GUC.
	 */
	if (!ts_guc_enable_sparse_index_bloom)
	{
		return;
	}

	/* Bitmapset of column attnums */
	Bitmapset *attnums_bitmap = NULL;

	/* Check the total width of the hashable columns */
	int total_width = 0;

	for (int i = 0; i < num_cols; i++)
	{
		AttrNumber attno = index_info->ii_IndexAttrNumbers[i];

		/* Skip expression indexes */
		if (attno == InvalidAttrNumber)
			continue;

		char *attname = get_attname(ht->main_table_relid, attno, false);

		/* Skip segmentby columns but continue processing other columns */
		if (ts_array_is_member(settings->fd.segmentby, attname))
			continue;

		Oid atttypid = get_atttype(ht->main_table_relid, attno);

		/* Check if hashable */
		FmgrInfo *finfo = NULL;
		if (bloom1_get_hash_function(atttypid, &finfo) == NULL)
			continue;

		TypeCacheEntry *type = lookup_type_cache(atttypid, TYPECACHE_HASH_EXTENDED_PROC);
		total_width += (type->typlen > 0 ? type->typlen : 4);

		/* Equality queries are unlikely for floating-point types, so we skip them. */
		if (atttypid == FLOAT4OID || atttypid == FLOAT8OID)
			continue;

		/* Add to bloom config */
		bloom_config.columns[valid_columns].attnum = attno;
		bloom_config.columns[valid_columns].name = attname;
		bloom_config.columns[valid_columns].type = atttypid;
		valid_columns++;

		attnums_bitmap = bms_add_member(attnums_bitmap, attno);
	}

	/* Need at least 2 valid columns for composite bloom and the total width must be at least 4
	 * bytes. */
	if (valid_columns < 2 || total_width < 4)
	{
		pfree(bloom_config.columns);
		bms_free(attnums_bitmap);
		return;
	}

	/* Check if this exact bloom already exists */
	if (ts_bmslist_contains_set(*sparse_index_columns, attnums_bitmap))
	{
		pfree(bloom_config.columns);
		bms_free(attnums_bitmap);
		return;
	}

	bloom_config.num_columns = valid_columns;

	/* Column names must be in attnum order for metadata column naming */
	qsort(bloom_config.columns,
		  bloom_config.num_columns,
		  sizeof(SparseIndexColumn),
		  ts_qsort_attrnumber_cmp);

	/* Add the bloom's column set to the list */
	*sparse_index_columns = ts_bmslist_add_set(*sparse_index_columns, attnums_bitmap);

	/* Convert to JSONB and add to array */
	ts_convert_sparse_index_config_to_jsonb(parse_state, &bloom_config.base);
	*has_object = true;

	pfree(bloom_config.columns);
}

static Jsonb *
compression_setting_sparse_index_get_default(Hypertable *ht, CompressionSettings *settings)
{
	bool has_object = false;
	TsBmsList sparse_index_columns = ts_bmslist_create();
	JsonbParseState *parse_state = NULL;

	/*
	 * Sparse indexes are only created automatically if they are not set in compression settings
	 */
	if (!ts_guc_auto_sparse_indexes || !can_set_default_sparse_index(settings))
		return NULL;

	/*
	 * Check which columns have btree indexes. We will create sparse minmax
	 * indexes for them in compressed chunk.
	 */
	Relation rel = table_open(ht->main_table_relid, AccessShareLock);

	ListCell *lc;
	List *index_oids = RelationGetIndexList(rel);

	pushJsonbValue(&parse_state, WJB_BEGIN_ARRAY, NULL);
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

		int num_cols = index_info->ii_NumIndexKeyAttrs;
		if (ts_guc_enable_sparse_index_bloom && num_cols >= 2 &&
			num_cols <= MAX_BLOOM_FILTER_COLUMNS)
		{
			create_composite_bloom(index_info,
								   ht,
								   settings,
								   parse_state,
								   &sparse_index_columns,
								   &has_object);
		}

		for (int i = 0; i < num_cols; i++)
		{
			char *attname;
			Oid atttypid;
			MinmaxIndexColumnConfig minmax_config;
			BloomFilterConfig bloom_config;
			SparseIndexConfigBase *config = NULL;
			TypeCacheEntry *type;
			const int attno = index_info->ii_IndexAttrNumbers[i];
			if (attno == InvalidAttrNumber)
			{
				continue;
			}
			attname = get_attname(ht->main_table_relid, attno, false);
			/* do not create sparse index for orderby columns */
			if (ts_array_is_member(settings->fd.orderby, attname) ||
				ts_array_is_member(settings->fd.segmentby, attname) ||
				ts_bmslist_contains_items(sparse_index_columns, &attno, 1))
				continue;

			atttypid = get_atttype(ht->main_table_relid, attno);

			type = lookup_type_cache(atttypid, TYPECACHE_LT_OPR | TYPECACHE_HASH_EXTENDED_PROC);

			/* construct sparse index config */
			if (ts_guc_enable_sparse_index_bloom &&
				should_create_bloom_sparse_index(atttypid, type, ht->main_table_relid))
			{
				config = &bloom_config.base;
				config->type = _SparseIndexTypeEnumBloom;
				bloom_config.num_columns = 1;
				bloom_config.columns = palloc(1 * sizeof(SparseIndexColumn));
				bloom_config.columns[0].attnum = attno;
				bloom_config.columns[0].name = attname;
				bloom_config.columns[0].type = atttypid;
			}
			else if (OidIsValid(type->lt_opr))
			{
				config = &minmax_config.base;
				config->type = _SparseIndexTypeEnumMinmax;
				minmax_config.col = attname;
			}
			else
				continue;

			config->source = _SparseIndexSourceEnumDefault;

			/* convert to json object */
			ts_convert_sparse_index_config_to_jsonb(parse_state, config);
			sparse_index_columns = ts_bmslist_add_member(sparse_index_columns, &attno, 1);
			has_object = true;
		}
	}
	table_close(rel, AccessShareLock);
	ts_bmslist_free(sparse_index_columns);
	return has_object ? JsonbValueToJsonb(pushJsonbValue(&parse_state, WJB_END_ARRAY, NULL)) : NULL;
}

void
compression_settings_set_defaults(Hypertable *ht, CompressionSettings *settings,
								  WithClauseResult *with_clause_options)
{
	/* orderby arrays should always be in sync either all NULL or none */
	Assert(
		(settings->fd.orderby && settings->fd.orderby_desc && settings->fd.orderby_nullsfirst) ||
		(!settings->fd.orderby && !settings->fd.orderby_desc && !settings->fd.orderby_nullsfirst));

	bool add_orderby_sparse_index = false;
	/* get default settings which will be stored at chunk level */
	if (!(settings->fd.orderby) && with_clause_options[AlterTableFlagOrderBy].is_default)
	{
		if (!settings->fd.segmentby && with_clause_options[AlterTableFlagSegmentBy].is_default)
		{
			settings->fd.segmentby = compression_setting_segmentby_get_default(ht);
		}
		settings->fd.index = ts_remove_orderby_sparse_index(settings);
		OrderBySettings obs = compression_setting_orderby_get_default(ht, settings->fd.segmentby);
		settings->fd.orderby = obs.orderby;
		settings->fd.orderby_desc = obs.orderby_desc;
		settings->fd.orderby_nullsfirst = obs.orderby_nullsfirst;
		add_orderby_sparse_index = settings->fd.index != NULL;
	}

	if (ts_guc_auto_sparse_indexes && can_set_default_sparse_index(settings))
	{
		settings->fd.index = compression_setting_sparse_index_get_default(ht, settings);
		settings->fd.index = ts_add_orderby_sparse_index(settings);
	}
	else if (add_orderby_sparse_index)
	{
		settings->fd.index = ts_add_orderby_sparse_index(settings);
	}

	/* should always be valid, but call as a sanity check */
	validate_compression_index_key_limit(settings);
}

static void
compression_settings_set_manually_for_alter(Hypertable *ht, CompressionSettings *settings,
											WithClauseResult *with_clause_options)
{
	/* orderby arrays should always be in sync either all NULL or none */
	Assert(
		(settings->fd.orderby && settings->fd.orderby_desc && settings->fd.orderby_nullsfirst) ||
		(!settings->fd.orderby && !settings->fd.orderby_desc && !settings->fd.orderby_nullsfirst));

	if (with_clause_options[AlterTableFlagSegmentBy].is_default &&
		with_clause_options[AlterTableFlagOrderBy].is_default &&
		with_clause_options[AlterTableFlagIndex].is_default)
		return;

	bool add_orderby_sparse_index = false;
	if (!with_clause_options[AlterTableFlagSegmentBy].is_default)
	{
		settings->fd.segmentby =
			ts_compress_hypertable_parse_segment_by(with_clause_options[AlterTableFlagSegmentBy],
													ht);
	}

	if (!with_clause_options[AlterTableFlagOrderBy].is_default)
	{
		settings->fd.index = ts_remove_orderby_sparse_index(settings);
		OrderBySettings obs =
			ts_compress_hypertable_parse_order_by(with_clause_options[AlterTableFlagOrderBy], ht);
		obs = add_time_to_order_by_if_not_included(obs, settings->fd.segmentby, ht);
		settings->fd.orderby = obs.orderby;
		settings->fd.orderby_desc = obs.orderby_desc;
		settings->fd.orderby_nullsfirst = obs.orderby_nullsfirst;
		add_orderby_sparse_index = settings->fd.index != NULL;
	}

	if (!with_clause_options[AlterTableFlagIndex].is_default)
	{
		if (!settings->fd.orderby)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot set sparse index option without orderby option"),
					 errdetail("Either set both options or remove both options to trigger default "
							   "values")));
		}
		settings->fd.index =
			ts_compress_hypertable_parse_index(with_clause_options[AlterTableFlagIndex], ht);
		settings->fd.index = ts_add_orderby_sparse_index(settings);
	}
	else if (add_orderby_sparse_index)
	{
		settings->fd.index = ts_add_orderby_sparse_index(settings);
	}

	validate_compression_index_key_limit(settings);

	/* update manual settings */
	ts_compression_settings_update(settings);
}

static void
compression_settings_set_manually_for_create(Hypertable *ht, CompressionSettings *settings,
											 WithClauseResult *with_clause_options)
{
	if (with_clause_options[CreateTableFlagSegmentBy].is_default &&
		with_clause_options[CreateTableFlagOrderBy].is_default &&
		with_clause_options[CreateTableFlagIndex].is_default)
		return;

	bool add_orderby_sparse_index = false;
	if (!with_clause_options[CreateTableFlagSegmentBy].is_default)
	{
		settings->fd.segmentby =
			ts_compress_hypertable_parse_segment_by(with_clause_options[CreateTableFlagSegmentBy],
													ht);
	}

	if (!with_clause_options[CreateTableFlagOrderBy].is_default)
	{
		settings->fd.index = ts_remove_orderby_sparse_index(settings);
		OrderBySettings obs =
			ts_compress_hypertable_parse_order_by(with_clause_options[CreateTableFlagOrderBy], ht);
		obs = add_time_to_order_by_if_not_included(obs, settings->fd.segmentby, ht);
		settings->fd.orderby = obs.orderby;
		settings->fd.orderby_desc = obs.orderby_desc;
		settings->fd.orderby_nullsfirst = obs.orderby_nullsfirst;
		add_orderby_sparse_index = settings->fd.index != NULL;
	}

	if (!with_clause_options[CreateTableFlagIndex].is_default)
	{
		if (!settings->fd.orderby)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot set sparse index option without orderby option"),
					 errdetail("Either set both options or remove both options to trigger default "
							   "values")));
		}
		settings->fd.index =
			ts_compress_hypertable_parse_index(with_clause_options[CreateTableFlagIndex], ht);
		settings->fd.index = ts_add_orderby_sparse_index(settings);
	}
	else if (add_orderby_sparse_index)
	{
		settings->fd.index = ts_add_orderby_sparse_index(settings);
	}

	validate_compression_index_key_limit(settings);

	/* update manual settings */
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
		CompressionSettings *settings =
			ts_compression_settings_get_by_compress_relid(chunk->table_id);
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
	Jsonb *jb = settings->fd.index;

	/* check if the column is a segmentby or orderby column */
	if (settings && (ts_array_is_member(settings->fd.segmentby, name) ||
					 ts_array_is_member(settings->fd.orderby, name)))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot drop orderby or segmentby column from a hypertable with "
						"columnstore enabled")));

	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.compressed_hypertable_id);
	ListCell *lc;
	int num_chunks = list_length(chunks);
	CompressionSettings **chunk_settings = palloc(sizeof(CompressionSettings *) * num_chunks);

	int i = 0;
	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		CompressionSettings *settings =
			ts_compression_settings_get_by_compress_relid(chunk->table_id);
		chunk_settings[i++] = settings;
		if (ts_array_is_member(settings->fd.segmentby, name) ||
			ts_array_is_member(settings->fd.orderby, name))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot drop orderby or segmentby column from a chunk with "
							"columnstore enabled")));
	}

	if (TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		for (int i = 0; i < num_chunks; i++)
		{
			CompressionSettings *comp_settings = chunk_settings[i];
			drop_column_from_compression_table(comp_settings, name);
		}
	}

	/* update the compression settings for the main table */
	if (jb)
	{
		SparseIndexSettings *parsed_settings = ts_convert_to_sparse_index_settings(jb);
		if (parsed_settings)
		{
			bool removed_any = false;
			ListCell *obj_cell = NULL;
			foreach (obj_cell, parsed_settings->objects)
			{
				bool removed = false;
				SparseIndexSettingsObject *obj = (SparseIndexSettingsObject *) lfirst(obj_cell);
				Assert(obj != NULL);
				foreach_ptr(SparseIndexSettingsPair, pair, obj->pairs)
				{
					if (strcmp(pair->key, ts_sparse_index_common_keys[SparseIndexKeyCol]) != 0)
					{
						continue;
					}
					foreach_ptr(const char, value, pair->values)
					{
						if (strcmp(value, name) == 0)
						{
							removed = true;
							break;
						}
					}
					if (removed)
					{
						break;
					}
				}
				/* if the column was removed, we need to remove the object from the list */
				if (removed)
				{
					removed_any = true;
					parsed_settings->objects =
						foreach_delete_current(parsed_settings->objects, obj_cell);
				}
			}
			if (removed_any)
			{
				jb = ts_convert_from_sparse_index_settings(parsed_settings);
				settings->fd.index = jb;
				ts_compression_settings_update(settings);
			}
			ts_free_sparse_index_settings(parsed_settings);
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

	struct RenameFromTo
	{
		char *from;
		char *to;
	};

	if (strncmp(stmt->newname,
				COMPRESSION_COLUMN_METADATA_PREFIX,
				strlen(COMPRESSION_COLUMN_METADATA_PREFIX)) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_RESERVED_NAME),
				 errmsg("cannot convert tables with reserved column prefix '%s' to columnstore",
						COMPRESSION_COLUMN_METADATA_PREFIX)));

	if (!TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht))
	{
		return;
	}

	RenameStmt *compressed_col_stmt = (RenameStmt *) copyObject(stmt);
	RenameStmt *compressed_index_stmt = (RenameStmt *) copyObject(stmt);
	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.compressed_hypertable_id);
	CompressionSettings *ht_settings = NULL;
	ListCell *lc;

	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		compressed_col_stmt->relation =
			makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), -1);
		ExecRenameStmt(compressed_col_stmt);

		List *rename_from_to = NIL;
		CompressionSettings *settings = ts_compression_settings_get(chunk->table_id);
		if (!settings || settings->fd.index == NULL)
		{
			/* only lookup ht settings if we haven't already */
			if (!ht_settings)
			{
				ht_settings = ts_compression_settings_get(ht->main_table_relid);
			}
			settings = ht_settings;
		}

		/* check the minmax and single bloom index columns no matter what the compression settings
		 * says */
		{
			/* handle minmax index */
			struct RenameFromTo *from_to =
				(struct RenameFromTo *) palloc(sizeof(struct RenameFromTo));
			from_to->from =
				compressed_column_metadata_name_v2("min", (const char **) &stmt->subname, 1);
			from_to->to =
				compressed_column_metadata_name_v2("min", (const char **) &stmt->newname, 1);
			rename_from_to = lappend(rename_from_to, from_to);
			from_to = (struct RenameFromTo *) palloc(sizeof(struct RenameFromTo));
			from_to->from =
				compressed_column_metadata_name_v2("max", (const char **) &stmt->subname, 1);
			from_to->to =
				compressed_column_metadata_name_v2("max", (const char **) &stmt->newname, 1);
			rename_from_to = lappend(rename_from_to, from_to);
		}

		{
			/* handle single bloom index */
			struct RenameFromTo *from_to =
				(struct RenameFromTo *) palloc(sizeof(struct RenameFromTo));
			from_to->from = compressed_column_metadata_name_v2(bloom1_column_prefix,
															   (const char **) &stmt->subname,
															   1);
			from_to->to = compressed_column_metadata_name_v2(bloom1_column_prefix,
															 (const char **) &stmt->newname,
															 1);
			rename_from_to = lappend(rename_from_to, from_to);
		}

		if (settings && settings->fd.index != NULL)
		{
			SparseIndexSettings *parsed_settings =
				ts_convert_to_sparse_index_settings(settings->fd.index);
			List *per_column_settings = ts_get_per_column_compression_settings(parsed_settings);
			PerColumnCompressionSettings *per_column_setting =
				per_column_settings ?
					ts_get_per_column_compression_settings_by_column_name(per_column_settings,
																		  stmt->subname) :
					NULL;

			if (per_column_setting != NULL)
			{
				if (per_column_setting->composite_bloom_index_obj_ids != NULL)
				{
					/* one column may participate in multiple composite bloom indices, so we need to
					 * handle all of them */
					int i = -1;
					struct RenameFromTo *from_to = NULL;

					while ((i = bms_next_member(per_column_setting->composite_bloom_index_obj_ids,
												i)) >= 0)
					{
						SparseIndexSettingsObject *obj =
							(SparseIndexSettingsObject *) list_nth(parsed_settings->objects, i);
						Assert(obj != NULL);
						List *column_names = ts_get_column_names_from_parsed_object(obj);
						Assert(column_names != NULL);
						Assert(list_length(column_names) > 1);
						Assert(list_length(column_names) <= MAX_BLOOM_FILTER_COLUMNS);
						char *new_name[MAX_BLOOM_FILTER_COLUMNS] = { NULL };
						int j = 0;
						ListCell *cell = NULL;
						foreach (cell, column_names)
						{
							const char *column_name = (const char *) lfirst(cell);
							if (strcmp(column_name, stmt->subname) == 0)
							{
								new_name[j] = stmt->newname;
							}
							else
							{
								new_name[j] = pstrdup(column_name);
							}
							j++;
						}
						/* handle composite bloom index */
						from_to = (struct RenameFromTo *) palloc(sizeof(struct RenameFromTo));
						from_to->from =
							compressed_column_metadata_name_list_v2(bloom1_column_prefix,
																	column_names);
						from_to->to = compressed_column_metadata_name_v2(bloom1_column_prefix,
																		 (const char **) new_name,
																		 list_length(column_names));
						rename_from_to = lappend(rename_from_to, from_to);
					}
				}
			}
			ts_free_sparse_index_settings(parsed_settings);
		}

		compressed_index_stmt->relation = compressed_col_stmt->relation;
		if (rename_from_to != NULL)
		{
			ListCell *cell = NULL;
			foreach (cell, rename_from_to)
			{
				struct RenameFromTo *from_to = (struct RenameFromTo *) lfirst(cell);
				Assert(from_to != NULL);
				Assert(from_to->from != NULL);
				Assert(from_to->to != NULL);
				if (get_attnum(chunk->table_id, from_to->from) == InvalidAttrNumber)
				{
					continue;
				}

				compressed_index_stmt->subname = from_to->from;
				compressed_index_stmt->newname = from_to->to;
				ExecRenameStmt(compressed_index_stmt);
			}

			list_free_deep(rename_from_to);
		}
	}
}

/*
 * Enables compression for a hypertable without creating initial configuration
 *
 * This is used when creating a hypertable with CREATE TABLE ... WITH (timescaledb.hypertable)
 */
void
tsl_columnstore_setup(Hypertable *ht, WithClauseResult *with_clause_options)
{
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE), RowExclusiveLock);
	Oid ownerid = ts_rel_get_owner(ht->main_table_relid);
	Oid tablespace_oid = get_rel_tablespace(ht->main_table_relid);
	CompressionSettings *settings = ts_compression_settings_create(ht->main_table_relid,
																   InvalidOid,
																   NULL,
																   NULL,
																   NULL,
																   NULL,
																   NULL);

	compression_settings_set_manually_for_create(ht, settings, with_clause_options);
	int compress_htid = compression_hypertable_create(ht, ownerid, tablespace_oid);
	ts_hypertable_set_compressed(ht, compress_htid);

	/* Add default compression policy when compression is enabled via CREATE TABLE WITH */
	/* Use the chunk interval as the compression interval */
	const Dimension *time_dim = hyperspace_get_open_dimension(ht->space, 0);
	if (time_dim != NULL)
	{
		Oid compress_after_type = ts_dimension_get_partition_type(time_dim);
		Datum compress_after_datum;
		if (IS_TIMESTAMP_TYPE(compress_after_type) || IS_UUID_TYPE(compress_after_type))
			compress_after_type = INTERVALOID;

		compress_after_datum =
			ts_internal_to_interval_value(time_dim->fd.interval_length, compress_after_type);

		policy_compression_add_internal(
			ht->main_table_relid,
			compress_after_datum,
			compress_after_type,
			NULL,								   /* created_before */
			DEFAULT_COMPRESSION_SCHEDULE_INTERVAL, /* default_schedule_interval
													*/
			true,								   /* user_defined_schedule_interval */
			true,								   /* if_not_exists */
			false,								   /* fixed_schedule */
			GetCurrentTimestamp() + USECS_PER_DAY, /* initial_start */
			NULL /* timezone */);
	}
}
