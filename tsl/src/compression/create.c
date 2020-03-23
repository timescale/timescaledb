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
#include <catalog/indexing.h>
#if PG11_GE
#include <catalog/pg_constraint_d.h>
#endif
#include <catalog/pg_constraint.h>
#include <catalog/pg_type.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/toasting.h>
#include <catalog/objectaccess.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <miscadmin.h>
#include <nodes/makefuncs.h>
#include <storage/lmgr.h>
#include <utils/array.h>
#include <utils/builtins.h>
#include <utils/rel.h>
#include <utils/syscache.h>
#include <utils/typcache.h>

#include "catalog.h"
#include "create.h"
#include "chunk.h"
#include "chunk_index.h"
#include "continuous_agg.h"
#include "compat.h"
#include "compression_with_clause.h"
#include "compression.h"
#include "hypertable_cache.h"
#include "hypertable_compression.h"
#include "custom_type_cache.h"
#include "license.h"
#include "trigger.h"
#include "utils.h"
#include "bgw_policy/compress_chunks.h"

/* entrypoint
 * tsl_process_compress_table : is the entry point.
 */
typedef struct CompressColInfo
{
	int numcols;
	FormData_hypertable_compression
		*col_meta;	/* metadata about columns from src hypertable that will be compressed*/
	List *coldeflist; /*list of ColumnDef for the compressed column */
} CompressColInfo;

static void compresscolinfo_init(CompressColInfo *cc, Oid srctbl_relid, List *segmentby_cols,
								 List *orderby_cols);
static void compresscolinfo_add_catalog_entries(CompressColInfo *compress_cols, int32 htid);

#define PRINT_COMPRESSION_TABLE_NAME(buf, prefix, hypertable_id)                                   \
	do                                                                                             \
	{                                                                                              \
		int ret = snprintf(buf, NAMEDATALEN, prefix, hypertable_id);                               \
		if (ret < 0 || ret > NAMEDATALEN)                                                          \
		{                                                                                          \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INTERNAL_ERROR),                                              \
					 errmsg(" bad compression hypertable internal name")));                        \
		}                                                                                          \
	} while (0);

static enum CompressionAlgorithms
get_default_algorithm_id(Oid typeoid)
{
	switch (typeoid)
	{
		case INT4OID:
		case INT2OID:
		case INT8OID:
		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return COMPRESSION_ALGORITHM_DELTADELTA;

		case FLOAT4OID:
		case FLOAT8OID:
			return COMPRESSION_ALGORITHM_GORILLA;

		case NUMERICOID:
			return COMPRESSION_ALGORITHM_ARRAY;

		default:
		{
			/* use dictitionary if possible, otherwise use array */
			TypeCacheEntry *tentry =
				lookup_type_cache(typeoid, TYPECACHE_EQ_OPR_FINFO | TYPECACHE_HASH_PROC_FINFO);
			if (tentry->hash_proc_finfo.fn_addr == NULL || tentry->eq_opr_finfo.fn_addr == NULL)
				return COMPRESSION_ALGORITHM_ARRAY;
			return COMPRESSION_ALGORITHM_DICTIONARY;
		}
	}
}

static char *
compression_column_segment_metadata_name(const FormData_hypertable_compression *fd,
										 const char *type)
{
	char *buf = palloc(sizeof(char) * NAMEDATALEN);
	int ret;

	Assert(fd->orderby_column_index > 0);
	ret = snprintf(buf,
				   NAMEDATALEN,
				   COMPRESSION_COLUMN_METADATA_PREFIX "%s_%d",
				   type,
				   fd->orderby_column_index);
	if (ret < 0 || ret > NAMEDATALEN)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("bad segment metadata column name")));
	}
	return buf;
}

char *
compression_column_segment_min_name(const FormData_hypertable_compression *fd)
{
	return compression_column_segment_metadata_name(fd, "min");
}

char *
compression_column_segment_max_name(const FormData_hypertable_compression *fd)
{
	return compression_column_segment_metadata_name(fd, "max");
}

static void
compresscolinfo_add_metadata_columns(CompressColInfo *cc, Relation uncompressed_rel)
{
	/* additional metadata columns.
	 * these are not listed in hypertable_compression catalog table
	 * and so only has a ColDef entry */
	int colno;

	/* count column */
	cc->coldeflist = lappend(cc->coldeflist,

							 /* count of the number of uncompressed rows */
							 makeColumnDef(COMPRESSION_COLUMN_METADATA_COUNT_NAME,
										   INT4OID,
										   -1 /* typemod */,
										   0 /*collation*/));
	/* sequence_num column */
	cc->coldeflist = lappend(cc->coldeflist,

							 /* count of the number of uncompressed rows */
							 makeColumnDef(COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME,
										   INT4OID,
										   -1 /* typemod */,
										   0 /*collation*/));

	for (colno = 0; colno < cc->numcols; colno++)
	{
		if (cc->col_meta[colno].orderby_column_index > 0)
		{
			FormData_hypertable_compression fd = cc->col_meta[colno];
			AttrNumber col_attno = get_attnum(uncompressed_rel->rd_id, NameStr(fd.attname));
			Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(uncompressed_rel),
												   AttrNumberGetAttrOffset(col_attno));
			TypeCacheEntry *type = lookup_type_cache(attr->atttypid, TYPECACHE_LT_OPR);

			if (!OidIsValid(type->lt_opr))
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_FUNCTION),
						 errmsg("invalid order by column type: could not identify an less-than "
								"operator for type %s",
								format_type_be(attr->atttypid))));

			/* segment_meta min and max columns */
			cc->coldeflist =
				lappend(cc->coldeflist,
						makeColumnDef(compression_column_segment_min_name(&cc->col_meta[colno]),
									  attr->atttypid,
									  -1 /* typemod */,
									  0 /*collation*/));
			cc->coldeflist =
				lappend(cc->coldeflist,
						makeColumnDef(compression_column_segment_max_name(&cc->col_meta[colno]),
									  attr->atttypid,
									  -1 /* typemod */,
									  0 /*collation*/));
		}
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
static void
compresscolinfo_init(CompressColInfo *cc, Oid srctbl_relid, List *segmentby_cols,
					 List *orderby_cols)
{
	Relation rel;
	TupleDesc tupdesc;
	int i, colno, attno;
	int16 *segorder_colindex;
	int seg_attnolen = 0;
	ListCell *lc;
	Oid compresseddata_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;

	seg_attnolen = list_length(segmentby_cols);
	rel = table_open(srctbl_relid, AccessShareLock);
	segorder_colindex = palloc0(sizeof(int32) * (rel->rd_att->natts));
	tupdesc = rel->rd_att;
	i = 1;

#if PG12_LT
	if (rel->rd_rel->relhasoids)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("compression cannot be used on table with OIDs")));
#endif

	foreach (lc, segmentby_cols)
	{
		CompressedParsedCol *col = (CompressedParsedCol *) lfirst(lc);
		AttrNumber col_attno = get_attnum(rel->rd_id, NameStr(col->colname));
		if (col_attno == InvalidAttrNumber)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column \"%s\" specified in option timescaledb.compress_segmentby does "
							"not exist",
							NameStr(col->colname))));
		}
		segorder_colindex[col_attno - 1] = i++;
	}
	/* the column indexes are numbered as seg_attnolen + <orderby_index>
	 */
	Assert(seg_attnolen == (i - 1));
	foreach (lc, orderby_cols)
	{
		CompressedParsedCol *col = (CompressedParsedCol *) lfirst(lc);
		AttrNumber col_attno = get_attnum(rel->rd_id, NameStr(col->colname));
		if (col_attno == InvalidAttrNumber)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column \"%s\" in option timescaledb.compress_orderby does not exist",
							NameStr(col->colname))));
		}
		/* check if orderby_cols and segmentby_cols are distinct */
		if (segorder_colindex[col_attno - 1] != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cannot use column \"%s\" in both timescaledb.compress_orderby and "
							"timescaledb.compress_segmentby",
							NameStr(col->colname))));
		}
		segorder_colindex[col_attno - 1] = i++;
	}

	cc->numcols = 0;
	cc->col_meta = palloc0(sizeof(FormData_hypertable_compression) * tupdesc->natts);
	cc->coldeflist = NIL;
	colno = 0;
	for (attno = 0; attno < tupdesc->natts; attno++)
	{
		Oid attroid = InvalidOid;
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

		namestrcpy(&cc->col_meta[colno].attname, NameStr(attr->attname));
		if (segorder_colindex[attno] > 0)
		{
			if (segorder_colindex[attno] <= seg_attnolen)
			{
				attroid = attr->atttypid; /*segment by columns have original type */
				cc->col_meta[colno].segmentby_column_index = segorder_colindex[attno];
			}
			else
			{
				int orderby_index = segorder_colindex[attno] - seg_attnolen;
				CompressedParsedCol *ordercol = list_nth(orderby_cols, orderby_index - 1);
				cc->col_meta[colno].orderby_column_index = orderby_index;
				cc->col_meta[colno].orderby_asc = ordercol->asc;
				cc->col_meta[colno].orderby_nullsfirst = ordercol->nullsfirst;
			}
		}
		if (attroid == InvalidOid)
		{
			attroid = compresseddata_oid; /* default type for column */
			cc->col_meta[colno].algo_id = get_default_algorithm_id(attr->atttypid);
		}
		else
		{
			cc->col_meta[colno].algo_id = 0; // invalid algo number
		}
		coldef = makeColumnDef(NameStr(attr->attname), attroid, -1 /*typmod*/, 0 /*collation*/);
		cc->coldeflist = lappend(cc->coldeflist, coldef);
		colno++;
	}
	cc->numcols = colno;
	compresscolinfo_add_metadata_columns(cc, rel);
	pfree(segorder_colindex);
	table_close(rel, AccessShareLock);
}

/* modify storage attributes for toast table columns attached to the
 * compression table
 */
static void
modify_compressed_toast_table_storage(CompressColInfo *cc, Oid compress_relid)
{
	int colno;
	List *cmds = NIL;
	for (colno = 0; colno < cc->numcols; colno++)
	{
		// get storage type for columns which have compression on
		if (cc->col_meta[colno].algo_id != 0)
		{
			CompressionStorage stor = compression_get_toast_storage(cc->col_meta[colno].algo_id);
			if (stor != TOAST_STORAGE_EXTERNAL)
			/* external is default storage for toast columns */
			{
				AlterTableCmd *cmd = makeNode(AlterTableCmd);
				cmd->subtype = AT_SetStorage;
				cmd->name = pstrdup(NameStr(cc->col_meta[colno].attname));
				Assert(stor == TOAST_STORAGE_EXTENDED);
				cmd->def = (Node *) makeString("extended");
				cmds = lappend(cmds, cmd);
			}
		}
	}
	if (cmds != NIL)
	{
		AlterTableInternal(compress_relid, cmds, false);
	}
}

/* prevent concurrent transactions from inserting into
 * hypertable_compression for the same table, acquire the lock but don't free
 * here
 * i.e. 2 concurrent ALTER TABLE to compressed will not succeed.
 */
static void
compresscolinfo_add_catalog_entries(CompressColInfo *compress_cols, int32 htid)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	Datum values[Natts_hypertable_compression];
	bool nulls[Natts_hypertable_compression] = { false };
	TupleDesc desc;
	int i;
	CatalogSecurityContext sec_ctx;

	rel = table_open(catalog_get_table_id(catalog, HYPERTABLE_COMPRESSION), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	for (i = 0; i < compress_cols->numcols; i++)
	{
		FormData_hypertable_compression *fd = &compress_cols->col_meta[i];
		fd->hypertable_id = htid;
		ts_hypertable_compression_fill_tuple_values(fd, &values[0], &nulls[0]);
		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		ts_catalog_insert_values(rel, desc, values, nulls);
		ts_catalog_restore_user(&sec_ctx);
	}

	table_close(rel, NoLock); /*lock will be released at end of transaction only*/
}

static void
create_compressed_table_indexes(Oid compresstable_relid, CompressColInfo *compress_cols)
{
	Cache *hcache;
	Hypertable *ht =
		ts_hypertable_cache_get_cache_and_entry(compresstable_relid, CACHE_FLAG_NONE, &hcache);
	IndexStmt stmt = {
		.type = T_IndexStmt,
		.accessMethod = DEFAULT_INDEX_TYPE,
		.idxname = NULL,
		.relation = makeRangeVar(NameStr(ht->fd.schema_name), NameStr(ht->fd.table_name), 0),
		.tableSpace = get_tablespace_name(get_rel_tablespace(ht->main_table_relid)),
	};
	IndexElem sequence_num_elem = {
		.type = T_IndexElem,
		.name = COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME,
	};
	int i;
	for (i = 0; i < compress_cols->numcols; i++)
	{
		NameData index_name;
		ObjectAddress index_addr;
		HeapTuple index_tuple;
		FormData_hypertable_compression *col = &compress_cols->col_meta[i];
		IndexElem segment_elem = { .type = T_IndexElem, .name = NameStr(col->attname) };

		if (col->segmentby_column_index <= 0)
			continue;

		stmt.indexParams = list_make2(&segment_elem, &sequence_num_elem);
		index_addr = DefineIndexCompat(ht->main_table_relid,
									   &stmt,
									   InvalidOid,
									   false,  /* is alter table */
									   false,  /* check rights */
									   false,  /* skip_build */
									   false); /* quiet */
		index_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(index_addr.objectId));

		if (!HeapTupleIsValid(index_tuple))
			elog(ERROR, "cache lookup failed for index relid %d", index_addr.objectId);
		index_name = ((Form_pg_class) GETSTRUCT(index_tuple))->relname;
		elog(NOTICE,
			 "adding index %s ON %s.%s USING BTREE(%s, %s)",
			 NameStr(index_name),
			 NameStr(ht->fd.schema_name),
			 NameStr(ht->fd.table_name),
			 NameStr(col->attname),
			 COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME);
		ReleaseSysCache(index_tuple);
	}

	ts_cache_release(hcache);
}

static void
set_statistics_on_compressed_table(Oid compressed_table_id)
{
	Relation table_rel = table_open(compressed_table_id, ShareUpdateExclusiveLock);
	Relation attrelation = table_open(AttributeRelationId, RowExclusiveLock);
	TupleDesc table_desc = RelationGetDescr(table_rel);
	Oid compressed_data_type = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;
	for (int i = 0; i < table_desc->natts; i++)
	{
		Form_pg_attribute attrtuple;
		HeapTuple tuple;
		Form_pg_attribute col_attr = TupleDescAttr(table_desc, i);

		/* skip system columns */
		if (col_attr->attnum <= 0)
			continue;

		tuple = SearchSysCacheCopyAttName(compressed_table_id, NameStr(col_attr->attname));

		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of compressed table \"%s\" does not exist",
							NameStr(col_attr->attname),
							RelationGetRelationName(table_rel))));

		attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

		/* the planner should never look at compressed column statistics because
		 * it will not understand them. Statistics on the other columns,
		 * segmentbys and metadata, are very important, so we increase their
		 * target.
		 */
		if (col_attr->atttypid == compressed_data_type)
			attrtuple->attstattarget = 0;
		else
			attrtuple->attstattarget = 1000;

		CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

		InvokeObjectPostAlterHook(RelationRelationId, compressed_table_id, attrtuple->attnum);
		heap_freetuple(tuple);
	}

	table_close(attrelation, NoLock);
	table_close(table_rel, NoLock);
}

#if !PG96 && !PG10
static void
set_toast_tuple_target_on_compressed(Oid compressed_table_id)
{
	DefElem def_elem = {
		.type = T_DefElem,
		.defname = "toast_tuple_target",
		.arg = (Node *) makeInteger(128),
		.defaction = DEFELEM_SET,
		.location = -1,
	};
	AlterTableCmd cmd = {
		.type = T_AlterTableCmd,
		.subtype = AT_SetRelOptions,
		.def = (Node *) list_make1(&def_elem),
	};
	AlterTableInternal(compressed_table_id, list_make1(&cmd), true);
}
#endif

static int32
create_compression_table(Oid owner, CompressColInfo *compress_cols)
{
	ObjectAddress tbladdress;
	char relnamebuf[NAMEDATALEN];
	CatalogSecurityContext sec_ctx;
	Datum toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	Oid compress_relid;

	CreateStmt *create;
	RangeVar *compress_rel;
	int32 compress_hypertable_id;

	create = makeNode(CreateStmt);
	create->tableElts = compress_cols->coldeflist;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = NULL;
	create->oncommit = ONCOMMIT_NOOP;
	create->tablespacename = NULL;
	create->if_not_exists = false;

	/* create the compression table */
	/* NewRelationCreateToastTable calls CommandCounterIncrement */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	compress_hypertable_id = ts_catalog_table_next_seq_id(ts_catalog_get(), HYPERTABLE);
	PRINT_COMPRESSION_TABLE_NAME(relnamebuf, "_compressed_hypertable_%d", compress_hypertable_id);
	compress_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);

	create->relation = compress_rel;
	tbladdress = DefineRelationCompat(create, RELKIND_RELATION, owner, NULL, NULL);
	CommandCounterIncrement();
	compress_relid = tbladdress.objectId;
	toast_options =
		transformRelOptions((Datum) 0, create->options, "toast", validnsps, true, false);
	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
	NewRelationCreateToastTable(compress_relid, toast_options);
	ts_catalog_restore_user(&sec_ctx);
	modify_compressed_toast_table_storage(compress_cols, compress_relid);
	ts_hypertable_create_compressed(compress_relid, compress_hypertable_id);

	set_statistics_on_compressed_table(compress_relid);

#if !PG96 && !PG10
	set_toast_tuple_target_on_compressed(compress_relid);
#endif

	create_compressed_table_indexes(compress_relid, compress_cols);
	return compress_hypertable_id;
}

Chunk *
create_compress_chunk_table(Hypertable *compress_ht, Chunk *src_chunk)
{
	Hyperspace *hs = compress_ht->space;
	Catalog *catalog = ts_catalog_get();
	CatalogSecurityContext sec_ctx;
	Chunk *compress_chunk;

	/* Create a new chunk based on the hypercube */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	compress_chunk =
		ts_chunk_create_base(ts_catalog_table_next_seq_id(catalog, CHUNK), hs->num_dimensions);
	ts_catalog_restore_user(&sec_ctx);

	compress_chunk->fd.hypertable_id = hs->hypertable_id;
	compress_chunk->cube = src_chunk->cube;
	compress_chunk->hypertable_relid = compress_ht->main_table_relid;
	compress_chunk->constraints = ts_chunk_constraints_alloc(1, CurrentMemoryContext);
	namestrcpy(&compress_chunk->fd.schema_name, INTERNAL_SCHEMA_NAME);
	snprintf(compress_chunk->fd.table_name.data,
			 NAMEDATALEN,
			 "compress%s_%d_chunk",
			 NameStr(compress_ht->fd.associated_table_prefix),
			 compress_chunk->fd.id);

	;

	/* Insert chunk */
	ts_chunk_insert_lock(compress_chunk, RowExclusiveLock);

	/* only add inheritable constraints. no dimension constraints */
	ts_chunk_constraints_add_inheritable_constraints(compress_chunk->constraints,
													 compress_chunk->fd.id,
													 compress_chunk->hypertable_relid);

	ts_chunk_constraints_insert_metadata(compress_chunk->constraints);

	/* Create the actual table relation for the chunk
	 * Note that we have to pick the tablespace here as the compressed ht doesn't have dimensions
	 * on which to base this decision. We simply pick the same tablespace as the uncompressed chunk
	 * for now.
	 */
	compress_chunk->table_id =
		ts_chunk_create_table(compress_chunk,
							  compress_ht,
							  get_tablespace_name(get_rel_tablespace(src_chunk->table_id)));

	if (!OidIsValid(compress_chunk->table_id))
		elog(ERROR, "could not create compressed chunk table");

	/* Create the chunk's constraints*/
	ts_chunk_constraints_create(compress_chunk->constraints,
								compress_chunk->table_id,
								compress_chunk->fd.id,
								compress_chunk->hypertable_relid,
								compress_chunk->fd.hypertable_id);

	ts_trigger_create_all_on_chunk(compress_ht, compress_chunk);

	ts_chunk_index_create_all(compress_chunk->fd.hypertable_id,
							  compress_chunk->hypertable_relid,
							  compress_chunk->fd.id,
							  compress_chunk->table_id);

	return compress_chunk;
}

/* Add  the hypertable time column to the end of the orderby list if
 * it's not already in the orderby or segmentby. */
static List *
add_time_to_order_by_if_not_included(List *orderby_cols, List *segmentby_cols, Hypertable *ht)
{
	ListCell *lc;
	Dimension *time_dim;
	char *time_col_name;
	bool found = false;

	time_dim = hyperspace_get_open_dimension(ht->space, 0);
	time_col_name = get_attname_compat(ht->main_table_relid, time_dim->column_attno, false);

	foreach (lc, orderby_cols)
	{
		CompressedParsedCol *col = (CompressedParsedCol *) lfirst(lc);
		if (namestrcmp(&col->colname, time_col_name) == 0)
			found = true;
	}
	foreach (lc, segmentby_cols)
	{
		CompressedParsedCol *col = (CompressedParsedCol *) lfirst(lc);
		if (namestrcmp(&col->colname, time_col_name) == 0)
			found = true;
	}

	if (!found)
	{
		/* Add time DESC NULLS FIRST to order by list */
		CompressedParsedCol *col = palloc(sizeof(*col));
		*col = (CompressedParsedCol){
			.index = list_length(orderby_cols),
			.asc = false,
			.nullsfirst = true,
		};
		namestrcpy(&col->colname, time_col_name);
		orderby_cols = lappend(orderby_cols, col);
	}
	return orderby_cols;
}

static FormData_hypertable_compression *
get_col_info_for_attnum(Hypertable *ht, CompressColInfo *colinfo, AttrNumber attno)
{
	char *attr_name = get_attname_compat(ht->main_table_relid, attno, false);
	int colno;

	for (colno = 0; colno < colinfo->numcols; colno++)
	{
		if (namestrcmp(&colinfo->col_meta[colno].attname, attr_name) == 0)
			return &colinfo->col_meta[colno];
	}
	return NULL;
}

/* returns list of constraints that need to be cloned on the compressed hypertable
 * This is limited to foreign key constraints now
 */
static List *
validate_existing_constraints(Hypertable *ht, CompressColInfo *colinfo)
{
	Oid relid = ht->main_table_relid;
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
				ObjectIdGetDatum(relid));

	scan = systable_beginscan(pg_constr, ConstraintRelidTypidNameIndexId, true, NULL, 1, &scankey);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_constraint form = (Form_pg_constraint) GETSTRUCT(tuple);

		/* we check primary ,unique and exclusion constraints.
		 * move foreign key constarints over to compression table
		 * ignore triggers
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
#if PG12_LT
				Oid oid = HeapTupleGetOid(tuple);
#else
				Oid oid = heap_getattr(tuple,
									   Anum_pg_constraint_oid,
									   RelationGetDescr(pg_constr),
									   &is_null);
#endif
				elog(ERROR, "null conkey for constraint %u", oid);
			}

			arr = DatumGetArrayTypeP(adatum); /* ensure not toasted */
			numkeys = ARR_DIMS(arr)[0];
			if (ARR_NDIM(arr) != 1 || numkeys < 0 || ARR_HASNULL(arr) ||
				ARR_ELEMTYPE(arr) != INT2OID)
				elog(ERROR, "conkey is not a 1-D smallint array");
			attnums = (int16 *) ARR_DATA_PTR(arr);
			for (j = 0; j < numkeys; j++)
			{
				FormData_hypertable_compression *col_def =
					get_col_info_for_attnum(ht, colinfo, attnums[j]);

				if (col_def == NULL)
					elog(ERROR, "missing column definition for constraint");

				if (form->contype == CONSTRAINT_FOREIGN)
				{
					/* is this a segment-by column */
					if (col_def->segmentby_column_index < 1)
					{
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("constraint \"%s\" requires column \"%s\" to be a "
										"timescaledb.compress_segmentby "
										"column for compression",
										NameStr(form->conname),
										NameStr(col_def->attname)),
								 errhint("Only segment by columns can be used in foreign key "
										 "constraints on hypertables that are compressed.")));
					}
					else
					{
						Name conname = palloc0(NAMEDATALEN);
						namecpy(conname, &form->conname);
						conlist = lappend(conlist, conname);
					}
				}
				else
				{
					/* is colno  a segment-by or order_by column */
					if (col_def->segmentby_column_index < 1 && col_def->orderby_column_index < 1)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("constraint \"%s\" requires column \"%s\" to be a "
										"timescaledb.compress_segmentby or "
										"timescaledb.compress_orderby column for compression",
										NameStr(form->conname),
										NameStr(col_def->attname)),
								 errhint("Only columns specified in timescaledb.compress_segmentby "
										 "and timescaledb.compress_orderby can be used in "
										 "constraints on hypertables that are compressed.")));
				}
			}
		}
	}

	systable_endscan(scan);
	table_close(pg_constr, AccessShareLock);
	return conlist;
}

static void
check_modify_compression_options(Hypertable *ht, WithClauseResult *with_clause_options)
{
	bool compress_enable = DatumGetBool(with_clause_options[CompressEnabled].parsed);
	bool compression_has_policy;
	bool compressed_chunks_exist;
	bool compression_already_enabled = TS_HYPERTABLE_HAS_COMPRESSION(ht);
	compressed_chunks_exist =
		compression_already_enabled && ts_chunk_exists_with_compression(ht->fd.id);
	compression_has_policy =
		compression_already_enabled && ts_bgw_policy_compress_chunks_find_by_hypertable(ht->fd.id);

	if (compressed_chunks_exist)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change compression options as compressed chunks already exist for "
						"this table")));

	if (compression_has_policy)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change compression options as a compression policy exists on the "
						"table")));

	/* Require both order by and segment by when altering if they were previously set because
	 * otherwise it's not clear what the default value means: does it mean leave as-is or is it an
	 * empty list. */
	if (compress_enable && compression_already_enabled)
	{
		List *info = ts_hypertable_compression_get(ht->fd.id);
		ListCell *lc;
		bool segment_by_set = false;
		bool order_by_set = false;

		foreach (lc, info)
		{
			FormData_hypertable_compression *fd = lfirst(lc);
			if (fd->segmentby_column_index > 0)
				segment_by_set = true;
			if (fd->orderby_column_index > 0)
				order_by_set = true;
		}
		if (with_clause_options[CompressOrderBy].is_default && order_by_set)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg(
						 "need to specify timescaledb.compress_orderby if it was previously set")));

		if (with_clause_options[CompressSegmentBy].is_default && segment_by_set)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("need to specify timescaledb.compress_segmentby if it was previously "
							"set")));
	}
}

static void
drop_existing_compression_table(Hypertable *ht)
{
	Hypertable *compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
	if (compressed == NULL)
		elog(ERROR, "compression enabled but no compressed hypertable found");
	/* need to drop the old compressed hypertable in case the segment by columns changed (and
	 * thus the column types of compressed hypertable need to change) */
	ts_hypertable_drop(compressed, DROP_RESTRICT);
	ts_hypertable_compression_delete_by_hypertable_id(ht->fd.id);
	ts_hypertable_unset_compressed_id(ht);
}

static bool
disable_compression(Hypertable *ht, WithClauseResult *with_clause_options)
{
	bool compression_already_enabled = TS_HYPERTABLE_HAS_COMPRESSION(ht);
	if (!with_clause_options[CompressOrderBy].is_default ||
		!with_clause_options[CompressSegmentBy].is_default)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot set additional compression options when disabling compression")));

	if (!compression_already_enabled)
		/* compression is not enabled, so just return */
		return false;
	/* compression is enabled. can we turn it off? */
	check_modify_compression_options(ht, with_clause_options);
	drop_existing_compression_table(ht);
	ts_hypertable_unset_compressed_id(ht);
	return true;
}

/*
 * enables compression for the passed in table by
 * creating a compression hypertable with special properties
Note:
 caller should check security permissions
*/
bool
tsl_process_compress_table(AlterTableCmd *cmd, Hypertable *ht,
						   WithClauseResult *with_clause_options)
{
	int32 compress_htid;
	struct CompressColInfo compress_cols;
	bool compress_enable = DatumGetBool(with_clause_options[CompressEnabled].parsed);
	Oid ownerid;
	List *segmentby_cols;
	List *orderby_cols;
	ContinuousAggHypertableStatus caggstat;
	List *constraint_list = NIL;

	/*check this is not a special internally created hypertable
	 * i.e. continuous agg table or compression hypertable
	 */
	caggstat = ts_continuous_agg_hypertable_status(ht->fd.id);
	if (!(caggstat == HypertableIsRawTable || caggstat == HypertableIsNotContinuousAgg))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("continuous aggregate tables do not support compression")));
	}
	if (ht->fd.compressed)
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

	/* Lock the uncompressed ht in exclusive mode and keep till end of txn */
	LockRelationOid(ht->main_table_relid, AccessExclusiveLock);

	/* reload info after lock */
	ht = ts_hypertable_get_by_id(ht->fd.id);
	if (!compress_enable)
	{
		return disable_compression(ht, with_clause_options);
	}
	if (TS_HYPERTABLE_HAS_COMPRESSION(ht))
		check_modify_compression_options(ht, with_clause_options);

	ownerid = ts_rel_get_owner(ht->main_table_relid);
	segmentby_cols = ts_compress_hypertable_parse_segment_by(with_clause_options, ht);
	orderby_cols = ts_compress_hypertable_parse_order_by(with_clause_options, ht);
	orderby_cols = add_time_to_order_by_if_not_included(orderby_cols, segmentby_cols, ht);
	compresscolinfo_init(&compress_cols, ht->main_table_relid, segmentby_cols, orderby_cols);
	/* check if we can create a compressed hypertable with existing constraints */
	constraint_list = validate_existing_constraints(ht, &compress_cols);

	/* take explicit locks on catalog tables and keep them till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE), RowExclusiveLock);
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE_COMPRESSION),
					RowExclusiveLock);

	if (TS_HYPERTABLE_HAS_COMPRESSION(ht))
	{
		/* compression is enabled */
		drop_existing_compression_table(ht);
	}

	compress_htid = create_compression_table(ownerid, &compress_cols);
	ts_hypertable_set_compressed_id(ht, compress_htid);

	compresscolinfo_add_catalog_entries(&compress_cols, ht->fd.id);
	/*add the constraints to the new compressed hypertable */
	ht = ts_hypertable_get_by_id(ht->fd.id); /*reload updated info*/
	ts_hypertable_clone_constraints_to_compressed(ht, constraint_list);

	/* do not release any locks, will get released by xact end */
	return true;
}
