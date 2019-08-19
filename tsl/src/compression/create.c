/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#include <postgres.h>
#include <miscadmin.h>
#include <access/heapam.h>
#include <access/reloptions.h>
#include <access/tupdesc.h>
#include <access/xact.h>
#include <catalog/pg_type.h>
#include <catalog/toasting.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <nodes/makefuncs.h>
#include <storage/lmgr.h>
#include <utils/builtins.h>
#include <utils/rel.h>

#include "catalog.h"
#include "compat.h"
#include "create.h"
#include "chunk.h"
#include "chunk_index.h"
#include "trigger.h"
#include "scan_iterator.h"
#include "hypertable_cache.h"
#include "compression_with_clause.h"
#include "compression.h"
#include "hypertable_compression.h"

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

#define COMPRESSEDDATA_TYPE_NAME "_timescaledb_internal.compressed_data"

static enum CompressionAlgorithms
get_default_algorithm_id(Oid typeoid)
{
	switch (typeoid)
	{
		case INT4OID:
		case INT2OID:
		case INT8OID:
		case INTERVALOID:
		case DATEOID:
		case TIMESTAMPOID:
		{
			return COMPRESSION_ALGORITHM_DELTADELTA;
		}
		case FLOAT4OID:
		case FLOAT8OID:
		{
			return COMPRESSION_ALGORITHM_GORILLA;
		}
		case NUMERICOID:
		{
			return COMPRESSION_ALGORITHM_ARRAY;
		}
		case TEXTOID:
		case CHAROID:
		{
			return COMPRESSION_ALGORITHM_DICTIONARY;
		}
		default:
			return COMPRESSION_ALGORITHM_DICTIONARY;
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
	int32 *segorder_colindex;
	int seg_attnolen = 0;
	ListCell *lc;
	const Oid compresseddata_oid =
		DatumGetObjectId(DirectFunctionCall1(regtypein, CStringGetDatum(COMPRESSEDDATA_TYPE_NAME)));

	if (!OidIsValid(compresseddata_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" does not exist", COMPRESSEDDATA_TYPE_NAME)));
	seg_attnolen = list_length(segmentby_cols);
	rel = relation_open(srctbl_relid, AccessShareLock);
	segorder_colindex = palloc0(sizeof(int32) * (rel->rd_att->natts));
	tupdesc = rel->rd_att;
	i = 1;
	foreach (lc, segmentby_cols)
	{
		CompressedParsedCol *col = (CompressedParsedCol *) lfirst(lc);
		AttrNumber col_attno = attno_find_by_attname(tupdesc, &col->colname);
		if (col_attno == InvalidAttrNumber)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column %s in compress_segmentby list does not exist",
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
		AttrNumber col_attno = attno_find_by_attname(tupdesc, &col->colname);
		if (col_attno == InvalidAttrNumber)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("column %s in compress_orderby list does not exist",
							NameStr(col->colname))));
		}
		/* check if orderby_cols and segmentby_cols are distinct */
		if (segorder_colindex[col_attno - 1] != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("cannot use the same column %s in compress_orderby and "
							"compress_segmentby",
							NameStr(col->colname))));
		}
		segorder_colindex[col_attno - 1] = i++;
	}

	cc->numcols = 0;
	cc->col_meta = palloc0(sizeof(FormData_hypertable_compression) * tupdesc->natts);
	cc->coldeflist = list_make1(
		/* count of the number of uncompressed rows */
		makeColumnDef(COMPRESSION_COLUMN_METADATA_COUNT_NAME,
					  INT4OID,
					  -1 /* typemod */,
					  0 /*collation*/));
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
	relation_close(rel, AccessShareLock);
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

	rel = heap_open(catalog_get_table_id(catalog, HYPERTABLE_COMPRESSION), RowExclusiveLock);
	desc = RelationGetDescr(rel);

	for (i = 0; i < compress_cols->numcols; i++)
	{
		FormData_hypertable_compression *fd = &compress_cols->col_meta[i];
		fd->hypertable_id = htid;
		hypertable_compression_fill_tuple_values(fd, &values[0], &nulls[0]);
		ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
		ts_catalog_insert_values(rel, desc, values, nulls);
		ts_catalog_restore_user(&sec_ctx);
	}

	heap_close(rel, NoLock); /*lock will be released at end of transaction only*/
}

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
	ts_hypertable_create_compressed(compress_relid, compress_hypertable_id);
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
		ts_chunk_create_stub(ts_catalog_table_next_seq_id(catalog, CHUNK), hs->num_dimensions);
	ts_catalog_restore_user(&sec_ctx);

	compress_chunk->fd.hypertable_id = hs->hypertable_id;
	compress_chunk->cube = src_chunk->cube;
	compress_chunk->hypertable_relid = compress_ht->main_table_relid;
	namestrcpy(&compress_chunk->fd.schema_name, INTERNAL_SCHEMA_NAME);
	snprintf(compress_chunk->fd.table_name.data,
			 NAMEDATALEN,
			 "compress%s_%d_chunk",
			 NameStr(compress_ht->fd.associated_table_prefix),
			 compress_chunk->fd.id);
	compress_chunk->constraints = NULL;

	/* Insert chunk */
	ts_chunk_insert_lock(compress_chunk, RowExclusiveLock);
	/* Create the actual table relation for the chunk */
	compress_chunk->table_id = ts_chunk_create_table(compress_chunk, compress_ht);

	if (!OidIsValid(compress_chunk->table_id))
		elog(ERROR, "could not create compress chunk table");

	/* compressed chunk has no constraints. But inherits indexes and triggers
	 * from the compressed hypertable
	 */

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
	bool compression_already_enabled;
	bool compressed_chunks_exist;

	/* Lock the uncompressed ht in exclusive mode and keep till end of txn */
	LockRelationOid(ht->main_table_relid, AccessExclusiveLock);

	/* reload info after lock */
	ht = ts_hypertable_get_by_id(ht->fd.id);
	ownerid = ts_rel_get_owner(ht->main_table_relid);
	segmentby_cols = ts_compress_hypertable_parse_segment_by(with_clause_options, ht);
	orderby_cols = ts_compress_hypertable_parse_order_by(with_clause_options, ht);
	orderby_cols = add_time_to_order_by_if_not_included(orderby_cols, segmentby_cols, ht);
	compression_already_enabled = ht->fd.compressed_hypertable_id != INVALID_HYPERTABLE_ID;
	compressed_chunks_exist =
		compression_already_enabled && ts_chunk_exists_with_compression(ht->fd.id);

	if (!compress_enable)
	{
		if (compression_already_enabled)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("disabling compression is not yet supported")));
		/* compression is not enabled, so just return */
		return false;
	}

	if (compressed_chunks_exist)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot change compression options as compressed chunks already exist for "
						"this table")));

	/* Require both order by and segment by when altering because otherwise it's not clear what
	 * the default value means: does it mean leave as-is or is it an empty list. */
	if (compression_already_enabled && (with_clause_options[CompressOrderBy].is_default ||
										with_clause_options[CompressSegmentBy].is_default))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("need to specify both compress_orderby and compress_groupby if altering "
						"compression")));

	compresscolinfo_init(&compress_cols, ht->main_table_relid, segmentby_cols, orderby_cols);

	/* take explicit locks on catalog tables and keep them till end of txn */
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE), RowExclusiveLock);
	LockRelationOid(catalog_get_table_id(ts_catalog_get(), HYPERTABLE_COMPRESSION),
					RowExclusiveLock);

	if (compression_already_enabled)
	{
		Hypertable *compressed = ts_hypertable_get_by_id(ht->fd.compressed_hypertable_id);
		if (compressed == NULL)
			elog(ERROR, "compression enabled but no compressed hypertable found");
		/* need to drop the old compressed hypertable in case the segment by columns changed (and
		 * thus the column types of compressed hypertable need to change) */
		ts_hypertable_drop(compressed, DROP_RESTRICT);
		hypertable_compression_delete_by_hypertable_id(ht->fd.id);
	}

	compress_htid = create_compression_table(ownerid, &compress_cols);
	ts_hypertable_set_compressed_id(ht, compress_htid);
	compresscolinfo_add_catalog_entries(&compress_cols, ht->fd.id);
	/* do not release any locks, will get released by xact end */
	return true;
}
