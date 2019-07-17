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
/* entrypoint
 * tsl_process_compress_table : is the entry point.
 */

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

static void test_compresschunk(Hypertable *ht, int32 compress_htid);

#define COMPRESSEDDATA_TYPE_NAME "_timescaledb_internal.compressed_data"

/* return ColumnDef list - dups columns of passed in relid
 *                         new columns have BYTEA type
 */
static List *
get_compress_columndef_from_table(Oid srctbl_relid)
{
	Relation rel;
	TupleDesc tupdesc;
	int attno;
	List *collist = NIL;
	const Oid compresseddata_oid =
		DatumGetObjectId(DirectFunctionCall1(regtypein, CStringGetDatum(COMPRESSEDDATA_TYPE_NAME)));

	if (!OidIsValid(compresseddata_oid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("type \"%s\" does not exist", COMPRESSEDDATA_TYPE_NAME)));
	/* Get the tupledesc and turn it over to expandTupleDesc */
	rel = relation_open(srctbl_relid, AccessShareLock);
	tupdesc = rel->rd_att;
	for (attno = 0; attno < tupdesc->natts; attno++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, attno);

		if (!attr->attisdropped)
		{
			ColumnDef *col = makeColumnDef(NameStr(attr->attname),
										   compresseddata_oid,
										   -1 /*typmod*/,
										   0 /*collation*/);
			collist = lappend(collist, col);
		}
	}
	relation_close(rel, AccessShareLock);
	return collist;
}

static int32
create_compression_table(Oid relid, Oid owner)
{
	ObjectAddress tbladdress;
	char relnamebuf[NAMEDATALEN];
	CatalogSecurityContext sec_ctx;
	Datum toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	Oid compress_relid;

	CreateStmt *create;
	RangeVar *compress_rel;
	List *collist;
	int32 compress_hypertable_id;

	collist = get_compress_columndef_from_table(relid);
	create = makeNode(CreateStmt);
	create->tableElts = collist;
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

/* this function will change in the follow up PR. Please do not review */
static Chunk *
create_compress_chunk(Hypertable *compress_ht, int32 compress_hypertable_id, Chunk *src_chunk)
{
	Hyperspace *hs = compress_ht->space;
	Catalog *catalog = ts_catalog_get();
	CatalogSecurityContext sec_ctx;
	// Hypercube *cube;
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
			 "compress_%s_%d_chunk",
			 NameStr(compress_ht->fd.associated_table_prefix),
			 compress_chunk->fd.id);
	compress_chunk->constraints = NULL;

	/* Insert chunk */
	ts_chunk_insert_lock(compress_chunk, RowExclusiveLock);
	/* Create the actual table relation for the chunk */
	compress_chunk->table_id = ts_chunk_create_table(compress_chunk, compress_ht);

	if (!OidIsValid(compress_chunk->table_id))
		elog(ERROR, "could not create chunk table");

	/* Create the chunk's constraints, triggers, and indexes */
	/*   ts_chunk_constraints_create(compress_chunk->constraints,
								   compress_chunk->table_id,
								   compress_chunk->fd.id,
								   compress_chunk->hypertable_relid,
								   compress_chunk->fd.hypertable_id);
   */
	ts_trigger_create_all_on_chunk(compress_ht, compress_chunk);

	ts_chunk_index_create_all(compress_chunk->fd.hypertable_id,
							  compress_chunk->hypertable_relid,
							  compress_chunk->fd.id,
							  compress_chunk->table_id);

	return compress_chunk;
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
	Oid ownerid = ts_rel_get_owner(ht->main_table_relid);

	compress_htid = create_compression_table(ht->main_table_relid, ownerid);
	ts_hypertable_set_compressed_id(ht, compress_htid);

	// TODO remove this after we have compress_chunks function
	test_compresschunk(ht, compress_htid);
	return true;
}

static List *
get_chunk_ids(int32 hypertable_id)
{
	List *chunk_ids = NIL;
	ScanIterator iterator = ts_scan_iterator_create(CHUNK, AccessShareLock, CurrentMemoryContext);
	ts_scanner_foreach(&iterator)
	{
		FormData_chunk *form = (FormData_chunk *) GETSTRUCT(ts_scan_iterator_tuple(&iterator));

		if (form->hypertable_id == hypertable_id)
			chunk_ids = lappend_int(chunk_ids, form->id);
	}
	return chunk_ids;
}
static void
test_compresschunk(Hypertable *ht, int32 compress_htid)
{
	Cache *hcache = ts_hypertable_cache_pin();
	Hypertable *compress_ht = ts_hypertable_cache_get_entry_by_id(hcache, compress_htid);

	// compress chunk from origin table */
	List *ht_chks = get_chunk_ids(ht->fd.id);
	ListCell *lc;
	foreach (lc, ht_chks)
	{
		int chkid = lfirst_int(lc);
		Chunk *src_chunk = ts_chunk_get_by_id(chkid, 0, true);
		Chunk *compress_chunk = create_compress_chunk(compress_ht, compress_ht->fd.id, src_chunk);
		ts_chunk_set_compressed_chunk(src_chunk, compress_chunk->fd.id, false);
	}
	ts_cache_release(hcache);
}
