/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains functions for manipulating compression related
 * internal storage objects like creating the underlying tables and
 * setting storage options.
 */

#include <postgres.h>
#include <access/reloptions.h>
#include <access/xact.h>
#include <catalog/indexing.h>
#include <catalog/objectaccess.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_class.h>
#include <catalog/pg_constraint.h>
#include <catalog/toasting.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <nodes/makefuncs.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/syscache.h>

#include "compression.h"
#include "compression_storage.h"
#include "create.h"
#include "custom_type_cache.h"
#include "extension_constants.h"
#include "guc.h"
#include "hypertable.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_settings.h"
#include "utils.h"

#define PRINT_COMPRESSION_TABLE_NAME(buf, prefix, hypertable_id)                                   \
	do                                                                                             \
	{                                                                                              \
		int ret = snprintf(buf, NAMEDATALEN, prefix, hypertable_id);                               \
		if (ret < 0 || ret > NAMEDATALEN)                                                          \
		{                                                                                          \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INTERNAL_ERROR),                                              \
					 errmsg("bad compression hypertable internal name")));                         \
		}                                                                                          \
	} while (0);

static void set_toast_tuple_target_on_chunk(Oid compressed_table_id);
static void set_statistics_on_compressed_chunk(Oid compressed_table_id);
static void create_compressed_chunk_indexes(Chunk *chunk, CompressionSettings *settings);

int32
compression_hypertable_create(Hypertable *ht, Oid owner, Oid tablespace_oid)
{
	ObjectAddress tbladdress;
	char relnamebuf[NAMEDATALEN];
	CatalogSecurityContext sec_ctx;
	Oid compress_relid;

	CreateStmt *create;
	RangeVar *compress_rel;
	int32 compress_hypertable_id;

	Assert(!TS_HYPERTABLE_HAS_COMPRESSION_TABLE(ht));

	create = makeNode(CreateStmt);
	create->tableElts = NIL;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = NULL;
	create->oncommit = ONCOMMIT_NOOP;
	create->tablespacename = get_tablespace_name(tablespace_oid);
	create->if_not_exists = false;

	/* Invalid tablespace_oid <=> NULL tablespace name */
	Assert(!OidIsValid(tablespace_oid) == (create->tablespacename == NULL));

	/* create the compression table */
	/* NewRelationCreateToastTable calls CommandCounterIncrement */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	compress_hypertable_id = ts_catalog_table_next_seq_id(ts_catalog_get(), HYPERTABLE);
	PRINT_COMPRESSION_TABLE_NAME(relnamebuf, "_compressed_hypertable_%d", compress_hypertable_id);
	compress_rel = makeRangeVar(pstrdup(INTERNAL_SCHEMA_NAME), pstrdup(relnamebuf), -1);

	create->relation = compress_rel;
	tbladdress = DefineRelation(create, RELKIND_RELATION, owner, NULL, NULL);
	CommandCounterIncrement();
	compress_relid = tbladdress.objectId;
	ts_copy_relation_acl(ht->main_table_relid, compress_relid, owner);
	ts_catalog_restore_user(&sec_ctx);
	ts_hypertable_create_compressed(compress_relid, compress_hypertable_id);

	return compress_hypertable_id;
}

Oid
compression_chunk_create(Chunk *src_chunk, Chunk *chunk, List *column_defs, Oid tablespace_oid)
{
	ObjectAddress tbladdress;
	CatalogSecurityContext sec_ctx;
	Datum toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	CompressionSettings *settings = ts_compression_settings_get(src_chunk->hypertable_relid);

	Oid owner = ts_rel_get_owner(chunk->hypertable_relid);

	CreateStmt *create;
	RangeVar *compress_rel;

	create = makeNode(CreateStmt);
	create->tableElts = column_defs;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = NULL;
	create->oncommit = ONCOMMIT_NOOP;
	create->tablespacename = get_tablespace_name(tablespace_oid);
	create->if_not_exists = false;

	/* Invalid tablespace_oid <=> NULL tablespace name */
	Assert(!OidIsValid(tablespace_oid) == (create->tablespacename == NULL));

	/* create the compression table */
	/* NewRelationCreateToastTable calls CommandCounterIncrement */
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	compress_rel = makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), -1);

	create->relation = compress_rel;
	tbladdress = DefineRelation(create, RELKIND_RELATION, owner, NULL, NULL);
	CommandCounterIncrement();
	chunk->table_id = tbladdress.objectId;
	ts_copy_relation_acl(chunk->hypertable_relid, chunk->table_id, owner);
	toast_options =
		transformRelOptions((Datum) 0, create->options, "toast", validnsps, true, false);
	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
	NewRelationCreateToastTable(chunk->table_id, toast_options);
	ts_catalog_restore_user(&sec_ctx);
	modify_compressed_toast_table_storage(settings, column_defs, chunk->table_id);

	set_statistics_on_compressed_chunk(chunk->table_id);
	set_toast_tuple_target_on_chunk(chunk->table_id);

	create_compressed_chunk_indexes(chunk, settings);

	return chunk->table_id;
}

static void
set_toast_tuple_target_on_chunk(Oid compressed_table_id)
{
	DefElem def_elem = {
		.type = T_DefElem,
		.defname = "toast_tuple_target",
		.arg = (Node *) makeInteger(ts_guc_debug_toast_tuple_target),
		.defaction = DEFELEM_SET,
		.location = -1,
	};
	AlterTableCmd cmd = {
		.type = T_AlterTableCmd,
		.subtype = AT_SetRelOptions,
		.def = (Node *) list_make1(&def_elem),
	};
	ts_alter_table_with_event_trigger(compressed_table_id, NULL, list_make1(&cmd), true);
}

static void
set_statistics_on_compressed_chunk(Oid compressed_table_id)
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
		Datum repl_val[Natts_pg_attribute] = { 0 };
		bool repl_null[Natts_pg_attribute] = { false };
		bool repl_repl[Natts_pg_attribute] = { false };

		/* skip system columns */
		if (col_attr->attnum <= 0)
			continue;

		tuple = SearchSysCacheCopyAttName(RelationGetRelid(table_rel), NameStr(col_attr->attname));

		if (!HeapTupleIsValid(tuple))
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of compressed table \"%s\" does not exist",
							NameStr(col_attr->attname),
							RelationGetRelationName(table_rel))));

		attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

		/* The planner should never look at compressed column statistics because
		 * it will not understand them. Statistics on the other columns,
		 * segmentbys and metadata, are very important, so we increase their
		 * target.
		 */
		if (col_attr->atttypid == compressed_data_type)
			repl_val[AttrNumberGetAttrOffset(Anum_pg_attribute_attstattarget)] = Int16GetDatum(0);
		else
			repl_val[AttrNumberGetAttrOffset(Anum_pg_attribute_attstattarget)] =
				Int16GetDatum(1000);
		repl_repl[AttrNumberGetAttrOffset(Anum_pg_attribute_attstattarget)] = true;

		tuple =
			heap_modify_tuple(tuple, RelationGetDescr(attrelation), repl_val, repl_null, repl_repl);
		CatalogTupleUpdate(attrelation, &tuple->t_self, tuple);

		InvokeObjectPostAlterHook(RelationRelationId,
								  RelationGetRelid(table_rel),
								  attrtuple->attnum);
		heap_freetuple(tuple);
	}

	table_close(attrelation, NoLock);
	table_close(table_rel, NoLock);
}

/* modify storage attributes for toast table columns attached to the
 * compression table
 */
void
modify_compressed_toast_table_storage(CompressionSettings *settings, List *coldefs,
									  Oid compress_relid)
{
	ListCell *lc;
	List *cmds = NIL;
	Oid compresseddata_oid = ts_custom_type_cache_get(CUSTOM_TYPE_COMPRESSED_DATA)->type_oid;

	foreach (lc, coldefs)
	{
		ColumnDef *cd = lfirst_node(ColumnDef, lc);
		AttrNumber attno = get_attnum(compress_relid, cd->colname);
		if (attno != InvalidAttrNumber && get_atttype(compress_relid, attno) == compresseddata_oid)
		{
			/*
			 * All columns that pass the datatype check are columns
			 * that are also present in the uncompressed hypertable.
			 * Metadata columns are missing from the uncompressed
			 * hypertable but they do not have compresseddata datatype
			 * and therefore would be skipped.
			 */
			attno = get_attnum(settings->fd.relid, cd->colname);
			Assert(attno != InvalidAttrNumber);
			Oid typid = get_atttype(settings->fd.relid, attno);
			CompressionStorage stor =
				compression_get_toast_storage(compression_get_default_algorithm(typid));
			if (stor != TOAST_STORAGE_EXTERNAL)
			/* external is default storage for toast columns */
			{
				AlterTableCmd *cmd = makeNode(AlterTableCmd);
				cmd->subtype = AT_SetStorage;
				cmd->name = pstrdup(cd->colname);
				Assert(stor == TOAST_STORAGE_EXTENDED);
				cmd->def = (Node *) makeString("extended");
				cmds = lappend(cmds, cmd);
			}
		}
	}

	if (cmds != NIL)
	{
		ts_alter_table_with_event_trigger(compress_relid, NULL, cmds, false);
	}
}

static void
create_compressed_chunk_indexes(Chunk *chunk, CompressionSettings *settings)
{
	IndexStmt stmt = {
		.type = T_IndexStmt,
		.accessMethod = DEFAULT_INDEX_TYPE,
		.idxname = NULL,
		.relation = makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), 0),
		.tableSpace = get_tablespace_name(get_rel_tablespace(chunk->table_id)),
	};
	IndexElem sequence_num_elem = {
		.type = T_IndexElem,
		.name = COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME,
	};
	NameData index_name;
	ObjectAddress index_addr;
	HeapTuple index_tuple;
	List *indexcols = NIL;

	StringInfo buf = makeStringInfo();

	if (settings->fd.segmentby)
	{
		Datum datum;
		bool isnull;
		ArrayIterator it = array_create_iterator(settings->fd.segmentby, 0, NULL);
		while (array_iterate(it, &datum, &isnull))
		{
			IndexElem *segment_elem = makeNode(IndexElem);
			segment_elem->name = TextDatumGetCString(datum);
			appendStringInfoString(buf, segment_elem->name);
			appendStringInfoString(buf, ", ");
			indexcols = lappend(indexcols, segment_elem);
		}
	}

	if (list_length(indexcols) == 0)
	{
		return;
	}

	appendStringInfoString(buf, COMPRESSION_COLUMN_METADATA_SEQUENCE_NUM_NAME);
	indexcols = lappend(indexcols, &sequence_num_elem);

	stmt.indexParams = indexcols;
	index_addr = DefineIndexCompat(chunk->table_id,
								   &stmt,
								   InvalidOid, /* IndexRelationId */
								   InvalidOid, /* parentIndexId */
								   InvalidOid, /* parentConstraintId */
								   -1,		   /* total_parts */
								   false,	   /* is_alter_table */
								   false,	   /* check_rights */
								   false,	   /* check_not_in_use */
								   false,	   /* skip_build */
								   false);	   /* quiet */
	index_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(index_addr.objectId));

	if (!HeapTupleIsValid(index_tuple))
		elog(ERROR, "cache lookup failed for index relid %u", index_addr.objectId);
	index_name = ((Form_pg_class) GETSTRUCT(index_tuple))->relname;

	elog(DEBUG1,
		 "adding index %s ON %s.%s USING BTREE(%s)",
		 NameStr(index_name),
		 NameStr(chunk->fd.schema_name),
		 NameStr(chunk->fd.table_name),
		 buf->data);

	ReleaseSysCache(index_tuple);
}
