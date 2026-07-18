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
#include "ts_catalog/array_utils.h"
#include "ts_catalog/catalog.h"
#include "ts_catalog/compression_settings.h"
#include "utils.h"

#define PRINT_COMPRESSION_TABLE_NAME(buf, prefix, hypertable_id)                                   \
	do                                                                                             \
	{                                                                                              \
		int ret = snprintf(buf, NAMEDATALEN, prefix, hypertable_id);                               \
		if (ret < 0 || ret >= NAMEDATALEN)                                                         \
		{                                                                                          \
			ereport(ERROR,                                                                         \
					(errcode(ERRCODE_INTERNAL_ERROR),                                              \
					 errmsg("bad compression hypertable internal name")));                         \
		}                                                                                          \
	} while (0);

static void create_compressed_chunk_indexes(Oid relid, CompressionSettings *settings);
static void set_toast_tuple_target_on_chunk(Oid compressed_table_id);
static void set_statistics_on_compressed_chunk(Oid compressed_table_id);

Oid
compression_table_create(Chunk *src_chunk, List *column_defs, Oid tablespace_oid,
						 CompressionSettings *settings)
{
	ObjectAddress tbladdress;
	CatalogSecurityContext sec_ctx;
	Datum toast_options;
#if PG18_LT
	char *validnsps[] = HEAP_RELOPT_NAMESPACES;
#else
	const char *const validnsps[] = HEAP_RELOPT_NAMESPACES;
#endif

	Oid owner = ts_rel_get_owner(src_chunk->hypertable_relid);

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
	NameData relname = build_compressed_relation_name(src_chunk);
	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	compress_rel = makeRangeVar(NameStr(src_chunk->fd.schema_name), NameStr(relname), -1);

	create->relation = compress_rel;
	/* Inherit the persistence (LOGGED or UNLOGGED) from the uncompressed chunk */
	create->relation->relpersistence = get_rel_persistence(src_chunk->table_id);

	tbladdress = DefineRelation(create, RELKIND_RELATION, owner, NULL, NULL);
	CommandCounterIncrement();
	Oid table_id = tbladdress.objectId;
	ts_copy_relation_acl(src_chunk->hypertable_relid, table_id, owner);
	toast_options =
		transformRelOptions((Datum) 0, create->options, "toast", validnsps, true, false);
	(void) heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);
	NewRelationCreateToastTable(table_id, toast_options);

	modify_compressed_toast_table_storage(settings, column_defs, table_id);
	set_statistics_on_compressed_chunk(table_id);
	set_toast_tuple_target_on_chunk(table_id);
	ts_catalog_restore_user(&sec_ctx);

	create_compressed_chunk_indexes(table_id, settings);

	return table_id;
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

	AlterTableInternal(compressed_table_id, list_make1(&cmd), true);
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
		{
			continue;
		}

		tuple = SearchSysCacheCopyAttName(RelationGetRelid(table_rel), NameStr(col_attr->attname));

		if (!HeapTupleIsValid(tuple))
		{
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of compressed table \"%s\" does not exist",
							NameStr(col_attr->attname),
							RelationGetRelationName(table_rel))));
		}

		attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);

		/* The planner should never look at compressed column statistics because
		 * it will not understand them. Statistics on the other columns,
		 * segmentbys and metadata, are very important, so we increase their
		 * target.
		 */
		if (col_attr->atttypid == compressed_data_type)
		{
			repl_val[AttrNumberGetAttrOffset(Anum_pg_attribute_attstattarget)] = Int16GetDatum(0);
		}
		else
		{
			repl_val[AttrNumberGetAttrOffset(Anum_pg_attribute_attstattarget)] =
				Int16GetDatum(1000);
		}
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
			CompressionStorage stor = compression_get_toast_storage(
				compression_get_column_algorithm(settings, cd->colname, typid));
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
		AlterTableInternal(compress_relid, cmds, false);
	}
}

void
create_compressed_chunk_indexes(Oid relid, CompressionSettings *settings)
{
	/*
	 * The index covers the segmentby columns followed by each orderby column's
	 * sparse metadata, in their configured order. Each orderby column adds its
	 * two metadata columns as `first` then `last`, inheriting that column's
	 * ASC/DESC and NULLS FIRST/LAST options.
	 */
	RangeVar *rv =
		makeRangeVar(get_namespace_name(get_rel_namespace(relid)), get_rel_name(relid), -1);
	IndexStmt stmt = {
		.type = T_IndexStmt,
		.accessMethod = DEFAULT_INDEX_TYPE,
		.idxname = NULL,
		.relation = rv,
		.tableSpace = get_tablespace_name(get_rel_tablespace(relid)),
	};

	NameData index_name;
	ObjectAddress index_addr;
	HeapTuple index_tuple;
	List *indexcols = NIL;

	StringInfoData buf;
	initStringInfo(&buf);

	if (settings->fd.segmentby)
	{
		Datum datum;
		bool isnull;
		ArrayIterator it = array_create_iterator(settings->fd.segmentby, 0, NULL);
		while (array_iterate(it, &datum, &isnull))
		{
			IndexElem *segment_elem = makeNode(IndexElem);
			segment_elem->name = TextDatumGetCString(datum);
			appendStringInfoString(&buf, segment_elem->name);
			appendStringInfoString(&buf, ", ");
			indexcols = lappend(indexcols, segment_elem);
		}
	}

	SortByDir ordering;
	SortByNulls nulls_ordering;

	StringInfoData orderby_buf;
	initStringInfo(&orderby_buf);
	for (int i = 1; i <= ts_array_length(settings->fd.orderby); i++)
	{
		char *first_name;
		char *last_name;
		/* Keeping the ability to create indexes based on minmax if
		 * explicitly specified through sparse index settings. Index will be
		 * created as (min, max) instead of (first, last).
		 */
		if (orderby_sparse_kind(settings, i) == ORDERBY_SPARSE_MINMAX)
		{
			orderby_sparse_metadata_names(settings, i, &first_name, &last_name);
		}
		else
		{
			orderby_firstlast_metadata_names(settings, i, &first_name, &last_name);
		}

		resetStringInfo(&orderby_buf);
		/* First metadata column. */
		IndexElem *orderby_first_elem = makeNode(IndexElem);
		orderby_first_elem->name = first_name;
		if (ts_array_get_element_bool(settings->fd.orderby_desc, i))
		{
			appendStringInfoString(&orderby_buf, " DESC");
			ordering = SORTBY_DESC;
		}
		else
		{
			appendStringInfoString(&orderby_buf, " ASC");
			ordering = SORTBY_ASC;
		}
		orderby_first_elem->ordering = ordering;

		if (ts_array_get_element_bool(settings->fd.orderby_nullsfirst, i))
		{
			if (orderby_first_elem->ordering != SORTBY_DESC)
			{
				appendStringInfoString(&orderby_buf, " NULLS FIRST");
				nulls_ordering = SORTBY_NULLS_FIRST;
			}
			else
			{
				nulls_ordering = SORTBY_NULLS_DEFAULT;
			}
		}
		else
		{
			if (orderby_first_elem->ordering != SORTBY_DESC)
			{
				nulls_ordering = SORTBY_NULLS_DEFAULT;
			}
			else
			{
				appendStringInfoString(&orderby_buf, " NULLS LAST");
				nulls_ordering = SORTBY_NULLS_LAST;
			}
		}
		orderby_first_elem->nulls_ordering = nulls_ordering;
		appendStringInfoString(&buf, orderby_first_elem->name);
		appendStringInfoString(&buf, orderby_buf.data);
		appendStringInfoString(&buf, ", ");
		indexcols = lappend(indexcols, orderby_first_elem);

		/* Second metadata column. */
		IndexElem *orderby_last_elem = makeNode(IndexElem);
		orderby_last_elem->name = last_name;
		orderby_last_elem->ordering = orderby_first_elem->ordering;
		orderby_last_elem->nulls_ordering = orderby_first_elem->nulls_ordering;
		appendStringInfoString(&buf, orderby_last_elem->name);
		appendStringInfoString(&buf, orderby_buf.data);
		appendStringInfoString(&buf, ", ");
		indexcols = lappend(indexcols, orderby_last_elem);
	}

	stmt.indexParams = indexcols;
	index_addr = DefineIndexCompat(relid,
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
	{
		elog(ERROR, "cache lookup failed for index relid %u", index_addr.objectId);
	}
	index_name = ((Form_pg_class) GETSTRUCT(index_tuple))->relname;

	elog(DEBUG1,
		 "adding index %s ON %s.%s USING BTREE(%s)",
		 NameStr(index_name),
		 rv->schemaname,
		 rv->relname,
		 buf.data);

	ReleaseSysCache(index_tuple);
}
