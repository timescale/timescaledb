/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/pg_index.h>
#include <catalog/pg_constraint.h>
#include <catalog/objectaddress.h>
#include <catalog/namespace.h>
#include <access/htup_details.h>
#include <utils/syscache.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/fmgroids.h>
#include <utils/builtins.h>
#include <nodes/parsenodes.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/cluster.h>
#include <access/xact.h>
#include <miscadmin.h>

#include "compat.h"
#if PG12_LT
#include <optimizer/var.h> /* f09346a */
#elif PG12_GE
#include <optimizer/optimizer.h>
#endif

#include "export.h"
#include "chunk_index.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "catalog.h"
#include "scanner.h"
#include "chunk.h"
#include "compat.h"

static bool chunk_index_insert(int32 chunk_id, const char *chunk_index, int32 hypertable_id,
							   const char *hypertable_index);
static Oid ts_chunk_index_create_post_adjustment(int32 hypertable_id, Relation template_indexrel,
												 Relation chunkrel, IndexInfo *indexinfo,
												 bool isconstraint, Oid index_tablespace);

static List *
create_index_colnames(Relation indexrel)
{
	List *colnames = NIL;
	int i;

	for (i = 0; i < indexrel->rd_att->natts; i++)
	{
		Form_pg_attribute idxattr = TupleDescAttr(indexrel->rd_att, i);

		colnames = lappend(colnames, pstrdup(NameStr(idxattr->attname)));
	}

	return colnames;
}

/*
 * Pick a name for a chunk index.
 *
 * The chunk's index name will the original index name prefixed with the chunk's
 * table name, modulo any conflict resolution we need to do.
 */
static char *
chunk_index_choose_name(const char *tabname, const char *main_index_name, Oid namespaceid)
{
	char buf[10];
	char *label = NULL;
	char *idxname;
	int n = 0;

	for (;;)
	{
		/* makeObjectName will ensure the index name fits within a NAME type */
		idxname = makeObjectName(tabname, main_index_name, label);

		if (!OidIsValid(get_relname_relid(idxname, namespaceid)))
			break;

		/* found a conflict, so try a new name component */
		pfree(idxname);
		snprintf(buf, sizeof(buf), "%d", ++n);
		label = buf;
	}

	return idxname;
}

static void
adjust_expr_attnos(Oid ht_relid, IndexInfo *ii, Relation chunkrel)
{
	List *vars = NIL;
	ListCell *lc;

	/* Get a list of references to all Vars in the expression */
	if (ii->ii_Expressions != NIL)
		vars = list_concat(vars, pull_var_clause((Node *) ii->ii_Expressions, 0));

	/* Get a list of references to all Vars in the predicate */
	if (ii->ii_Predicate != NIL)
		vars = list_concat(vars, pull_var_clause((Node *) ii->ii_Predicate, 0));

	foreach (lc, vars)
	{
		Var *var = lfirst_node(Var, lc);

		char *attname = get_attname_compat(ht_relid, var->varattno, false);
		var->varattno = get_attnum(chunkrel->rd_id, attname);

		if (var->varattno == InvalidAttrNumber)
			elog(ERROR, "index attribute %s not found in chunk", attname);
	}
}

/*
 * Adjust column reference attribute numbers for regular indexes.
 */
static void
chunk_adjust_colref_attnos(IndexInfo *ii, Relation idxrel, Relation chunkrel)
{
	int i;

	for (i = 0; i < idxrel->rd_att->natts; i++)
	{
		FormData_pg_attribute *idxattr = TupleDescAttr(idxrel->rd_att, i);
		AttrNumber attno = get_attnum(chunkrel->rd_id, NameStr(idxattr->attname));

		if (attno == InvalidAttrNumber)
			elog(ERROR, "index attribute %s not found in chunk", NameStr(idxattr->attname));
#if PG96 || PG10
		ii->ii_KeyAttrNumbers[i] = attno;
#else
		ii->ii_IndexAttrNumbers[i] = attno;
#endif
	}
}

void
ts_adjust_indexinfo_attnos(IndexInfo *indexinfo, Oid ht_relid, Relation template_indexrel,
						   Relation chunkrel)
{
	/*
	 * Adjust a hypertable's index attribute numbers to match a chunk.
	 *
	 * A hypertable's IndexInfo for one of its indexes references the attributes
	 * (columns) in the hypertable by number. These numbers might not be the same
	 * for the corresponding attribute in one of its chunks. To be able to use an
	 * IndexInfo from a hypertable's index to create a corresponding index on a
	 * chunk, we need to adjust the attribute numbers to match the chunk.
	 *
	 * We need to handle two cases: (1) regular indexes that reference columns
	 * directly, and (2) expression indexes that reference columns in expressions.
	 *
	 * Additionally we need to adjust column references in predicates.
	 */
	if (list_length(indexinfo->ii_Expressions) == 0)
		chunk_adjust_colref_attnos(indexinfo, template_indexrel, chunkrel);

	adjust_expr_attnos(ht_relid, indexinfo, chunkrel);
}

#define CHUNK_INDEX_TABLESPACE_OFFSET 1

/*
 * Pick a chunk index's tablespace at an offset from the chunk's tablespace in
 * order to avoid colocating chunks and their indexes in the same tablespace.
 * This hopefully leads to more I/O parallelism.
 */
static Oid
chunk_index_select_tablespace(int32 hypertable_id, Relation chunkrel)
{
	Tablespace *tspc;
	Oid tablespace_oid = InvalidOid;

	tspc = ts_hypertable_get_tablespace_at_offset_from(hypertable_id,
													   chunkrel->rd_rel->reltablespace,
													   CHUNK_INDEX_TABLESPACE_OFFSET);

	if (NULL != tspc)
		tablespace_oid = tspc->tablespace_oid;

	return tablespace_oid;
}

Oid
ts_chunk_index_get_tablespace(int32 hypertable_id, Relation template_indexrel, Relation chunkrel)
{
	/*
	 * Determine the index's tablespace. Use the main index's tablespace, or,
	 * if not set, select one at an offset from the chunk's tablespace.
	 */
	if (OidIsValid(template_indexrel->rd_rel->reltablespace))
		return template_indexrel->rd_rel->reltablespace;
	else
		return chunk_index_select_tablespace(hypertable_id, chunkrel);
}

/*
 * Create a chunk index based on the configuration of the "parent" index.
 */
static Oid
chunk_relation_index_create(Relation htrel, Relation template_indexrel, Relation chunkrel,
							bool isconstraint, Oid index_tablespace)
{
	IndexInfo *indexinfo = BuildIndexInfo(template_indexrel);
	int32 hypertable_id;

	/*
	 * Convert the IndexInfo's attnos to match the chunk instead of the
	 * hypertable
	 */
	if (chunk_index_need_attnos_adjustment(RelationGetDescr(htrel), RelationGetDescr(chunkrel)))
		ts_adjust_indexinfo_attnos(indexinfo, htrel->rd_id, template_indexrel, chunkrel);

	hypertable_id = ts_hypertable_relid_to_id(htrel->rd_id);

	return ts_chunk_index_create_post_adjustment(hypertable_id,
												 template_indexrel,
												 chunkrel,
												 indexinfo,
												 isconstraint,
												 index_tablespace);
}

static Oid
ts_chunk_index_create_post_adjustment(int32 hypertable_id, Relation template_indexrel,
									  Relation chunkrel, IndexInfo *indexinfo, bool isconstraint,
									  Oid index_tablespace)
{
	Oid chunk_indexrelid = InvalidOid;
	const char *indexname;
	HeapTuple tuple;
	bool isnull;
	Datum reloptions;
	Datum indclass;
	oidvector *indclassoid;
	List *colnames = create_index_colnames(template_indexrel);
	Oid tablespace;
	bits16 flags = 0;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(RelationGetRelid(template_indexrel)));

	if (!HeapTupleIsValid(tuple))
		elog(ERROR,
			 "cache lookup failed for index relation %u",
			 RelationGetRelid(template_indexrel));

	reloptions = SysCacheGetAttr(RELOID, tuple, Anum_pg_class_reloptions, &isnull);

	indclass = SysCacheGetAttr(INDEXRELID,
							   template_indexrel->rd_indextuple,
							   Anum_pg_index_indclass,
							   &isnull);
	Assert(!isnull);
	indclassoid = (oidvector *) DatumGetPointer(indclass);

	indexname = chunk_index_choose_name(get_rel_name(RelationGetRelid(chunkrel)),
										get_rel_name(RelationGetRelid(template_indexrel)),
										get_rel_namespace(RelationGetRelid(chunkrel)));
	if (OidIsValid(index_tablespace))
		tablespace = index_tablespace;
	else
		tablespace = ts_chunk_index_get_tablespace(hypertable_id, template_indexrel, chunkrel);

	/* assign flags for index creation and constraint creation */
	if (isconstraint)
		flags |= INDEX_CREATE_ADD_CONSTRAINT;
	if (template_indexrel->rd_index->indisprimary)
		flags |= INDEX_CREATE_IS_PRIMARY;

	chunk_indexrelid = index_create_compat(chunkrel,
										   indexname,
										   InvalidOid,
										   InvalidOid,
										   indexinfo,
										   colnames,
										   template_indexrel->rd_rel->relam,
										   tablespace,
										   template_indexrel->rd_indcollation,
										   indclassoid->values,
										   template_indexrel->rd_indoption,
										   reloptions,
										   flags,
										   0,	  /* constr_flags constant and 0
													* for now */
										   false,  /* allow system table mods */
										   false); /* is internal */

	ReleaseSysCache(tuple);

	return chunk_indexrelid;
}

static bool
chunk_index_insert_relation(Relation rel, int32 chunk_id, const char *chunk_index,
							int32 hypertable_id, const char *parent_index)
{
	TupleDesc desc = RelationGetDescr(rel);
	Datum values[Natts_chunk_index];
	bool nulls[Natts_chunk_index] = { false };
	CatalogSecurityContext sec_ctx;

	values[AttrNumberGetAttrOffset(Anum_chunk_index_chunk_id)] = Int32GetDatum(chunk_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_index_index_name)] =
		DirectFunctionCall1(namein, CStringGetDatum(chunk_index));
	values[AttrNumberGetAttrOffset(Anum_chunk_index_hypertable_id)] = Int32GetDatum(hypertable_id);
	values[AttrNumberGetAttrOffset(Anum_chunk_index_hypertable_index_name)] =
		DirectFunctionCall1(namein, CStringGetDatum(parent_index));

	ts_catalog_database_info_become_owner(ts_catalog_database_info_get(), &sec_ctx);
	ts_catalog_insert_values(rel, desc, values, nulls);
	ts_catalog_restore_user(&sec_ctx);

	return true;
}

/*
 * Add an parent-child index mapping to the catalog.
 */
static bool
chunk_index_insert(int32 chunk_id, const char *chunk_index, int32 hypertable_id,
				   const char *hypertable_index)
{
	Catalog *catalog = ts_catalog_get();
	Relation rel;
	bool result;

	rel = table_open(catalog_get_table_id(catalog, CHUNK_INDEX), RowExclusiveLock);
	result =
		chunk_index_insert_relation(rel, chunk_id, chunk_index, hypertable_id, hypertable_index);
	table_close(rel, RowExclusiveLock);

	return result;
}

void
ts_chunk_index_create_from_constraint(int32 hypertable_id, Oid hypertable_constraint,
									  int32 chunk_id, Oid chunk_constraint)
{
	Oid chunk_indexrelid = get_constraint_index(chunk_constraint);
	Oid hypertable_indexrelid = get_constraint_index(hypertable_constraint);

	Assert(OidIsValid(chunk_indexrelid));
	Assert(OidIsValid(hypertable_indexrelid));

	chunk_index_insert(chunk_id,
					   get_rel_name(chunk_indexrelid),
					   hypertable_id,
					   get_rel_name(hypertable_indexrelid));
}

/*
 * Create a new chunk index as a child of a parent hypertable index.
 *
 * The chunk index is created based on the information from the parent index
 * relation. This function is typically called when a new chunk is created and
 * it should, for each hypertable index, have a corresponding index of its own.
 */
static void
chunk_index_create(Relation hypertable_rel, int32 hypertable_id, Relation hypertable_idxrel,
				   int32 chunk_id, Relation chunkrel, Oid constraint_oid)
{
	Oid chunk_indexrelid;

	if (OidIsValid(constraint_oid))
	{
		/*
		 * If there is an associated constraint then that constraint created
		 * both the index and the catalog entry for the index
		 */
		return;
	}

	chunk_indexrelid =
		chunk_relation_index_create(hypertable_rel, hypertable_idxrel, chunkrel, false, InvalidOid);

	chunk_index_insert(chunk_id,
					   get_rel_name(chunk_indexrelid),
					   hypertable_id,
					   get_rel_name(RelationGetRelid(hypertable_idxrel)));
}

void
ts_chunk_index_create_from_adjusted_index_info(int32 hypertable_id, Relation hypertable_idxrel,
											   int32 chunk_id, Relation chunkrel,
											   IndexInfo *indexinfo)
{
	Oid chunk_indexrelid = ts_chunk_index_create_post_adjustment(hypertable_id,
																 hypertable_idxrel,
																 chunkrel,
																 indexinfo,
																 false,
																 false);

	chunk_index_insert(chunk_id,
					   get_rel_name(chunk_indexrelid),
					   hypertable_id,
					   get_rel_name(RelationGetRelid(hypertable_idxrel)));
}

/*
 * Create a new chunk index as a child of a parent hypertable index.
 *
 * The chunk index is created based on the statement that also creates the
 * parent index. This function is typically called when a new index is created
 * on the hypertable and we must add a corresponding index to each chunk.
 */
Oid
ts_chunk_index_create_from_stmt(IndexStmt *stmt, int32 chunk_id, Oid chunkrelid,
								int32 hypertable_id, Oid hypertable_indexrelid)
{
	ObjectAddress idxobj;
	char *hypertable_indexname = get_rel_name(hypertable_indexrelid);

	if (hypertable_indexname == NULL)
		return InvalidOid; /* oops, the root index is gone, lets stop */

	if (NULL != stmt->idxname)
		stmt->idxname = chunk_index_choose_name(get_rel_name(chunkrelid),
												hypertable_indexname,
												get_rel_namespace(chunkrelid));

	idxobj = DefineIndexCompat(chunkrelid,
							   stmt,
							   InvalidOid,
							   false, /* is alter table */
							   true,  /* check rights */
							   false, /* skip build */
							   true); /* quiet */

	chunk_index_insert(chunk_id,
					   get_rel_name(idxobj.objectId),
					   hypertable_id,
					   hypertable_indexname);

	return idxobj.objectId;
}

static inline Oid
chunk_index_get_schemaid(Form_chunk_index chunk_index, bool missing_ok)
{
	Chunk *chunk = ts_chunk_get_by_id(chunk_index->chunk_id, 0, true);

	return get_namespace_oid(NameStr(chunk->fd.schema_name), missing_ok);
}

#define chunk_index_tuple_get_schema(tuple)                                                        \
	chunk_index_get_schema((FormData_chunk_index *) GETSTRUCT(tuple));

/*
 * Create all indexes on a chunk, given the indexes that exists on the chunk's
 * hypertable.
 */
void
ts_chunk_index_create_all(int32 hypertable_id, Oid hypertable_relid, int32 chunk_id, Oid chunkrelid)
{
	Relation htrel;
	Relation chunkrel;
	List *indexlist;
	ListCell *lc;

	htrel = table_open(hypertable_relid, AccessShareLock);

	/* Need ShareLock on the heap relation we are creating indexes on */
	chunkrel = table_open(chunkrelid, ShareLock);

	/*
	 * We should only add those indexes that aren't created from constraints,
	 * since those are added separately.
	 *
	 * Ideally, we should just be able to check the index relation's rd_index
	 * struct for the flags indisunique, indisprimary, indisexclusion to
	 * figure out if this is a constraint-supporting index. However,
	 * indisunique is true both for plain unique indexes and those created
	 * from constraints. Instead, we prune the main table's index list,
	 * removing those indexes that are supporting a constraint.
	 */
	indexlist = RelationGetIndexList(htrel);

	foreach (lc, indexlist)
	{
		Oid hypertable_idxoid = lfirst_oid(lc);
		Relation hypertable_idxrel = index_open(hypertable_idxoid, AccessShareLock);

		chunk_index_create(htrel,
						   hypertable_id,
						   hypertable_idxrel,
						   chunk_id,
						   chunkrel,
						   get_index_constraint(hypertable_idxoid));

		relation_close(hypertable_idxrel, AccessShareLock);
	}

	relation_close(chunkrel, NoLock);
	relation_close(htrel, AccessShareLock);
}

static int
chunk_index_scan(int indexid, ScanKeyData scankey[], int nkeys, tuple_found_func tuple_found,
				 tuple_filter_func tuple_filter, void *data, LOCKMODE lockmode)
{
	Catalog *catalog = ts_catalog_get();
	ScannerCtx scanCtx = {
		.table = catalog_get_table_id(catalog, CHUNK_INDEX),
		.index = catalog_get_index(catalog, CHUNK_INDEX, indexid),
		.nkeys = nkeys,
		.scankey = scankey,
		.tuple_found = tuple_found,
		.filter = tuple_filter,
		.data = data,
		.lockmode = lockmode,
		.scandirection = ForwardScanDirection,
	};

	return ts_scanner_scan(&scanCtx);
}

#define chunk_index_scan_update(idxid, scankey, nkeys, tuple_found, tuple_filter, data)            \
	chunk_index_scan(idxid, scankey, nkeys, tuple_found, tuple_filter, data, RowExclusiveLock)

static ChunkIndexMapping *
chunk_index_mapping_from_tuple(TupleInfo *ti, ChunkIndexMapping *cim)
{
	FormData_chunk_index *chunk_index = (FormData_chunk_index *) GETSTRUCT(ti->tuple);
	Chunk *chunk = ts_chunk_get_by_id(chunk_index->chunk_id, 0, true);
	Oid nspoid_chunk = get_rel_namespace(chunk->table_id);
	Oid nspoid_hyper = get_rel_namespace(chunk->hypertable_relid);

	if (cim == NULL)
	{
		cim = palloc(sizeof(ChunkIndexMapping));
	}
	cim->chunkoid = chunk->table_id;
	cim->indexoid = get_relname_relid(NameStr(chunk_index->index_name), nspoid_chunk);
	cim->parent_indexoid =
		get_relname_relid(NameStr(chunk_index->hypertable_index_name), nspoid_hyper);
	cim->hypertableoid = chunk->hypertable_relid;

	return cim;
}

static ScanTupleResult
chunk_index_collect(TupleInfo *ti, void *data)
{
	List **mappings = data;
	ChunkIndexMapping *cim = chunk_index_mapping_from_tuple(ti, NULL);

	*mappings = lappend(*mappings, cim);

	return SCAN_CONTINUE;
}

List *
ts_chunk_index_get_mappings(Hypertable *ht, Oid hypertable_indexrelid)
{
	ScanKeyData scankey[2];
	const char *indexname = get_rel_name(hypertable_indexrelid);
	List *mappings = NIL;

	ScanKeyInit(&scankey[0],
				Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(ht->fd.id));
	ScanKeyInit(&scankey[1],
				Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_index_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum((indexname))));

	chunk_index_scan(CHUNK_INDEX_HYPERTABLE_ID_HYPERTABLE_INDEX_NAME_IDX,
					 scankey,
					 2,
					 chunk_index_collect,
					 NULL,
					 &mappings,
					 AccessShareLock);

	return mappings;
}

typedef struct ChunkIndexDeleteData
{
	const char *index_name;
	const char *schema;
	bool drop_index;
} ChunkIndexDeleteData;

static ScanTupleResult
chunk_index_tuple_delete(TupleInfo *ti, void *data)
{
	FormData_chunk_index *chunk_index = (FormData_chunk_index *) GETSTRUCT(ti->tuple);
	Oid schemaid = chunk_index_get_schemaid(chunk_index, true);
	ChunkIndexDeleteData *cid = data;

	ts_catalog_delete(ti->scanrel, ti->tuple);

	if (cid->drop_index)
	{
		ObjectAddress idxobj = {
			.classId = RelationRelationId,
			.objectId = get_relname_relid(NameStr(chunk_index->index_name), schemaid),
		};

		if (OidIsValid(idxobj.objectId))
			performDeletion(&idxobj, DROP_RESTRICT, 0);
	}

	return SCAN_CONTINUE;
}

static ScanFilterResult
chunk_index_name_and_schema_filter(TupleInfo *ti, void *data)
{
	FormData_chunk_index *chunk_index = (FormData_chunk_index *) GETSTRUCT(ti->tuple);
	ChunkIndexDeleteData *cid = data;

	if (namestrcmp(&chunk_index->index_name, cid->index_name) == 0)
	{
		Chunk *chunk = ts_chunk_get_by_id(chunk_index->chunk_id, 0, false);

		if (NULL != chunk && namestrcmp(&chunk->fd.schema_name, cid->schema) == 0)
			return SCAN_INCLUDE;
	}

	if (namestrcmp(&chunk_index->hypertable_index_name, cid->index_name) == 0)
	{
		Hypertable *ht;

		ht = ts_hypertable_get_by_id(chunk_index->hypertable_id);

		if (NULL != ht && namestrcmp(&ht->fd.schema_name, cid->schema) == 0)
			return SCAN_INCLUDE;
	}

	return SCAN_EXCLUDE;
}

int
ts_chunk_index_delete(Chunk *chunk, Oid chunk_indexrelid, bool drop_index)
{
	ScanKeyData scankey[2];
	const char *indexname = get_rel_name(chunk_indexrelid);
	ChunkIndexDeleteData data = {
		.drop_index = drop_index,
	};

	ScanKeyInit(&scankey[0],
				Anum_chunk_index_chunk_id_index_name_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk->fd.id));
	ScanKeyInit(&scankey[1],
				Anum_chunk_index_chunk_id_index_name_idx_index_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(indexname)));

	return chunk_index_scan_update(CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX,
								   scankey,
								   2,
								   chunk_index_tuple_delete,
								   NULL,
								   &data);
}

void
ts_chunk_index_delete_by_name(const char *schema, const char *index_name, bool drop_index)
{
	ChunkIndexDeleteData data = {
		.index_name = index_name,
		.drop_index = drop_index,
		.schema = schema,
	};

	chunk_index_scan_update(INVALID_INDEXID,
							NULL,
							0,
							chunk_index_tuple_delete,
							chunk_index_name_and_schema_filter,
							&data);
}

int
ts_chunk_index_delete_by_chunk_id(int32 chunk_id, bool drop_index)
{
	ScanKeyData scankey[1];
	ChunkIndexDeleteData data = {
		.drop_index = drop_index,
	};

	ScanKeyInit(&scankey[0],
				Anum_chunk_index_chunk_id_index_name_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk_id));

	return chunk_index_scan_update(CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX,
								   scankey,
								   1,
								   chunk_index_tuple_delete,
								   NULL,
								   &data);
}

static ScanTupleResult
chunk_index_tuple_found(TupleInfo *ti, void *const data)
{
	ChunkIndexMapping *const cim = data;

	chunk_index_mapping_from_tuple(ti, cim);
	return SCAN_DONE;
}

bool
ts_chunk_index_get_by_indexrelid(Chunk *chunk, Oid chunk_indexrelid, ChunkIndexMapping *cim_out)
{
	int tuples_found;
	ScanKeyData scankey[2];
	const char *indexname = get_rel_name(chunk_indexrelid);

	Assert(cim_out != NULL);

	ScanKeyInit(&scankey[0],
				Anum_chunk_index_chunk_id_index_name_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk->fd.id));
	ScanKeyInit(&scankey[1],
				Anum_chunk_index_chunk_id_index_name_idx_index_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				DirectFunctionCall1(namein, CStringGetDatum(indexname)));

	tuples_found = chunk_index_scan(CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX,
									scankey,
									2,
									chunk_index_tuple_found,
									NULL,
									cim_out,
									AccessShareLock);

	return tuples_found > 0;
}

static ScanFilterResult
chunk_hypertable_index_name_filter(TupleInfo *ti, void *data)
{
	FormData_chunk_index *chunk_index = (FormData_chunk_index *) GETSTRUCT(ti->tuple);
	ChunkIndexMapping *cim = data;
	const char *hypertable_indexname = get_rel_name(cim->parent_indexoid);

	if (namestrcmp(&chunk_index->hypertable_index_name, hypertable_indexname) == 0)
		return SCAN_INCLUDE;

	return SCAN_EXCLUDE;
}

TSDLLEXPORT bool
ts_chunk_index_get_by_hypertable_indexrelid(Chunk *chunk, Oid hypertable_indexrelid,
											ChunkIndexMapping *cim_out)
{
	int tuples_found;
	ScanKeyData scankey[1];

	Assert(cim_out != NULL);

	cim_out->parent_indexoid = hypertable_indexrelid;

	ScanKeyInit(&scankey[0],
				Anum_chunk_index_chunk_id_index_name_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk->fd.id));

	tuples_found = chunk_index_scan(CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX,
									scankey,
									1,
									chunk_index_tuple_found,
									chunk_hypertable_index_name_filter,
									cim_out,
									AccessShareLock);

	return tuples_found > 0;
}

typedef struct ChunkIndexRenameInfo
{
	const char *oldname, *newname;
	bool isparent;
} ChunkIndexRenameInfo;

static ScanTupleResult
chunk_index_tuple_rename(TupleInfo *ti, void *data)
{
	ChunkIndexRenameInfo *info = data;
	HeapTuple tuple = heap_copytuple(ti->tuple);
	FormData_chunk_index *chunk_index = (FormData_chunk_index *) GETSTRUCT(tuple);

	if (info->isparent)
	{
		/*
		 * If the renaming is for a hypertable index, we also rename all
		 * corresponding chunk indexes
		 */
		Chunk *chunk = ts_chunk_get_by_id(chunk_index->chunk_id, 0, true);
		Oid chunk_schemaoid = get_namespace_oid(NameStr(chunk->fd.schema_name), false);
		const char *chunk_index_name =
			chunk_index_choose_name(NameStr(chunk->fd.table_name), info->newname, chunk_schemaoid);
		Oid chunk_indexrelid = get_relname_relid(NameStr(chunk_index->index_name), chunk_schemaoid);

		/* Update the metadata */
		namestrcpy(&chunk_index->index_name, chunk_index_name);
		namestrcpy(&chunk_index->hypertable_index_name, info->newname);

		/* Rename the chunk index */
		RenameRelationInternalCompat(chunk_indexrelid, chunk_index_name, false, true);
	}
	else
		namestrcpy(&chunk_index->index_name, info->newname);

	ts_catalog_update(ti->scanrel, tuple);
	heap_freetuple(tuple);

	if (info->isparent)
		return SCAN_CONTINUE;

	return SCAN_DONE;
}

int
ts_chunk_index_rename(Chunk *chunk, Oid chunk_indexrelid, const char *newname)
{
	ScanKeyData scankey[2];
	const char *indexname = get_rel_name(chunk_indexrelid);
	ChunkIndexRenameInfo renameinfo = {
		.oldname = indexname,
		.newname = newname,
	};

	ScanKeyInit(&scankey[0],
				Anum_chunk_index_chunk_id_index_name_idx_chunk_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(chunk->fd.id));
	ScanKeyInit(&scankey[1],
				Anum_chunk_index_chunk_id_index_name_idx_index_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(indexname));

	return chunk_index_scan_update(CHUNK_INDEX_CHUNK_ID_INDEX_NAME_IDX,
								   scankey,
								   2,
								   chunk_index_tuple_rename,
								   NULL,
								   &renameinfo);
}

int
ts_chunk_index_rename_parent(Hypertable *ht, Oid hypertable_indexrelid, const char *newname)
{
	ScanKeyData scankey[2];
	const char *indexname = get_rel_name(hypertable_indexrelid);
	ChunkIndexRenameInfo renameinfo = {
		.oldname = indexname,
		.newname = newname,
		.isparent = true,
	};

	ScanKeyInit(&scankey[0],
				Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(ht->fd.id));
	ScanKeyInit(&scankey[1],
				Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_index_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(indexname));

	return chunk_index_scan_update(CHUNK_INDEX_HYPERTABLE_ID_HYPERTABLE_INDEX_NAME_IDX,
								   scankey,
								   2,
								   chunk_index_tuple_rename,
								   NULL,
								   &renameinfo);
}

static ScanTupleResult
chunk_index_tuple_set_tablespace(TupleInfo *ti, void *data)
{
	char *tablespace = data;
	FormData_chunk_index *chunk_index = (FormData_chunk_index *) GETSTRUCT(ti->tuple);
	Oid schemaoid = chunk_index_get_schemaid(chunk_index, false);
	Oid indexrelid = get_relname_relid(NameStr(chunk_index->index_name), schemaoid);
	AlterTableCmd *cmd = makeNode(AlterTableCmd);
	List *cmds = NIL;

	cmd->subtype = AT_SetTableSpace;
	cmd->name = tablespace;
	cmds = lappend(cmds, cmd);

	AlterTableInternal(indexrelid, cmds, false);

	return SCAN_CONTINUE;
}

int
ts_chunk_index_set_tablespace(Hypertable *ht, Oid hypertable_indexrelid, const char *tablespace)
{
	ScanKeyData scankey[2];
	char *indexname = get_rel_name(hypertable_indexrelid);

	ScanKeyInit(&scankey[0],
				Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_id,
				BTEqualStrategyNumber,
				F_INT4EQ,
				Int32GetDatum(ht->fd.id));
	ScanKeyInit(&scankey[1],
				Anum_chunk_index_hypertable_id_hypertable_index_name_idx_hypertable_index_name,
				BTEqualStrategyNumber,
				F_NAMEEQ,
				CStringGetDatum(indexname));

	return chunk_index_scan_update(CHUNK_INDEX_HYPERTABLE_ID_HYPERTABLE_INDEX_NAME_IDX,
								   scankey,
								   2,
								   chunk_index_tuple_set_tablespace,
								   NULL,
								   (char *) tablespace);
}

TSDLLEXPORT void
ts_chunk_index_mark_clustered(Oid chunkrelid, Oid indexrelid)
{
	Relation rel = table_open(chunkrelid, AccessShareLock);

	mark_index_clustered(rel, indexrelid, true);
	CommandCounterIncrement();
	table_close(rel, AccessShareLock);
}

TS_FUNCTION_INFO_V1(ts_chunk_index_clone);

static Oid
chunk_index_duplicate_index(Relation hypertable_rel, Chunk *src_chunk, Oid chunk_index_oid,
							Relation dest_chunk_rel, Oid index_tablespace)
{
	Relation chunk_index_rel = index_open(chunk_index_oid, AccessShareLock);
	ChunkIndexMapping cim;
	Oid constraint_oid;
	Oid new_chunk_indexrelid;

	ts_chunk_index_get_by_indexrelid(src_chunk, chunk_index_oid, &cim);

	constraint_oid = get_index_constraint(cim.parent_indexoid);

	new_chunk_indexrelid = chunk_relation_index_create(hypertable_rel,
													   chunk_index_rel,
													   dest_chunk_rel,
													   OidIsValid(constraint_oid),
													   index_tablespace);

	index_close(chunk_index_rel, NoLock);
	return new_chunk_indexrelid;
}

/*
 * Create versions of every index over src_chunkrelid over chunkrelid.
 * Returns the relids of the new indexes created.
 * New indexes are in the same order as RelationGetIndexList.
 */
TSDLLEXPORT List *
ts_chunk_index_duplicate(Oid src_chunkrelid, Oid dest_chunkrelid, List **src_index_oids,
						 Oid index_tablespace)
{
	Relation hypertable_rel = NULL;
	Relation src_chunk_rel;
	Relation dest_chunk_rel;
	List *index_oids;
	ListCell *index_elem;
	List *new_index_oids = NIL;
	Chunk *src_chunk;

	/* TODO lock order? */
	src_chunk_rel = table_open(src_chunkrelid, AccessShareLock);
	dest_chunk_rel = table_open(dest_chunkrelid, ShareLock);

	src_chunk = ts_chunk_get_by_relid(src_chunkrelid, 0, true);

	hypertable_rel = table_open(src_chunk->hypertable_relid, AccessShareLock);

	index_oids = RelationGetIndexList(src_chunk_rel);
	foreach (index_elem, index_oids)
	{
		Oid chunk_index_oid = lfirst_oid(index_elem);
		Oid new_chunk_indexrelid = chunk_index_duplicate_index(hypertable_rel,
															   src_chunk,
															   chunk_index_oid,
															   dest_chunk_rel,
															   index_tablespace);

		new_index_oids = lappend_oid(new_index_oids, new_chunk_indexrelid);
	}

	table_close(hypertable_rel, AccessShareLock);

	table_close(dest_chunk_rel, NoLock);
	table_close(src_chunk_rel, NoLock);

	if (src_index_oids != NULL)
		*src_index_oids = index_oids;

	return new_index_oids;
}

TS_FUNCTION_INFO_V1(chunk_index_clone);
Datum
ts_chunk_index_clone(PG_FUNCTION_ARGS)
{
	Oid chunk_index_oid = PG_GETARG_OID(0);
	Relation chunk_index_rel;
	Relation hypertable_rel;
	Relation chunk_rel;
	Oid constraint_oid;
	Oid new_chunk_indexrelid;
	Chunk *chunk;
	ChunkIndexMapping cim;

	chunk_index_rel = index_open(chunk_index_oid, AccessShareLock);

	chunk = ts_chunk_get_by_relid(chunk_index_rel->rd_index->indrelid, 0, true);
	ts_chunk_index_get_by_indexrelid(chunk, chunk_index_oid, &cim);

	ts_hypertable_permissions_check(cim.hypertableoid, GetUserId());

	hypertable_rel = table_open(cim.hypertableoid, AccessShareLock);

	/* Need ShareLock on the heap relation we are creating indexes on */
	chunk_rel = table_open(chunk_index_rel->rd_index->indrelid, ShareLock);

	constraint_oid = get_index_constraint(cim.parent_indexoid);

	new_chunk_indexrelid = chunk_relation_index_create(hypertable_rel,
													   chunk_index_rel,
													   chunk_rel,
													   OidIsValid(constraint_oid),
													   InvalidOid);

	table_close(chunk_rel, NoLock);

	table_close(hypertable_rel, AccessShareLock);

	index_close(chunk_index_rel, AccessShareLock);

	PG_RETURN_OID(new_chunk_indexrelid);
}

TS_FUNCTION_INFO_V1(ts_chunk_index_replace);

Datum
ts_chunk_index_replace(PG_FUNCTION_ARGS)
{
	Oid chunk_index_oid_old = PG_GETARG_OID(0);
	Oid chunk_index_oid_new = PG_GETARG_OID(1);
	Relation index_rel;
	Chunk *chunk;
	ChunkIndexMapping cim;

	Oid constraint_oid;
	char *name;

	index_rel = index_open(chunk_index_oid_old, ShareLock);

	/* check permissions */
	chunk = ts_chunk_get_by_relid(index_rel->rd_index->indrelid, 0, true);
	ts_chunk_index_get_by_indexrelid(chunk, chunk_index_oid_old, &cim);
	ts_hypertable_permissions_check(cim.hypertableoid, GetUserId());

	name = pstrdup(RelationGetRelationName(index_rel));
	constraint_oid = get_index_constraint(chunk_index_oid_old);

	index_close(index_rel, NoLock);

	if (OidIsValid(constraint_oid))
	{
		ObjectAddress constraintobj = {
			.classId = ConstraintRelationId,
			.objectId = constraint_oid,
		};

		performDeletion(&constraintobj, DROP_RESTRICT, 0);
	}
	else
	{
		ObjectAddress idxobj = {
			.classId = RelationRelationId,
			.objectId = chunk_index_oid_old,
		};

		performDeletion(&idxobj, DROP_RESTRICT, 0);
	}

	RenameRelationInternalCompat(chunk_index_oid_new, name, false, true);

	PG_RETURN_VOID();
}
