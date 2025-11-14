/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/htup_details.h>
#include <access/xact.h>
#include <catalog/dependency.h>
#include <catalog/index.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/objectaddress.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_depend.h>
#include <catalog/pg_index.h>
#include <commands/cluster.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <commands/tablespace.h>
#include <miscadmin.h>
#include <nodes/parsenodes.h>
#include <optimizer/optimizer.h>
#include <utils/builtins.h>
#include <utils/fmgroids.h>
#include <utils/lsyscache.h>
#include <utils/rel.h>
#include <utils/syscache.h>

#include "chunk.h"
#include "chunk_index.h"
#include "hypertable.h"
#include "hypertable_cache.h"
#include "indexing.h"
#include "scan_iterator.h"
#include "scanner.h"
#include "ts_catalog/catalog.h"

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

		var->varattno = ts_map_attno(ht_relid, chunkrel->rd_id, var->varattno);
		var->varattnosyn = var->varattno;
	}
}

/*
 * Adjust column reference attribute numbers for regular indexes.
 */
static void
chunk_adjust_colref_attnos(IndexInfo *ii, Oid ht_relid, Relation chunkrel)
{
	int i;

	for (i = 0; i < ii->ii_NumIndexAttrs; i++)
	{
		/* zeroes indicate expressions */
		if (ii->ii_IndexAttrNumbers[i] == 0)
			continue;
		/* we must not use get_attname on the index here as the index column names
		 * are independent of parent relation column names. Instead we need to look
		 * up the attno of the referenced hypertable column and do the matching
		 * with the hypertable column name */
		ii->ii_IndexAttrNumbers[i] =
			ts_map_attno(ht_relid, chunkrel->rd_id, ii->ii_IndexAttrNumbers[i]);
	}
}

void
ts_adjust_indexinfo_attnos(IndexInfo *indexinfo, Oid ht_relid, Relation chunkrel)
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
	 * We need to handle 3 places:
	 * - direct column references in ii_IndexAttrNumbers
	 * - references in expressions in ii_Expressions
	 * - references in expressions in ii_Predicate
	 */
	chunk_adjust_colref_attnos(indexinfo, ht_relid, chunkrel);

	if (indexinfo->ii_Expressions || indexinfo->ii_Predicate)
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
	bool skip_mapping = false;

	/*
	 * If the supplied template index is not on the hypertable we must not do attnum
	 * mapping based on the hypertable. Ideally we would check for the template being
	 * on the chunk but we cannot do that since when we rebuild a chunk the new chunk
	 * has a different id. But the template index should always be either on the
	 * hypertable or on a relation with the same physical layout as chunkrel.
	 */
	if (IndexGetRelation(template_indexrel->rd_id, false) != htrel->rd_id)
		skip_mapping = true;

	/*
	 * Convert the IndexInfo's attnos to match the chunk instead of the
	 * hypertable
	 */
	if (!skip_mapping &&
		chunk_index_need_attnos_adjustment(RelationGetDescr(htrel), RelationGetDescr(chunkrel)))
		ts_adjust_indexinfo_attnos(indexinfo, htrel->rd_id, chunkrel);

	hypertable_id = ts_hypertable_relid_to_id(htrel->rd_id);
	Assert(hypertable_id != INVALID_HYPERTABLE_ID);

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
										   InvalidOid,
										   InvalidOid,
										   indexinfo,
										   colnames,
										   template_indexrel->rd_rel->relam,
										   tablespace,
										   template_indexrel->rd_indcollation,
										   indclassoid->values,
										   NULL, /* opclassOptions */
										   template_indexrel->rd_indoption,
										   NULL, /* stattargets */
										   reloptions,
										   flags,
										   0,	  /* constr_flags constant and 0
												   * for now */
										   false, /* allow system table mods */
										   false, /* is internal */
										   NULL); /* constraintId */

	ReleaseSysCache(tuple);

	return chunk_indexrelid;
}

static Oid
chunk_index_find_matching(Relation chunk_rel, Oid ht_indexoid)
{
	List *indexlist = RelationGetIndexList(chunk_rel);
	ListCell *lc;
	foreach (lc, indexlist)
	{
		Oid indexoid = lfirst_oid(lc);
		if (ts_indexing_compare(indexoid, ht_indexoid))
			return indexoid;
	}
	list_free(indexlist);
	return InvalidOid;
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
				   int32 chunk_id, Relation chunkrel, Oid constraint_oid, Oid index_tblspc)
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

	chunk_indexrelid = chunk_index_find_matching(chunkrel, RelationGetRelid(hypertable_idxrel));
	if (!OidIsValid(chunk_indexrelid))
	{
		chunk_indexrelid = chunk_relation_index_create(hypertable_rel,
													   hypertable_idxrel,
													   chunkrel,
													   false,
													   index_tblspc);
	}
}

void
ts_chunk_index_create_from_adjusted_index_info(int32 hypertable_id, Relation hypertable_idxrel,
											   int32 chunk_id, Relation chunkrel,
											   IndexInfo *indexinfo)
{
	ts_chunk_index_create_post_adjustment(hypertable_id,
										  hypertable_idxrel,
										  chunkrel,
										  indexinfo,
										  false,
										  false);
}

/*
 * Create all indexes on a chunk, given the indexes that exists on the chunk's
 * hypertable.
 */
void
ts_chunk_index_create_all(int32 hypertable_id, Oid hypertable_relid, int32 chunk_id, Oid chunkrelid,
						  Oid index_tblspc)
{
	Relation htrel;
	Relation chunkrel;
	List *indexlist;
	ListCell *lc;
	const char chunk_relkind = get_rel_relkind(chunkrelid);

	/* Foreign table chunks don't support indexes */
	if (chunk_relkind == RELKIND_FOREIGN_TABLE)
		return;

	Assert(chunk_relkind == RELKIND_RELATION);

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
						   get_index_constraint(hypertable_idxoid),
						   index_tblspc);

		index_close(hypertable_idxrel, AccessShareLock);
	}

	table_close(chunkrel, NoLock);
	table_close(htrel, AccessShareLock);
}

List *
ts_chunk_index_get_mappings(Hypertable *ht, Oid hypertable_indexrelid)
{
	List *mappings = NIL;
	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.id);
	ListCell *lc;

	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		if (!OidIsValid(chunk->table_id))
			continue;

		Relation chunk_rel = table_open(chunk->table_id, AccessShareLock);
		Oid chunk_indexrelid =
			ts_chunk_index_get_by_hypertable_indexrelid(chunk_rel, hypertable_indexrelid);
		table_close(chunk_rel, AccessShareLock);
		if (OidIsValid(chunk_indexrelid))
		{
			ChunkIndexMapping *cim = palloc0(sizeof(ChunkIndexMapping));
			cim->chunkoid = chunk->table_id;
			cim->indexoid = chunk_indexrelid;
			cim->parent_indexoid = hypertable_indexrelid;
			cim->hypertableoid = ht->fd.id;
			mappings = lappend(mappings, cim);
		}
	}
	return mappings;
}

TSDLLEXPORT Oid
ts_chunk_index_get_by_hypertable_indexrelid(Relation chunk_rel, Oid ht_indexoid)
{
	List *indexlist = RelationGetIndexList(chunk_rel);
	ListCell *lc;
	Oid chunk_index_oid = InvalidOid;

	foreach (lc, indexlist)
	{
		Oid indexoid = lfirst_oid(lc);

		if (ts_indexing_compare(indexoid, ht_indexoid))
		{
			chunk_index_oid = indexoid;
			break;
		}
	}
	list_free(indexlist);
	return chunk_index_oid;
}

void
ts_chunk_index_rename(Hypertable *ht, Oid hypertable_indexrelid, const char *ht_name)
{
	ListCell *lc;
	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.id);
	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		if (!OidIsValid(chunk->table_id))
			continue;

		Relation chunk_rel = table_open(chunk->table_id, AccessExclusiveLock);
		Oid chunk_indexrelid =
			ts_chunk_index_get_by_hypertable_indexrelid(chunk_rel, hypertable_indexrelid);
		table_close(chunk_rel, NoLock);

		/* If there is no matching index on the chunk, skip it */
		if (OidIsValid(chunk_indexrelid))
		{
			Oid chunk_schemaoid = get_namespace_oid(NameStr(chunk->fd.schema_name), false);
			const char *chunk_old_name = get_rel_name(chunk_indexrelid);
			const char *chunk_new_name =
				chunk_index_choose_name(NameStr(chunk->fd.table_name), ht_name, chunk_schemaoid);

			/*
			 * Index might also have a constraint which we track separately in our catalog
			 * and needs to be updated too
			 */
			ts_chunk_constraint_adjust_meta(chunk->fd.id, ht_name, chunk_old_name, chunk_new_name);
			RenameRelationInternal(chunk_indexrelid, chunk_new_name, false, true);
		}
	}
}

void
ts_chunk_index_set_tablespace(Hypertable *ht, Oid hypertable_indexrelid, char *tablespace)
{
	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.id);
	ListCell *lc;

	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		Relation chunk_rel = table_open(chunk->table_id, AccessExclusiveLock);
		Oid chunk_indexrelid =
			ts_chunk_index_get_by_hypertable_indexrelid(chunk_rel, hypertable_indexrelid);
		table_close(chunk_rel, NoLock);

		if (OidIsValid(chunk_indexrelid))
		{
			AlterTableCmd *cmd = makeNode(AlterTableCmd);
			List *cmds = NIL;

			cmd->subtype = AT_SetTableSpace;
			cmd->name = tablespace;
			cmds = lappend(cmds, cmd);

			ts_alter_table_with_event_trigger(chunk_indexrelid, NULL, cmds, false);
		}
	}
}

TSDLLEXPORT void
ts_chunk_index_mark_clustered(Oid chunkrelid, Oid indexrelid)
{
	Relation rel = table_open(chunkrelid, AccessShareLock);

	mark_index_clustered(rel, indexrelid, true);
	CommandCounterIncrement();
	table_close(rel, AccessShareLock);
}

static Oid
chunk_index_duplicate_index(Relation hypertable_rel, Chunk *src_chunk, Oid chunk_index_oid,
							Relation dest_chunk_rel, Oid index_tablespace)
{
	Relation chunk_index_rel = index_open(chunk_index_oid, AccessShareLock);
	Oid constraint_oid;
	Oid new_chunk_indexrelid;

	constraint_oid = get_index_constraint(chunk_index_oid);

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

	src_chunk_rel = table_open(src_chunkrelid, AccessShareLock);
	dest_chunk_rel = table_open(dest_chunkrelid, ShareLock);

	src_chunk = ts_chunk_get_by_relid(src_chunkrelid, true);

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

void
ts_chunk_index_move_all(Oid chunk_relid, Oid index_tblspc)
{
	Relation chunkrel;
	List *indexlist;
	ListCell *lc;
	const char chunk_relkind = get_rel_relkind(chunk_relid);

	/* execute ALTER INDEX .. SET TABLESPACE for each index on the chunk */
	AlterTableCmd cmd = { .type = T_AlterTableCmd,
						  .subtype = AT_SetTableSpace,
						  .name = get_tablespace_name(index_tblspc) };

	/* Foreign table chunks don't support indexes */
	if (chunk_relkind == RELKIND_FOREIGN_TABLE)
		return;

	Assert(chunk_relkind == RELKIND_RELATION);

	chunkrel = table_open(chunk_relid, AccessShareLock);

	indexlist = RelationGetIndexList(chunkrel);

	foreach (lc, indexlist)
	{
		Oid chunk_idxoid = lfirst_oid(lc);
		ts_alter_table_with_event_trigger(chunk_idxoid, NULL, list_make1(&cmd), false);
	}
	table_close(chunkrel, AccessShareLock);
}
