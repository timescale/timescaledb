/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/attmap.h>
#include <access/toast_compression.h>
#include <catalog/heap.h>
#include <catalog/pg_constraint.h>
#include <commands/tablecmds.h>
#include <executor/executor.h>
#include <nodes/makefuncs.h>
#include <nodes/parsenodes.h>
#include <rewrite/rewriteManip.h>
#include <utils/partcache.h>

#include "chunk.h"
#include "extension.h"
#include "guc.h"
#include "hypercube.h"
#include "hypertable.h"
#include "partition_chunk.h"

void _executor_init(void);
void _executor_fini(void);
static ExecutorEnd_hook_type prev_executor_end_hook = NULL;

/*
 * Cache and the memory context to store recently created chunks to be attached
 * as partitions.
 */
static HTAB *PartChunkCache = NULL;
static MemoryContext PartChunkCacheCxt = NULL;

/*
 * Transaction callback to clean up the partition chunk cache on abort.
 * Memory context is deleted by the portal context cleanup. Just nullify the
 * pointers here.
 */
static void
partcache_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			PartChunkCache = NULL;
			PartChunkCacheCxt = NULL;
			break;

		default:
			/* do nothing? */
			break;
	}
}

/*
 * Insert a chunk into the partition cache.
 */
void
ts_partition_cache_insert_chunk(const Hypertable *ht, Oid chunk_relid)
{
	PartChunkCacheEntry *entry;
	bool found;

	if (PartChunkCache == NULL)
	{
		if (PartChunkCacheCxt == NULL)
			PartChunkCacheCxt = AllocSetContextCreate(PortalContext,
													  "partition chunk cache",
													  ALLOCSET_DEFAULT_SIZES);

		HASHCTL ctl;
		ctl.hcxt =
			AllocSetContextCreate(PortalContext, "partition chunk cache", ALLOCSET_DEFAULT_SIZES);
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(PartChunkCacheEntry);

		PartChunkCache = hash_create("partition chunk cache",
									 256, /* start small, grows automatically */
									 &ctl,
									 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
		RegisterXactCallback(partcache_xact_callback, NULL);
	}

	entry = hash_search(PartChunkCache, &ht->main_table_relid, HASH_ENTER, &found);
	if (!found)
		entry->chunk_oids = NIL;

	MemoryContext oldctx = MemoryContextSwitchTo(PartChunkCacheCxt);
	entry->chunk_oids = lappend_oid(entry->chunk_oids, chunk_relid);
	MemoryContextSwitchTo(oldctx);
}

/*
 * Get a partition cache entry by hypertable relid.
 */
PartChunkCacheEntry *
ts_partition_cache_get_by_hypertable(Oid ht_relid)
{
	PartChunkCacheEntry *entry;

	if (PartChunkCache == NULL)
		return NULL;

	entry = (PartChunkCacheEntry *) hash_search(PartChunkCache, &ht_relid, HASH_FIND, NULL);

	return entry;
}

/*
 * Destroy the partition chunk cache.
 */
void
ts_partition_cache_destroy(void)
{
	if (PartChunkCache != NULL)
	{
		hash_destroy(PartChunkCache);
		PartChunkCache = NULL;
		PartChunkCacheCxt = NULL;
	}
}

/*
 * Fill the attribute and constraint lists by copying from the parent hypertable attributes.
 * Partition chunk's attributes are derived from the hypertable's attributes including storage,
 * compression, generation expressions, and default values.
 *
 * The constraints list is filled with the CHECK and NOT NULL constraints on attributes.
 *
 * This code is adapted from MergeAttributes() in tablecmds.c.
 */
void
ts_partition_chunk_prepare_attributes(Oid ht_relid, List **attlist, List **constraints)
{
	Relation rel = table_open(ht_relid, AccessShareLock);
	TupleDesc tupleDesc = RelationGetDescr(rel);
	TupleConstr *constr = tupleDesc->constr;
	AttrMap *newattmap = make_attrmap(tupleDesc->natts);
	List *inherited_defaults = NIL;
	List *cols_with_defaults = NIL;
	int child_attno = 0;

	for (int parent_attno = 1; parent_attno <= tupleDesc->natts; parent_attno++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDesc, parent_attno - 1);
		char *attributeName = NameStr(attribute->attname);
		ColumnDef *newdef;

		/*
		 * Ignore dropped columns in the parent.
		 */
		if (attribute->attisdropped)
			continue; /* leave newattmap->attnums entry as zero */

		/*
		 * Create new column definition
		 */
		newdef = makeColumnDef(attributeName,
							   attribute->atttypid,
							   attribute->atttypmod,
							   attribute->attcollation);
		newdef->type = T_ColumnDef;
		newdef->is_not_null = attribute->attnotnull;
		newdef->storage = attribute->attstorage;
		newdef->generated = attribute->attgenerated;
		if (CompressionMethodIsValid(attribute->attcompression))
			newdef->compression = pstrdup(GetCompressionMethodName(attribute->attcompression));

		newdef->inhcount = 0;
		newdef->is_local = false;
		newattmap->attnums[parent_attno - 1] = ++child_attno;

		/*
		 * Locate default/generation expression if any
		 */
		if (attribute->atthasdef)
		{
			Node *this_default = NULL;

			/* Find default in constraint structure */
			if (constr != NULL)
			{
				AttrDefault *attrdef = constr->defval;

				for (int i = 0; i < constr->num_defval; i++)
				{
					if (attrdef[i].adnum == parent_attno)
					{
						this_default = stringToNode(attrdef[i].adbin);
						break;
					}
				}
			}
			if (this_default == NULL)
				elog(ERROR,
					 "default expression not found for attribute %d of relation \"%s\"",
					 parent_attno,
					 RelationGetRelationName(rel));

			/*
			 * If it's a GENERATED default, it might contain Vars that
			 * need to be mapped to the inherited column(s)' new numbers.
			 * We can't do that till newattmap is ready, so just remember
			 * all the inherited default expressions for the moment.
			 */
			inherited_defaults = lappend(inherited_defaults, this_default);
			cols_with_defaults = lappend(cols_with_defaults, newdef);
		}
		*attlist = lappend(*attlist, newdef);
	}

	/*
	 * Now process any inherited default expressions, adjusting attnos
	 * using the completed newattmap map.
	 */
	ListCell *lc1, *lc2;
	forboth (lc1, inherited_defaults, lc2, cols_with_defaults)
	{
		Node *this_default = (Node *) lfirst(lc1);
		ColumnDef *def = (ColumnDef *) lfirst(lc2);
		bool found_whole_row;

		/* Adjust Vars to match new table's column numbering */
		this_default =
			map_variable_attnos(this_default, 1, 0, newattmap, InvalidOid, &found_whole_row);

		/*
		 * For the moment we have to reject whole-row variables.  We could
		 * convert them, if we knew the new table's rowtype OID, but that
		 * hasn't been assigned yet.  (A variable could only appear in a
		 * generation expression, so the error message is correct.)
		 */
		if (found_whole_row)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot convert whole-row table reference"),
					 errdetail("Generation expression for column \"%s\" contains a whole-row "
							   "reference to table \"%s\".",
							   def->colname,
							   RelationGetRelationName(rel))));

		Assert(def->raw_default == NULL);
		def->cooked_default = this_default;
	}

	/*
	 * Now copy the CHECK constraints of this parent, adjusting attnos
	 * using the completed newattmap map.
	 */
	if (constr && constr->num_check > 0)
	{
		for (int i = 0; i < constr->num_check; i++)
		{
			Node *expr;
			bool found_whole_row;

			/* ignore if the constraint is non-inheritable */
			if (constr->check[i].ccnoinherit)
				continue;

			/* Adjust Vars to match new table's column numbering */
			expr = map_variable_attnos(stringToNode(constr->check[i].ccbin),
									   1,
									   0,
									   newattmap,
									   InvalidOid,
									   &found_whole_row);

			/*
			 * For the moment we have to reject whole-row variables. We
			 * could convert them, if we knew the new table's rowtype OID,
			 * but that hasn't been assigned yet.
			 */
			if (found_whole_row)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot convert whole-row table reference")));

			Constraint *c = makeNode(Constraint);
			c->type = T_Constraint;
			c->contype = CONSTR_CHECK;
			c->conname = pstrdup(constr->check[i].ccname);
			c->skip_validation = !constr->check[i].ccvalid;
			c->initially_valid = true;
			c->location = -1;
			c->is_no_inherit = constr->check[i].ccnoinherit;
			c->raw_expr = NULL;
			c->cooked_expr = nodeToString(expr);
			*constraints = lappend(*constraints, c);
#if PG18_GE
			c->is_enforced = constr->check[i].ccenforced;
#endif
		}
	}

#if PG18_GE
	/* A row is added into pg_constraints for each NOT NULL constraint since PG18 */
	List *notnulls = RelationGetNotNullConstraints(ht_relid, false, false);
	foreach_ptr(Constraint, nn, notnulls)
	{
		Assert(nn->contype == CONSTR_NOTNULL);
		*constraints = lappend(*constraints, nn);
	}
#endif

	free_attrmap(newattmap);
	table_close(rel, NoLock);
}

/*
 * Attach a standalone chunk to a partitioned hypertable as a partition.
 */
static void
partition_chunk_attach(const Hypertable *ht, const Chunk *chunk)
{
	/* Currently only single-dimensional partitioned hypertables are supported */
	Assert(chunk->cube->num_slices == 1);

	const Dimension *dim =
		ts_hyperspace_get_dimension_by_id(ht->space, chunk->cube->slices[0]->fd.dimension_id);
	Oid dimtype = ts_dimension_get_partition_type(dim);

	A_Const prd_lower =  {
		.type = T_A_Const,
		.val.sval = {
			.type = T_String,
			.sval = ts_internal_to_time_string(chunk->cube->slices[0]->fd.range_start, dimtype),
		},
		.location = -1
	};
	A_Const prd_upper =  {
		.type = T_A_Const,
		.val.sval = {
			.type = T_String,
			.sval = ts_internal_to_time_string(chunk->cube->slices[0]->fd.range_end, dimtype),
		},
		.location = -1
	};
	PartitionBoundSpec pbspec = {
		.type = T_PartitionBoundSpec,
		.is_default = false,
		.location = -1,
		.strategy = PARTITION_STRATEGY_RANGE,
		.lowerdatums = list_make1(&prd_lower),
		.upperdatums = list_make1(&prd_upper),
	};
	PartitionCmd partcmd = {
		.type = T_PartitionCmd,
		.name = makeRangeVar((char *) NameStr(chunk->fd.schema_name),
							 (char *) NameStr(chunk->fd.table_name),
							 0),
		.bound = &pbspec,
		.concurrent = false,
	};
	AlterTableCmd altercmd = {
		.type = T_AlterTableCmd,
		.subtype = AT_AttachPartition,
		.def = (Node *) &partcmd,
		.missing_ok = false,
	};
	AlterTableStmt alterstmt = {
		.type = T_AlterTableStmt,
		.cmds = list_make1(&altercmd),
		.missing_ok = false,
		.objtype = OBJECT_TABLE,
		.relation = makeRangeVar((char *) NameStr(ht->fd.schema_name),
								 (char *) NameStr(ht->fd.table_name),
								 0),
	};

	LOCKMODE lockmode = AlterTableGetLockLevel(alterstmt.cmds);
	AlterTableUtilityContext atcontext = {
		.relid = AlterTableLookupRelation(&alterstmt, lockmode),
	};

	AlterTable(&alterstmt, lockmode, &atcontext);
}

/*
 * ExecutoreEnd hook to attach cached partition chunks to their hypertables.
 */

static void
ts_executor_end_hook(QueryDesc *queryDesc)
{
	ListCell *lc;

	if (prev_executor_end_hook)
		prev_executor_end_hook(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);

	/*
	 * Chunks cannot be created as a partition or attached as partition until
	 * this point since Postgres does not allow such operations when there is
	 * an open reference to the parent table. ModifyTable node opens the parent
	 * table and it only gets closed in ExecEndPlan.
	 */
	if (queryDesc->operation == CMD_INSERT && PartChunkCache != NULL && ts_extension_is_loaded())
	{
		Cache *hcache = ts_hypertable_cache_pin();
		HASH_SEQ_STATUS status;
		PartChunkCacheEntry *entry;

		hash_seq_init(&status, PartChunkCache);
		while ((entry = hash_seq_search(&status)) != NULL)
		{
			foreach (lc, entry->chunk_oids)
			{
				Hypertable *ht =
					ts_hypertable_cache_get_entry(hcache, entry->ht_relid, CACHE_FLAG_MISSING_OK);

				if (ht)
					partition_chunk_attach(ht, ts_chunk_get_by_relid(lfirst_oid(lc), true));
			}
		}

		ts_cache_release(&hcache);
		ts_partition_cache_destroy();
	}
}

void
_executor_init(void)
{
	prev_executor_end_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = ts_executor_end_hook;
}

void
_executor_fini(void)
{
	ExecutorEnd_hook = prev_executor_end_hook;
}
