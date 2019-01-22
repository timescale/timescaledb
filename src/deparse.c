/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

#include <postgres.h>
#include <utils/rel.h>
#include <lib/stringinfo.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/relcache.h>
#include <catalog/indexing.h>
#include <utils/ruleutils.h>
#include <utils/syscache.h>
#include <commands/tablespace.h>
#include <catalog/pg_class.h>
#include <utils/rel.h>
#include <access/relscan.h>
#include <utils/fmgroids.h>
#include <catalog/pg_constraint.h>
#include <catalog/pg_index.h>
#include <nodes/pg_list.h>
#include "export.h"
#include "compat.h"
#include "deparse.h"

/**
 * Deparse a table into a set of SQL commands that can be used to recreate it.
 * Together with column definiton it deparses constraints, indexes, triggers and rules as well.
 * There are some table types that are not supported: temporary, partitioned,
 * foreign, inherited and a table that uses options. Row security is also not supported.
 */

TS_FUNCTION_INFO_V1(ts_get_tabledef);

Datum
ts_get_tabledef(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	PG_RETURN_TEXT_P(cstring_to_text(deparse_get_tabledef(relid)));
}

static void
deparse_columns (StringInfo stmt, Relation rel)
{
	int			att_idx;
	TupleDesc	rel_desc = RelationGetDescr(rel);
	TupleConstr *constraints = rel_desc->constr;

	for (att_idx = 0; att_idx < rel_desc->natts; att_idx++)
	{
		int			dim_idx;
		Form_pg_attribute attr = TupleDescAttr(rel_desc, att_idx);

		if (attr->attisdropped)
			continue;

		appendStringInfo(stmt, "\"%s\" %s",
						 NameStr(attr->attname),
						 format_type_with_typemod_qualified(attr->atttypid, attr->atttypmod));

		if (attr->attnotnull)
			appendStringInfoString(stmt, " NOT NULL");

		if (OidIsValid(attr->attcollation))
			appendStringInfo(stmt, " COLLATE \"%s\"",
							 get_collation_name(attr->attcollation));

		if (attr->atthasdef)
		{
			int			co_idx;

			for (co_idx = 0; co_idx < constraints->num_defval; co_idx++)
			{
				AttrDefault attr_def = constraints->defval[co_idx];

				if (attr->attnum == attr_def.adnum)
				{
					char	   *attr_default = TextDatumGetCString(DirectFunctionCall2(pg_get_expr,
																					   CStringGetTextDatum(attr_def.adbin),
																					   ObjectIdGetDatum(rel->rd_id)));

					appendStringInfo(stmt, " DEFAULT %s", attr_default);
					break;
				}
			}
		}

		for (dim_idx = 1; dim_idx < attr->attndims; dim_idx++)
			appendStringInfoString(stmt, "[]");

		if (att_idx != (rel_desc->natts - 1))
			appendStringInfoString(stmt, ", ");
	}
	appendStringInfoString(stmt, ") ");
}

static List *
deparse_constraints(StringInfo stmt, Oid relid)
{
	ScanKeyData skey;
	Relation	constraint_rel;
	SysScanDesc scan;
	HeapTuple	htup;
	List	    *constraint_indexes = NIL;

	ScanKeyInit(&skey,
				Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ, relid);

	constraint_rel = heap_open(ConstraintRelationId, AccessShareLock);
	scan = systable_beginscan(constraint_rel, ConstraintRelidTypidNameIndexId, true,
							  NULL, 1, &skey);

	while (HeapTupleIsValid(htup = systable_getnext(scan)))
	{
		Form_pg_constraint pg_constraint = (Form_pg_constraint) GETSTRUCT(htup);

		const char *constraint_stmt = pg_get_constraintdef_command(HeapTupleGetOid(htup));

		if (pg_constraint->conindid != 0)
			constraint_indexes = lappend_oid(constraint_indexes, pg_constraint->conindid);
		appendStringInfo(stmt, "%s;", constraint_stmt);
	}

	systable_endscan(scan);
	heap_close(constraint_rel, AccessShareLock);
	return constraint_indexes;
}

static void
deparse_indexes(StringInfo stmt, Relation rel, List *exclude_indexes)
{
	ListCell   *cell;

	foreach(cell, RelationGetIndexList(rel))
	{
		Oid			indexid = lfirst_oid(cell);

		if (!list_member_oid(exclude_indexes, indexid))
		{
			HeapTuple	index_tuple;
			Form_pg_index index;
			char	   *index_stmt;

			index_tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexid));
			if (!HeapTupleIsValid(index_tuple))
				ereport(ERROR,
						(errmsg("cache lookup failed for index %u", indexid)));

			index = (Form_pg_index) GETSTRUCT(index_tuple);

			index_stmt = pg_get_indexdef_string(index->indexrelid);
			appendStringInfo(stmt, "%s;", index_stmt);

			ReleaseSysCache(index_tuple);
		}
	}
}

static void
deparse_triggers(StringInfo stmt, Relation rel)
{
	Datum		trigger_stmt;
	FunctionCallInfoData fcinfo;

	InitFunctionCallInfoData(fcinfo, NULL, 1, InvalidOid, NULL, NULL);
	fcinfo.argnull[0] = false;

	if (rel->trigdesc != NULL)
	{
		int			i;

		for (i = 0; i < rel->trigdesc->numtriggers; i++)
		{
			const Trigger trigger = rel->trigdesc->triggers[i];

			if (!trigger.tgisinternal)
			{
				fcinfo.arg[0] = Int32GetDatum(trigger.tgoid);
				trigger_stmt = pg_get_triggerdef(&fcinfo);
				appendStringInfo(stmt, "%s;", TextDatumGetCString(trigger_stmt));
			}
		}
	}
}

static void
deparse_rules(StringInfo stmt, Relation rel)
{
	Datum		rule_stmt;
	FunctionCallInfoData fcinfo;

	InitFunctionCallInfoData(fcinfo, NULL, 1, InvalidOid, NULL, NULL);
	fcinfo.argnull[0] = false;

	if (rel->rd_rules != NULL)
	{
		int			i;

		for (i = 0; i < rel->rd_rules->numLocks; i++)
		{
			const RewriteRule *rule = rel->rd_rules->rules[i];

			fcinfo.arg[0] = Int32GetDatum(rule->ruleId);
			rule_stmt = pg_get_ruledef(&fcinfo);
			appendStringInfo(stmt, "%s;", TextDatumGetCString(rule_stmt));
		}
	}
}

static void
validate_relation(Relation rel)
{
	if (rel->rd_rel->relkind != RELKIND_RELATION)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("given relation is not an ordinary table")));
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_TEMP)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("temporary table is not supported")));
	}
	if (rel->rd_rel->relrowsecurity)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row security is not supported")));
}

const char *
deparse_get_tabledef(Oid relid)
{
	StringInfo	stmt = makeStringInfo();
	List	   *exclude_indexes = NIL;
	Oid			tablespace;

	Relation	rel = relation_open(relid, AccessShareLock);

	if (rel == NULL)
		ereport(ERROR,
				(errmsg("relation with id %d not found", relid)));

	validate_relation(rel);

	appendStringInfoString(stmt, "CREATE");
	if (rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED)
		appendStringInfoString(stmt, " UNLOGGED");
	appendStringInfoString(stmt, " TABLE");

	appendStringInfo(stmt, " \"%s\".\"%s\" (",
					 get_namespace_name(rel->rd_rel->relnamespace),
					 NameStr(rel->rd_rel->relname));

	deparse_columns (stmt, rel);

	tablespace = get_rel_tablespace(relid);
	if (tablespace != InvalidOid)
		appendStringInfo(stmt, " TABLESPACE %s", get_tablespace_name(tablespace));

	appendStringInfoString(stmt, ";");
	exclude_indexes = deparse_constraints(stmt, relid);
	deparse_indexes(stmt, rel, exclude_indexes);
	deparse_triggers(stmt, rel);
	deparse_rules(stmt, rel);

	relation_close(rel, AccessShareLock);
	return stmt->data;
}
