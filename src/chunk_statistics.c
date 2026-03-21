/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <access/heapam.h>
#include <access/htup_details.h>
#include <catalog/indexing.h>
#include <catalog/namespace.h>
#include <catalog/pg_statistic_ext.h>
#include <commands/defrem.h>
#include <commands/tablecmds.h>
#include <nodes/makefuncs.h>
#include <nodes/nodeFuncs.h>
#include <nodes/nodes.h>
#include <nodes/parsenodes.h>
#include <nodes/readfuncs.h>
#include <optimizer/optimizer.h>
#include <parser/parse_node.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>
#include <utils/memutils.h>
#include <utils/regproc.h>
#include <utils/rel.h>
#include <utils/relcache.h>
#include <utils/syscache.h>

#include "compat/compat.h"
#include "chunk.h"
#include "chunk_statistics.h"
#include "hypertable.h"
#include "utils.h"

/* Forward declarations */
static CreateStatsStmt *build_chunk_extended_statistics_stmt(Chunk *chunk,
															 Form_pg_statistic_ext stat_ext_form,
															 HeapTuple stat_ext_tuple,
															 Oid hypertable_relid, Oid chunk_relid);
static List *convert_stxkeys_to_chunk_exprs(int2vector *stxkeys, Oid hypertable_relid,
											Oid chunk_relid);
static List *convert_stxkind_to_string_list(ArrayType *stxkind);
static List *process_chunk_extended_statistics_exprs(HeapTuple stat_ext_tuple, List *chunk_exprs,
													 Oid hypertable_relid, Oid chunk_relid);
static char *chunk_extended_statistics_choose_name(const char *chunk_table_name,
												   const char *stat_ext_name, Oid namespaceid);
static bool compare_extended_statistics_columns(Form_pg_statistic_ext stat_ext_form1,
												Form_pg_statistic_ext stat_ext_form2);
static bool compare_extended_statistics_kinds(HeapTuple stat_ext_tuple1, HeapTuple stat_ext_tuple2);
static bool compare_extended_statistics_expressions(HeapTuple stat_ext_tuple1,
													HeapTuple stat_ext_tuple2);
static bool compare_extended_statistics_name_pattern(const char *chunk_stat_name,
													 const char *hypertable_stat_name);

/*
 * Create all extended statistics on a chunk, given the extended statistics
 * that exist on the chunk's hypertable.
 *
 * Similar to ts_chunk_index_create_all() for indexes.
 */
void
ts_chunk_extended_statistics_create_all(int32 hypertable_id, Oid hypertable_relid, int32 chunk_id,
										Oid chunk_relid)
{
	Relation hypertable_relation;
	Relation chunk_relation;
	List *extended_statistics_list;
	ListCell *extended_statistics_cell;
	const char chunk_relkind = get_rel_relkind(chunk_relid);

	/* Foreign table chunks don't support extended statistics */
	if (chunk_relkind == RELKIND_FOREIGN_TABLE)
		return;

	Assert(chunk_relkind == RELKIND_RELATION);

	hypertable_relation = table_open(hypertable_relid, AccessShareLock);
	chunk_relation = table_open(chunk_relid, ShareLock);

	/*
	 * Get all extended statistics on the hypertable.
	 * RelationGetStatExtList() returns a list of OIDs for all extended
	 * statistics objects defined on the relation, similar to how
	 * RelationGetIndexList() works for indexes.
	 */
	extended_statistics_list = RelationGetStatExtList(hypertable_relation);

	foreach (extended_statistics_cell, extended_statistics_list)
	{
		Oid stat_ext_oid = lfirst_oid(extended_statistics_cell);
		HeapTuple stat_ext_tuple;
		Form_pg_statistic_ext stat_ext_form;
		Chunk *chunk;
		CreateStatsStmt *chunk_stmt;

		/* Get the extended statistics tuple from system cache */
		stat_ext_tuple = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(stat_ext_oid));
		if (!HeapTupleIsValid(stat_ext_tuple))
			continue; /* Should not happen, but skip if it does */

		stat_ext_form = (Form_pg_statistic_ext) GETSTRUCT(stat_ext_tuple);

		/* Get chunk structure */
		chunk = ts_chunk_get_by_relid(chunk_relid, true);
		if (chunk == NULL)
		{
			ReleaseSysCache(stat_ext_tuple);
			continue;
		}

		/* Build CreateStatsStmt node */
		chunk_stmt = build_chunk_extended_statistics_stmt(chunk,
														  stat_ext_form,
														  stat_ext_tuple,
														  hypertable_relid,
														  chunk_relid);
		if (chunk_stmt == NULL)
		{
			ReleaseSysCache(stat_ext_tuple);
			continue;
		}

		/* Create extended statistics on chunk */
		CreateStatistics(chunk_stmt, false);

		ReleaseSysCache(stat_ext_tuple);
	}

	list_free(extended_statistics_list);
	table_close(chunk_relation, NoLock);
	table_close(hypertable_relation, AccessShareLock);
}

/*
 * Build CreateStatsStmt node for creating extended statistics on a chunk.
 *
 * Returns: CreateStatsStmt node, or NULL if extended statistics cannot be created
 *          (e.g., expressions present, invalid data).
 */
static CreateStatsStmt *
build_chunk_extended_statistics_stmt(Chunk *chunk, Form_pg_statistic_ext stat_ext_form,
									 HeapTuple stat_ext_tuple, Oid hypertable_relid,
									 Oid chunk_relid)
{
	CreateStatsStmt *chunk_stmt;
	List *chunk_exprs;
	List *chunk_stat_ext_types;
	RangeVar *chunk_range_var;
	Datum datum;
	bool isnull;
	int2vector *stxkeys;
	ArrayType *stxkind;

	/* Get stxkeys (column numbers) */
	datum = SysCacheGetAttr(STATEXTOID, stat_ext_tuple, Anum_pg_statistic_ext_stxkeys, &isnull);
	if (isnull)
		return NULL;
	stxkeys = (int2vector *) DatumGetPointer(datum);

	/* Convert stxkeys to chunk exprs */
	chunk_exprs = convert_stxkeys_to_chunk_exprs(stxkeys, hypertable_relid, chunk_relid);
	if (chunk_exprs == NULL)
		return NULL; /* Column doesn't exist in chunk */

	/* Get stxkind (statistics types) */
	datum = SysCacheGetAttr(STATEXTOID, stat_ext_tuple, Anum_pg_statistic_ext_stxkind, &isnull);
	if (isnull)
	{
		list_free_deep(chunk_exprs);
		return NULL;
	}
	stxkind = DatumGetArrayTypeP(datum);

	/*
	 * Convert stxkind to string list.
	 * NIL is valid when only expressions exist (stxkind = {e}).
	 */
	chunk_stat_ext_types = convert_stxkind_to_string_list(stxkind);

	/* Process expression extended statistics if present */
	chunk_exprs = process_chunk_extended_statistics_exprs(stat_ext_tuple,
														  chunk_exprs,
														  hypertable_relid,
														  chunk_relid);

	/* Create CreateStatsStmt node */
	chunk_stmt = makeNode(CreateStatsStmt);

	/* Build qualified name: schema.chunk_table_name_stat_ext_name */
	char *chunk_stat_ext_name =
		chunk_extended_statistics_choose_name(NameStr(chunk->fd.table_name),
											  NameStr(stat_ext_form->stxname),
											  get_rel_namespace(chunk_relid));
	chunk_stmt->defnames =
		list_make2(makeString(NameStr(chunk->fd.schema_name)), makeString(chunk_stat_ext_name));

	/* Build RangeVar for chunk relation */
	chunk_range_var =
		makeRangeVar(NameStr(chunk->fd.schema_name), NameStr(chunk->fd.table_name), 0);

	/* Set up CreateStatsStmt */
	chunk_stmt->exprs = chunk_exprs;
	chunk_stmt->stat_types = chunk_stat_ext_types;
	chunk_stmt->relations = list_make1(chunk_range_var);
	chunk_stmt->if_not_exists = true;

	return chunk_stmt;
}

/*
 * Convert hypertable's stxkeys (column numbers) to chunk's StatsElem list.
 * Gets column names from hypertable and verifies they exist in chunk.
 *
 * Returns: List of StatsElem nodes with column names.
 *          ERROR if any column doesn't exist in chunk.
 */
static List *
convert_stxkeys_to_chunk_exprs(int2vector *stxkeys, Oid hypertable_relid, Oid chunk_relid)
{
	List *chunk_exprs = NIL;
	int i;

	for (i = 0; i < stxkeys->dim1; i++)
	{
		AttrNumber hypertable_attnum = stxkeys->values[i];
		char *column_name;
		AttrNumber chunk_attnum;
		StatsElem *stats_elem;

		/* Get column name from hypertable */
		column_name = get_attname(hypertable_relid, hypertable_attnum, false);
		if (column_name == NULL)
		{
			list_free_deep(chunk_exprs);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column with attribute number %d does not exist in hypertable \"%s\"",
							hypertable_attnum,
							get_rel_name(hypertable_relid))));
		}

		/* Verify column exists in chunk by name */
		chunk_attnum = get_attnum(chunk_relid, column_name);
		if (chunk_attnum == InvalidAttrNumber)
		{
			list_free_deep(chunk_exprs);
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" of hypertable \"%s\" does not exist in chunk \"%s\"",
							column_name,
							get_rel_name(hypertable_relid),
							get_rel_name(chunk_relid))));
		}

		/* Create StatsElem with hypertable's column name */
		stats_elem = makeNode(StatsElem);
		stats_elem->name = pstrdup(column_name);
		stats_elem->expr = NULL;
		chunk_exprs = lappend(chunk_exprs, stats_elem);
	}

	return chunk_exprs;
}

/*
 * Convert stxkind char array to string list.
 * Maps 'd' -> "ndistinct", 'f' -> "dependencies", 'm' -> "mcv".
 * Skips 'e' (expressions) - PostgreSQL adds it automatically when expressions exist.
 *
 * Returns: List of strings. NIL is valid when only expressions exist (stxkind = {e}).
 */
static List *
convert_stxkind_to_string_list(ArrayType *stxkind)
{
	List *chunk_stat_types = NIL;
	char *kind_chars;
	int num_kinds;
	int i;

	/* PostgreSQL guarantees stxkind is a valid 1-dimensional char array */
	Assert(ARR_NDIM(stxkind) == 1);
	Assert(!ARR_HASNULL(stxkind));

	kind_chars = (char *) ARR_DATA_PTR(stxkind);
	num_kinds = ARR_DIMS(stxkind)[0];

	for (i = 0; i < num_kinds; i++)
	{
		char kind_char = kind_chars[i];
		char *kind_str;

		switch (kind_char)
		{
			case STATS_EXT_NDISTINCT:
				kind_str = "ndistinct";
				break;
			case STATS_EXT_DEPENDENCIES:
				kind_str = "dependencies";
				break;
			case STATS_EXT_MCV:
				kind_str = "mcv";
				break;
			case STATS_EXT_EXPRESSIONS:
				/*
				 * Skip 'expressions' kind. PostgreSQL automatically adds it
				 * when CREATE STATISTICS includes expressions. We cannot specify
				 * it explicitly in SQL syntax, so we omit it from stat_types.
				 * PostgreSQL will add it again for the chunk automatically.
				 */
				continue;
			default:
				/*
				 * Skip unknown kinds for forward compatibility with future
				 * PostgreSQL versions that may add new statistics kinds.
				 */
				continue;
		}
		chunk_stat_types = lappend(chunk_stat_types, makeString(kind_str));
	}

	/*
	 * NIL is valid here - it means only expressions exist (stxkind = {e}).
	 * PostgreSQL will handle stat_types = NIL correctly.
	 */
	return chunk_stat_types;
}

/*
 * Process expression extended statistics and append to chunk_exprs.
 * Parses pg_node_tree, maps attribute numbers, and creates StatsElem nodes.
 *
 * Returns: Updated chunk_exprs with expression elements appended,
 *          or original chunk_exprs if no expressions present.
 */
static List *
process_chunk_extended_statistics_exprs(HeapTuple stat_ext_tuple, List *chunk_exprs,
										Oid hypertable_relid, Oid chunk_relid)
{
	Datum datum;
	bool isnull;
	char *expression_string;
	List *expression_nodes;
	List *variable_nodes = NIL;
	ListCell *expression_cell;

	/* Get stxexprs (expressions) */
	datum = SysCacheGetAttr(STATEXTOID, stat_ext_tuple, Anum_pg_statistic_ext_stxexprs, &isnull);
	if (isnull)
		return chunk_exprs; /* No expressions, return as-is */

	/* Convert pg_node_tree to C string and parse to Node tree */
	expression_string = TextDatumGetCString(datum);
	expression_nodes = (List *) stringToNode(expression_string);
	pfree(expression_string);

	/* Extract all Var nodes from the expression list */
	if (expression_nodes != NIL)
		variable_nodes = pull_var_clause((Node *) expression_nodes, 0);

	/* Map attribute numbers from hypertable to chunk */
	foreach (expression_cell, variable_nodes)
	{
		Var *variable_node = lfirst_node(Var, expression_cell);
		/* ts_map_attno() will ERROR if column doesn't exist in chunk */
		variable_node->varattno =
			ts_map_attno(hypertable_relid, chunk_relid, variable_node->varattno);
		variable_node->varattnosyn = variable_node->varattno;
	}

	/* Convert expressions to StatsElem nodes and append to chunk_exprs */
	foreach (expression_cell, expression_nodes)
	{
		Node *expression_node = (Node *) lfirst(expression_cell);
		StatsElem *stats_elem = makeNode(StatsElem);

		stats_elem->name = NULL;			/* NULL for expressions */
		stats_elem->expr = expression_node; /* Expression tree */

		chunk_exprs = lappend(chunk_exprs, stats_elem);
	}

	return chunk_exprs;
}

/*
 * Choose a unique name for chunk extended statistics object.
 * Follows the same pattern as chunk_index_choose_name() in chunk_index.c.
 *
 * Pattern: chunk_table_name_stat_ext_name (e.g., "_hyper_1_1_chunk_stats_test_stat")
 * If the name conflicts, appends a number (e.g., "_hyper_1_1_chunk_stats_test_stat_1")
 */
static char *
chunk_extended_statistics_choose_name(const char *chunk_table_name, const char *stat_ext_name,
									  Oid namespaceid)
{
	char buf[10];
	char *label = NULL;
	char *extended_statistics_name;
	int n = 0;

	for (;;)
	{
		/* makeObjectName will ensure the extended statistics name fits within a NAME type */
		extended_statistics_name = makeObjectName(chunk_table_name, stat_ext_name, label);

		/* Check if this name already exists in the namespace */
		List *names = list_make2(makeString(get_namespace_name(namespaceid)),
								 makeString(extended_statistics_name));
		Oid existing_extended_statistics_oid = get_statistics_object_oid(names, true);
		list_free_deep(names);

		if (!OidIsValid(existing_extended_statistics_oid))
			break;

		/* Found a conflict, so try a new name component */
		pfree(extended_statistics_name);
		snprintf(buf, sizeof(buf), "%d", ++n);
		label = buf;
	}

	return extended_statistics_name;
}

/*
 * Create a specific extended statistics on a chunk by name.
 *
 * This function is called during DDL propagation when a new extended statistics
 * is created on a hypertable and needs to be propagated to all existing chunks.
 */
void
ts_chunk_extended_statistics_create_from_stat(Oid hypertable_relid, Oid chunk_relid,
											  const char *stat_ext_name)
{
	Relation hypertable_relation;
	Relation chunk_relation;
	List *stat_ext_names;
	Oid stat_ext_oid;
	HeapTuple stat_ext_tuple;
	Form_pg_statistic_ext stat_ext_form;
	Chunk *chunk;
	CreateStatsStmt *chunk_stmt;

	/* Open relations */
	hypertable_relation = table_open(hypertable_relid, AccessShareLock);
	chunk_relation = table_open(chunk_relid, ShareLock);

	/* Convert extended statistics name to qualified name list and get OID */
#if PG16_LT
	stat_ext_names = stringToQualifiedNameList(stat_ext_name);
#else
	stat_ext_names = stringToQualifiedNameList(stat_ext_name, NULL);
#endif
	stat_ext_oid = get_statistics_object_oid(stat_ext_names, false);

	/* Get extended statistics metadata from system cache */
	stat_ext_tuple = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(stat_ext_oid));
	if (!HeapTupleIsValid(stat_ext_tuple))
	{
		list_free(stat_ext_names);
		table_close(chunk_relation, NoLock);
		table_close(hypertable_relation, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("extended statistics object \"%s\" does not exist", stat_ext_name)));
	}

	stat_ext_form = (Form_pg_statistic_ext) GETSTRUCT(stat_ext_tuple);

	/* Get chunk structure */
	chunk = ts_chunk_get_by_relid(chunk_relid, true);
	if (chunk == NULL)
	{
		ReleaseSysCache(stat_ext_tuple);
		list_free(stat_ext_names);
		table_close(chunk_relation, NoLock);
		table_close(hypertable_relation, AccessShareLock);
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("chunk with OID %u not found", chunk_relid)));
	}

	/* Build CreateStatsStmt for the chunk */
	chunk_stmt = build_chunk_extended_statistics_stmt(chunk,
													  stat_ext_form,
													  stat_ext_tuple,
													  hypertable_relid,
													  chunk_relid);
	if (chunk_stmt == NULL)
	{
		/* NULL means expression-only extended statistics, silently skip */
		ReleaseSysCache(stat_ext_tuple);
		list_free(stat_ext_names);
		table_close(chunk_relation, NoLock);
		table_close(hypertable_relation, AccessShareLock);
		return;
	}

	/* Create extended statistics on chunk */
	CreateStatistics(chunk_stmt, false);

	/* Cleanup */
	ReleaseSysCache(stat_ext_tuple);
	list_free(stat_ext_names);
	table_close(chunk_relation, NoLock);
	table_close(hypertable_relation, AccessShareLock);
}

/*
 * Find a chunk extended statistics that matches the structure of a hypertable
 * extended statistics.
 *
 * This function searches through all extended statistics on the chunk and returns
 * the OID of the first extended statistics that has the same structure as the
 * hypertable extended statistics.
 *
 * Similar to ts_chunk_index_get_by_hypertable_indexrelid() for indexes.
 *
 * Returns InvalidOid if no matching extended statistics is found.
 */
Oid
ts_chunk_extended_statistics_get_by_hypertable_relid(Oid chunk_relid, Oid hypertable_stat_ext_oid)
{
	ScanKeyData scan_key;
	SysScanDesc extended_statistics_scan;
	HeapTuple extended_statistics_tuple;
	Relation extended_statistics_relation;
	Oid matched_chunk_stat_ext_oid = InvalidOid;

	/* Open pg_statistic_ext catalog for scanning */
	extended_statistics_relation = table_open(StatisticExtRelationId, AccessShareLock);

	/*
	 * Initialize scan key to find all extended statistics on the chunk relation.
	 * We're searching for: stxrelid = chunk_relid
	 */
	ScanKeyInit(&scan_key,
				Anum_pg_statistic_ext_stxrelid,
				BTEqualStrategyNumber,
				F_OIDEQ,
				ObjectIdGetDatum(chunk_relid));

	/*
	 * Begin index scan on pg_statistic_ext using StatisticExtRelidIndexId.
	 * This uses the index on stxrelid for efficient lookup.
	 */
	extended_statistics_scan = systable_beginscan(extended_statistics_relation,
												  StatisticExtRelidIndexId,
												  true,
												  NULL,
												  1,
												  &scan_key);

	/* Iterate through all extended statistics on the chunk */
	while (HeapTupleIsValid(extended_statistics_tuple = systable_getnext(extended_statistics_scan)))
	{
		Form_pg_statistic_ext chunk_stat_ext_form =
			(Form_pg_statistic_ext) GETSTRUCT(extended_statistics_tuple);
		Oid chunk_stat_ext_oid = chunk_stat_ext_form->oid;

		/* Compare structure with hypertable extended statistics */
		if (ts_extended_statistics_compare(chunk_stat_ext_oid, hypertable_stat_ext_oid))
		{
			matched_chunk_stat_ext_oid = chunk_stat_ext_oid;
			break; /* Found first match, stop searching */
		}
	}

	systable_endscan(extended_statistics_scan);
	table_close(extended_statistics_relation, AccessShareLock);

	return matched_chunk_stat_ext_oid;
}

/*
 * Compare two extended statistics to determine if they have the same structure.
 *
 * Two extended statistics are considered structurally identical if they have:
 * - Same stxkeys (column numbers)
 * - Same stxkind (statistics types: d, f, m, e)
 * - Same stxexprs (expressions, if any)
 * - Matching name pattern (chunk statistics name contains hypertable statistics name)
 *
 * This function is similar to ts_indexing_compare() for indexes, but for
 * extended statistics.
 *
 * Returns true if the extended statistics have identical structure, false otherwise.
 */
bool
ts_extended_statistics_compare(Oid stat_ext_oid1, Oid stat_ext_oid2)
{
	HeapTuple stat_ext_tuple1;
	HeapTuple stat_ext_tuple2;
	Form_pg_statistic_ext stat_ext_form1;
	Form_pg_statistic_ext stat_ext_form2;
	bool columns_match;
	bool kinds_match;
	bool expressions_match;
	bool name_pattern_match;

	/* Fetch extended statistics tuples from system cache */
	stat_ext_tuple1 = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(stat_ext_oid1));
	if (!HeapTupleIsValid(stat_ext_tuple1))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("extended statistics with OID %u does not exist", stat_ext_oid1)));

	stat_ext_tuple2 = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(stat_ext_oid2));
	if (!HeapTupleIsValid(stat_ext_tuple2))
	{
		ReleaseSysCache(stat_ext_tuple1);
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("extended statistics with OID %u does not exist", stat_ext_oid2)));
	}

	/* Extract extended statistics form data */
	stat_ext_form1 = (Form_pg_statistic_ext) GETSTRUCT(stat_ext_tuple1);
	stat_ext_form2 = (Form_pg_statistic_ext) GETSTRUCT(stat_ext_tuple2);

	/* Compare columns */
	columns_match = compare_extended_statistics_columns(stat_ext_form1, stat_ext_form2);
	if (!columns_match)
	{
		ReleaseSysCache(stat_ext_tuple1);
		ReleaseSysCache(stat_ext_tuple2);
		return false;
	}

	/* Compare extended statistics kinds */
	kinds_match = compare_extended_statistics_kinds(stat_ext_tuple1, stat_ext_tuple2);
	if (!kinds_match)
	{
		ReleaseSysCache(stat_ext_tuple1);
		ReleaseSysCache(stat_ext_tuple2);
		return false;
	}

	/* Compare expressions */
	expressions_match = compare_extended_statistics_expressions(stat_ext_tuple1, stat_ext_tuple2);
	if (!expressions_match)
	{
		ReleaseSysCache(stat_ext_tuple1);
		ReleaseSysCache(stat_ext_tuple2);
		return false;
	}

	/*
	 * Compare name patterns to ensure chunk statistics matches hypertable statistics.
	 * Chunk statistics are named: {chunk_prefix}_{hypertable_stat_name}[_{number}]
	 * For example:
	 *   Hypertable: multi_drop_stat1
	 *   Chunk:      _hyper_5_12_chunk_multi_drop_stat1
	 *   Chunk (dup): _hyper_5_12_chunk_multi_drop_stat1_1
	 *
	 * We check if stat1 (chunk) name contains "_{stat2_name}" pattern,
	 * optionally followed by "_digits" suffix.
	 */
	name_pattern_match = compare_extended_statistics_name_pattern(NameStr(stat_ext_form1->stxname),
																  NameStr(stat_ext_form2->stxname));
	if (!name_pattern_match)
	{
		ReleaseSysCache(stat_ext_tuple1);
		ReleaseSysCache(stat_ext_tuple2);
		return false;
	}

	/* All checks passed - extended statistics are structurally identical */
	ReleaseSysCache(stat_ext_tuple1);
	ReleaseSysCache(stat_ext_tuple2);
	return true;
}

/*
 * Compare stxkeys (column numbers) of two extended statistics.
 *
 * Returns true if both extended statistics target the same columns in the same order.
 */
static bool
compare_extended_statistics_columns(Form_pg_statistic_ext stat_ext_form1,
									Form_pg_statistic_ext stat_ext_form2)
{
	int num_columns_stat_ext1;
	int num_columns_stat_ext2;

	num_columns_stat_ext1 = stat_ext_form1->stxkeys.dim1;
	num_columns_stat_ext2 = stat_ext_form2->stxkeys.dim1;

	/* Check if both extended statistics have the same number of columns */
	if (num_columns_stat_ext1 != num_columns_stat_ext2)
		return false;

	/* Compare each column number */
	for (int i = 0; i < num_columns_stat_ext1; i++)
	{
		if (stat_ext_form1->stxkeys.values[i] != stat_ext_form2->stxkeys.values[i])
			return false;
	}

	return true;
}

/*
 * Compare stxkind (statistics types) of two extended statistics.
 *
 * stxkind is an array of char values like {'d', 'f', 'm', 'e'} representing:
 * - 'd': ndistinct
 * - 'f': dependencies
 * - 'm': mcv (most common values)
 * - 'e': expressions
 *
 * Returns true if both extended statistics have the same types in the same order.
 */
static bool
compare_extended_statistics_kinds(HeapTuple stat_ext_tuple1, HeapTuple stat_ext_tuple2)
{
	Datum stxkind_datum_stat_ext1;
	Datum stxkind_datum_stat_ext2;
	bool stxkind_isnull_stat_ext1;
	bool stxkind_isnull_stat_ext2;
	ArrayType *stxkind_array_stat_ext1;
	ArrayType *stxkind_array_stat_ext2;
	int num_kinds_stat_ext1;
	int num_kinds_stat_ext2;
	char *stxkind_data_stat_ext1;
	char *stxkind_data_stat_ext2;

	/* Fetch stxkind attribute from both extended statistics tuples */
	stxkind_datum_stat_ext1 = SysCacheGetAttr(STATEXTOID,
											  stat_ext_tuple1,
											  Anum_pg_statistic_ext_stxkind,
											  &stxkind_isnull_stat_ext1);
	stxkind_datum_stat_ext2 = SysCacheGetAttr(STATEXTOID,
											  stat_ext_tuple2,
											  Anum_pg_statistic_ext_stxkind,
											  &stxkind_isnull_stat_ext2);

	/* Check if both are NULL or both are NOT NULL */
	if (stxkind_isnull_stat_ext1 != stxkind_isnull_stat_ext2)
		return false;

	/* If both are NULL, they match */
	if (stxkind_isnull_stat_ext1)
		return true;

	/* Both have stxkind, compare the arrays */
	stxkind_array_stat_ext1 = DatumGetArrayTypeP(stxkind_datum_stat_ext1);
	stxkind_array_stat_ext2 = DatumGetArrayTypeP(stxkind_datum_stat_ext2);

	num_kinds_stat_ext1 = ARR_DIMS(stxkind_array_stat_ext1)[0];
	num_kinds_stat_ext2 = ARR_DIMS(stxkind_array_stat_ext2)[0];

	/* Check if both have the same number of kinds */
	if (num_kinds_stat_ext1 != num_kinds_stat_ext2)
		return false;

	/* Compare each kind character */
	stxkind_data_stat_ext1 = ARR_DATA_PTR(stxkind_array_stat_ext1);
	stxkind_data_stat_ext2 = ARR_DATA_PTR(stxkind_array_stat_ext2);

	for (int i = 0; i < num_kinds_stat_ext1; i++)
	{
		if (stxkind_data_stat_ext1[i] != stxkind_data_stat_ext2[i])
			return false;
	}

	return true;
}

/*
 * Compare stxexprs (expression trees) of two extended statistics.
 *
 * For expression extended statistics like:
 *   CREATE STATISTICS stat ON (a + b), (c * 2) FROM table;
 *
 * Returns true if both extended statistics have identical expression trees.
 */
static bool
compare_extended_statistics_expressions(HeapTuple stat_ext_tuple1, HeapTuple stat_ext_tuple2)
{
	Datum stxexprs_datum_stat_ext1;
	Datum stxexprs_datum_stat_ext2;
	bool stxexprs_isnull_stat_ext1;
	bool stxexprs_isnull_stat_ext2;
	List *expression_list_stat_ext1;
	List *expression_list_stat_ext2;

	/* Fetch stxexprs attribute from both extended statistics tuples */
	stxexprs_datum_stat_ext1 = SysCacheGetAttr(STATEXTOID,
											   stat_ext_tuple1,
											   Anum_pg_statistic_ext_stxexprs,
											   &stxexprs_isnull_stat_ext1);
	stxexprs_datum_stat_ext2 = SysCacheGetAttr(STATEXTOID,
											   stat_ext_tuple2,
											   Anum_pg_statistic_ext_stxexprs,
											   &stxexprs_isnull_stat_ext2);

	/* Check if both are NULL or both are NOT NULL */
	if (stxexprs_isnull_stat_ext1 != stxexprs_isnull_stat_ext2)
		return false;

	/* If both are NULL, they match */
	if (stxexprs_isnull_stat_ext1)
		return true;

	/*
	 * Parse and compare expression trees in a short-lived context so that
	 * the allocated node trees are freed when we're done, avoiding transaction
	 * memory buildup when comparing many extended statistics.
	 */
	{
		MemoryContext old_ctx;
		MemoryContext expr_ctx;
		bool result;

		expr_ctx = AllocSetContextCreate(CurrentMemoryContext,
										 "compare extended statistics expressions",
										 ALLOCSET_SMALL_SIZES);
		old_ctx = MemoryContextSwitchTo(expr_ctx);

		expression_list_stat_ext1 =
			(List *) stringToNode(TextDatumGetCString(stxexprs_datum_stat_ext1));
		expression_list_stat_ext2 =
			(List *) stringToNode(TextDatumGetCString(stxexprs_datum_stat_ext2));

		result = equal(expression_list_stat_ext1, expression_list_stat_ext2);

		MemoryContextSwitchTo(old_ctx);
		MemoryContextDelete(expr_ctx);

		return result;
	}
}

/*
 * Compare name patterns to determine if chunk statistics matches hypertable statistics.
 *
 * Chunk statistics are named following the pattern:
 *   {chunk_table_name}_{hypertable_stat_name}[_{number}]
 *
 * Examples:
 *   Hypertable stat: multi_drop_stat1
 *   Chunk stat:      _hyper_5_12_chunk_multi_drop_stat1
 *   Chunk stat (dup): _hyper_5_12_chunk_multi_drop_stat1_1
 *
 * This function checks if chunk_stat_name contains "_{hypertable_stat_name}"
 * pattern, optionally followed by "_{digits}" suffix for duplicate resolution.
 *
 * Returns true if the name pattern matches, false otherwise.
 */
static bool
compare_extended_statistics_name_pattern(const char *chunk_stat_name,
										 const char *hypertable_stat_name)
{
	const char *pattern_pos;
	const char *suffix_pos;
	size_t chunk_name_len;
	size_t hypertable_name_len;

	if (chunk_stat_name == NULL || hypertable_stat_name == NULL)
		return false;

	chunk_name_len = strlen(chunk_stat_name);
	hypertable_name_len = strlen(hypertable_stat_name);

	/* Chunk name must be longer than hypertable name (has prefix) */
	if (chunk_name_len <= hypertable_name_len)
		return false;

	/* Find hypertable_stat_name within chunk_stat_name */
	pattern_pos = strstr(chunk_stat_name, hypertable_stat_name);
	if (pattern_pos == NULL)
		return false;

	/* Check if hypertable_stat_name is preceded by underscore */
	if (pattern_pos == chunk_stat_name || *(pattern_pos - 1) != '_')
		return false;

	/* Check what comes after hypertable_stat_name */
	suffix_pos = pattern_pos + hypertable_name_len;

	/* If it's the end of string, it's a perfect match */
	if (*suffix_pos == '\0')
		return true;

	/* If not the end, it must be "_digits" pattern (duplicate resolution) */
	if (*suffix_pos != '_')
		return false;

	/* Skip the underscore */
	suffix_pos++;

	/* Verify that the rest is all digits */
	while (*suffix_pos != '\0')
	{
		if (!isdigit((unsigned char) *suffix_pos))
			return false;
		suffix_pos++;
	}

	return true;
}

/*
 * Get the relation OID that an extended statistics object belongs to.
 *
 * Given an extended statistics OID, returns the OID of the table that the
 * extended statistics object is defined on (stxrelid from pg_statistic_ext).
 *
 * Returns InvalidOid if the extended statistics object doesn't exist.
 */
Oid
ts_get_relation_from_extended_statistics(Oid stat_ext_oid)
{
	HeapTuple stat_ext_tuple;
	Form_pg_statistic_ext stat_ext_form;
	Oid relid;

	/* Fetch extended statistics tuple from system cache */
	stat_ext_tuple = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(stat_ext_oid));
	if (!HeapTupleIsValid(stat_ext_tuple))
		return InvalidOid;

	stat_ext_form = (Form_pg_statistic_ext) GETSTRUCT(stat_ext_tuple);
	relid = stat_ext_form->stxrelid;

	ReleaseSysCache(stat_ext_tuple);

	return relid;
}

/*
 * Rename chunk extended statistics to match the new hypertable statistics name.
 *
 * This function is called when a hypertable extended statistics is renamed.
 * It finds the corresponding chunk extended statistics and renames them to
 * maintain naming consistency.
 *
 * Similar to ts_chunk_index_rename() for indexes.
 */
void
ts_chunk_extended_statistics_rename(Hypertable *ht, Oid hypertable_stat_oid, const char *new_name)
{
	List *chunks = ts_chunk_get_by_hypertable_id(ht->fd.id);
	ListCell *lc;

	foreach (lc, chunks)
	{
		Chunk *chunk = lfirst(lc);
		if (!OidIsValid(chunk->table_id))
			continue;

		/* Find matching extended statistics on chunk using structural comparison */
		Oid chunk_stat_oid =
			ts_chunk_extended_statistics_get_by_hypertable_relid(chunk->table_id,
																 hypertable_stat_oid);

		if (OidIsValid(chunk_stat_oid))
		{
			Oid chunk_schemaoid = get_namespace_oid(NameStr(chunk->fd.schema_name), false);
			HeapTuple old_tuple;
			HeapTuple new_tuple;
			Relation catalog;
			Datum values[Natts_pg_statistic_ext];
			bool isnull[Natts_pg_statistic_ext];
			bool replace[Natts_pg_statistic_ext] = { false };
			NameData name_data;

			/* Generate new name: {chunk_table_name}_{new_name} */
			const char *chunk_new_name =
				chunk_extended_statistics_choose_name(NameStr(chunk->fd.table_name),
													  new_name,
													  chunk_schemaoid);

			/* Open pg_statistic_ext catalog */
			catalog = table_open(StatisticExtRelationId, RowExclusiveLock);

			/* Fetch the old tuple */
			old_tuple = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(chunk_stat_oid));
			if (!HeapTupleIsValid(old_tuple))
				elog(ERROR, "cache lookup failed for statistics object %u", chunk_stat_oid);

			/* Deform the tuple */
			heap_deform_tuple(old_tuple, RelationGetDescr(catalog), values, isnull);

			/* Replace the name */
			namestrcpy(&name_data, chunk_new_name);
			values[AttrNumberGetAttrOffset(Anum_pg_statistic_ext_stxname)] =
				NameGetDatum(&name_data);
			replace[AttrNumberGetAttrOffset(Anum_pg_statistic_ext_stxname)] = true;

			/* Create new tuple and update catalog */
			new_tuple =
				heap_modify_tuple(old_tuple, RelationGetDescr(catalog), values, isnull, replace);
			CatalogTupleUpdate(catalog, &new_tuple->t_self, new_tuple);

			/* Cleanup */
			heap_freetuple(new_tuple);
			ReleaseSysCache(old_tuple);
			table_close(catalog, RowExclusiveLock);

			/* Invalidate cache */
			CommandCounterIncrement();
		}
	}
}
