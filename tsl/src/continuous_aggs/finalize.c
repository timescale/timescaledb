/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains the code related to the *NOT* finalized version of
 * Continuous Aggregates (with partials)
 */
#include "finalize.h"
#include "create.h"
#include "common.h"

typedef struct CAggHavingCxt
{
	List *origq_tlist;
	List *finalizeq_tlist;
	AggPartCxt agg_cxt;
} CAggHavingCxt;

/* Static function prototypes */
static Datum get_input_types_array_datum(Aggref *original_aggregate);
static Aggref *add_partialize_column(Aggref *agg_to_partialize, AggPartCxt *cxt);
static void set_var_mapping(Var *orig_var, Var *mapped_var, AggPartCxt *cxt);
static Var *var_already_mapped(Var *var, AggPartCxt *cxt);
static Node *create_replace_having_qual_mutator(Node *node, CAggHavingCxt *cxt);
static Node *finalizequery_create_havingqual(FinalizeQueryInfo *inp,
											 MatTableColumnInfo *mattblinfo);
static Var *mattablecolumninfo_addentry(MatTableColumnInfo *out, Node *input,
										int original_query_resno, bool finalized,
										bool *skip_adding);
static FuncExpr *get_partialize_funcexpr(Aggref *agg);
static inline void makeMaterializeColumnName(char *colbuf, const char *type,
											 int original_query_resno, int colno);

static inline void
makeMaterializeColumnName(char *colbuf, const char *type, int original_query_resno, int colno)
{
	int ret = snprintf(colbuf, NAMEDATALEN, "%s_%d_%d", type, original_query_resno, colno);
	if (ret < 0 || ret >= NAMEDATALEN)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR), errmsg("bad materialization table column name")));
}

/*
 * Creates a partialize expr for the passed in agg:
 * partialize_agg(agg).
 */
static FuncExpr *
get_partialize_funcexpr(Aggref *agg)
{
	FuncExpr *partialize_fnexpr;
	Oid partfnoid, partargtype;
	partargtype = ANYELEMENTOID;
	partfnoid =
		LookupFuncName(list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(TS_PARTIALFN)),
					   1,
					   &partargtype,
					   false);
	partialize_fnexpr = makeFuncExpr(partfnoid,
									 BYTEAOID,
									 list_make1(agg), /*args*/
									 InvalidOid,
									 InvalidOid,
									 COERCE_EXPLICIT_CALL);
	return partialize_fnexpr;
}

/*
 * Build a [N][2] array where N is number of arguments
 * and the inner array is of [schema_name,type_name].
 */
static Datum
get_input_types_array_datum(Aggref *original_aggregate)
{
	ListCell *lc;
	MemoryContext builder_context =
		AllocSetContextCreate(CurrentMemoryContext, "input types builder", ALLOCSET_DEFAULT_SIZES);
	Oid name_array_type_oid = get_array_type(NAMEOID);
	ArrayBuildStateArr *outer_builder =
		initArrayResultArr(name_array_type_oid, NAMEOID, builder_context, false);
	Datum result;

	foreach (lc, original_aggregate->args)
	{
		TargetEntry *te = lfirst(lc);
		Oid type_oid = exprType((Node *) te->expr);
		ArrayBuildState *schema_name_builder = initArrayResult(NAMEOID, builder_context, false);
		HeapTuple tp;
		Form_pg_type typtup;
		char *schema_name;
		Name type_name = (Name) palloc0(NAMEDATALEN);
		Datum schema_datum;
		Datum type_name_datum;
		Datum inner_array_datum;

		tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type_oid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for type %u", type_oid);

		typtup = (Form_pg_type) GETSTRUCT(tp);
		namestrcpy(type_name, NameStr(typtup->typname));
		schema_name = get_namespace_name(typtup->typnamespace);
		ReleaseSysCache(tp);

		type_name_datum = NameGetDatum(type_name);
		/* Using name in because creating from a char * (that may be null or too long). */
		schema_datum = DirectFunctionCall1(namein, CStringGetDatum(schema_name));

		accumArrayResult(schema_name_builder, schema_datum, false, NAMEOID, builder_context);
		accumArrayResult(schema_name_builder, type_name_datum, false, NAMEOID, builder_context);

		inner_array_datum = makeArrayResult(schema_name_builder, CurrentMemoryContext);

		accumArrayResultArr(outer_builder,
							inner_array_datum,
							false,
							name_array_type_oid,
							builder_context);
	}
	result = makeArrayResultArr(outer_builder, CurrentMemoryContext, false);

	MemoryContextDelete(builder_context);
	return result;
}

static Aggref *
add_partialize_column(Aggref *agg_to_partialize, AggPartCxt *cxt)
{
	Aggref *newagg;
	Var *var;
	bool skip_adding;

	/*
	 * Step 1: create partialize( aggref) column
	 * for materialization table.
	 */
	var = mattablecolumninfo_addentry(cxt->mattblinfo,
									  (Node *) agg_to_partialize,
									  cxt->original_query_resno,
									  false,
									  &skip_adding);
	cxt->added_aggref_col = true;
	/*
	 * Step 2: create finalize_agg expr using var
	 * for the column added to the materialization table.
	 */

	/* This is a var for the column we created. */
	newagg = get_finalize_aggref(agg_to_partialize, var);
	return newagg;
}

static void
set_var_mapping(Var *orig_var, Var *mapped_var, AggPartCxt *cxt)
{
	cxt->orig_vars = lappend(cxt->orig_vars, orig_var);
	cxt->mapped_vars = lappend(cxt->mapped_vars, mapped_var);
}

/*
 * Checks whether var has already been mapped and returns the
 * corresponding column of the materialization table.
 */
static Var *
var_already_mapped(Var *var, AggPartCxt *cxt)
{
	ListCell *lc_old, *lc_new;

	forboth (lc_old, cxt->orig_vars, lc_new, cxt->mapped_vars)
	{
		Var *orig_var = (Var *) lfirst_node(Var, lc_old);
		Var *mapped_var = (Var *) lfirst_node(Var, lc_new);

		/* There should be no subqueries so varlevelsup should not be a problem here. */
		if (var->varno == orig_var->varno && var->varattno == orig_var->varattno)
			return mapped_var;
	}
	return NULL;
}

/*
 * Add ts_internal_cagg_final to bytea column.
 * bytea column is the internal state for an agg. Pass info for the agg as "inp".
 * inpcol = bytea column.
 * This function returns an aggref
 * ts_internal_cagg_final( Oid, Oid, bytea, NULL::output_typeid)
 * the arguments are a list of targetentry
 */
Oid
get_finalize_function_oid(void)
{
	Oid finalfnoid;
	Oid finalfnargtypes[] = { TEXTOID,	NAMEOID,	  NAMEOID, get_array_type(NAMEOID),
							  BYTEAOID, ANYELEMENTOID };
	List *funcname = list_make2(makeString(INTERNAL_SCHEMA_NAME), makeString(FINALFN));
	int nargs = sizeof(finalfnargtypes) / sizeof(finalfnargtypes[0]);
	finalfnoid = LookupFuncName(funcname, nargs, finalfnargtypes, false);
	return finalfnoid;
}

/*
 * Creates an aggref of the form:
 * finalize-agg(
 *                "sum(int)" TEXT,
 *                collation_schema_name NAME, collation_name NAME,
 *                input_types_array NAME[N][2],
 *                <partial-column-name> BYTEA,
 *                null::<return-type of sum(int)>
 *             )
 * here sum(int) is the input aggregate "inp" in the parameter-list.
 */
Aggref *
get_finalize_aggref(Aggref *inp, Var *partial_state_var)
{
	Aggref *aggref;
	TargetEntry *te;
	char *aggregate_signature;
	Const *aggregate_signature_const, *collation_schema_const, *collation_name_const,
		*input_types_const, *return_type_const;
	Oid name_array_type_oid = get_array_type(NAMEOID);
	Var *partial_bytea_var;
	List *tlist = NIL;
	int tlist_attno = 1;
	List *argtypes = NIL;
	char *collation_name = NULL, *collation_schema_name = NULL;
	Datum collation_name_datum = (Datum) 0;
	Datum collation_schema_datum = (Datum) 0;
	Oid finalfnoid = get_finalize_function_oid();

	argtypes = list_make5_oid(TEXTOID, NAMEOID, NAMEOID, name_array_type_oid, BYTEAOID);
	argtypes = lappend_oid(argtypes, inp->aggtype);

	aggref = makeNode(Aggref);
	aggref->aggfnoid = finalfnoid;
	aggref->aggtype = inp->aggtype;
	aggref->aggcollid = inp->aggcollid;
	aggref->inputcollid = inp->inputcollid;
	aggref->aggtranstype = InvalidOid; /* will be set by planner */
	aggref->aggargtypes = argtypes;
	aggref->aggdirectargs = NULL; /*relevant for hypothetical set aggs*/
	aggref->aggorder = NULL;
	aggref->aggdistinct = NULL;
	aggref->aggfilter = NULL;
	aggref->aggstar = false;
	aggref->aggvariadic = false;
	aggref->aggkind = AGGKIND_NORMAL;
	aggref->aggsplit = AGGSPLIT_SIMPLE;
	aggref->location = -1;
	/* Construct the arguments. */
	aggregate_signature = format_procedure_qualified(inp->aggfnoid);
	aggregate_signature_const = makeConst(TEXTOID,
										  -1,
										  DEFAULT_COLLATION_OID,
										  -1,
										  CStringGetTextDatum(aggregate_signature),
										  false,
										  false /* passbyval */
	);
	te = makeTargetEntry((Expr *) aggregate_signature_const, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	if (OidIsValid(inp->inputcollid))
	{
		/* Similar to generate_collation_name. */
		HeapTuple tp;
		Form_pg_collation colltup;
		tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(inp->inputcollid));
		if (!HeapTupleIsValid(tp))
			elog(ERROR, "cache lookup failed for collation %u", inp->inputcollid);
		colltup = (Form_pg_collation) GETSTRUCT(tp);
		collation_name = pstrdup(NameStr(colltup->collname));
		collation_name_datum = DirectFunctionCall1(namein, CStringGetDatum(collation_name));

		collation_schema_name = get_namespace_name(colltup->collnamespace);
		if (collation_schema_name != NULL)
			collation_schema_datum =
				DirectFunctionCall1(namein, CStringGetDatum(collation_schema_name));
		ReleaseSysCache(tp);
	}
	collation_schema_const = makeConst(NAMEOID,
									   -1,
									   InvalidOid,
									   NAMEDATALEN,
									   collation_schema_datum,
									   (collation_schema_name == NULL) ? true : false,
									   false /* passbyval */
	);
	te = makeTargetEntry((Expr *) collation_schema_const, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	collation_name_const = makeConst(NAMEOID,
									 -1,
									 InvalidOid,
									 NAMEDATALEN,
									 collation_name_datum,
									 (collation_name == NULL) ? true : false,
									 false /* passbyval */
	);
	te = makeTargetEntry((Expr *) collation_name_const, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	input_types_const = makeConst(get_array_type(NAMEOID),
								  -1,
								  InvalidOid,
								  -1,
								  get_input_types_array_datum(inp),
								  false,
								  false /* passbyval */
	);
	te = makeTargetEntry((Expr *) input_types_const, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	partial_bytea_var = copyObject(partial_state_var);
	te = makeTargetEntry((Expr *) partial_bytea_var, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	return_type_const = makeNullConst(inp->aggtype, -1, inp->aggcollid);
	te = makeTargetEntry((Expr *) return_type_const, tlist_attno++, NULL, false);
	tlist = lappend(tlist, te);

	Assert(tlist_attno == 7);
	aggref->args = tlist;
	return aggref;
}

Node *
add_var_mutator(Node *node, AggPartCxt *cxt)
{
	if (node == NULL)
		return NULL;
	if (IsA(node, Aggref))
	{
		return node; /* don't process this further */
	}
	if (IsA(node, Var))
	{
		Var *orig_var, *mapped_var;
		bool skip_adding = false;

		mapped_var = var_already_mapped((Var *) node, cxt);
		/* Avoid duplicating columns in the materialization table. */
		if (mapped_var)
			/*
			 * There should be no subquery so mapped_var->varlevelsup
			 * should not be a problem here.
			 */
			return (Node *) copyObject(mapped_var);

		orig_var = (Var *) node;
		mapped_var = mattablecolumninfo_addentry(cxt->mattblinfo,
												 node,
												 cxt->original_query_resno,
												 false,
												 &skip_adding);
		set_var_mapping(orig_var, mapped_var, cxt);
		return (Node *) mapped_var;
	}
	return expression_tree_mutator(node, add_var_mutator, cxt);
}

/*
 * This function modifies the passed in havingQual by mapping exprs to
 * columns in materialization table or finalized aggregate form.
 * Note that HAVING clause can contain only exprs from group-by or aggregates
 * and GROUP BY clauses cannot be aggregates.
 * (By the time we process havingQuals, all the group by exprs have been
 * processed and have associated columns in the materialization hypertable).
 * Example, if  the original query has
 * GROUP BY  colA + colB, colC
 *   HAVING colA + colB + sum(colD) > 10 OR count(colE) = 10
 *
 * The transformed havingqual would be
 * HAVING   matCol3 + finalize_agg( sum(matCol4) > 10
 *       OR finalize_agg( count(matCol5)) = 10
 *
 *
 * Note: GROUP BY exprs always appear in the query's targetlist.
 * Some of the aggregates from the havingQual  might also already appear in the targetlist.
 * We replace all existing entries with their corresponding entry from the modified targetlist.
 * If an aggregate (in the havingqual) does not exist in the TL, we create a
 *  materialization table column for it and use the finalize(column) form in the
 * transformed havingQual.
 */
static Node *
create_replace_having_qual_mutator(Node *node, CAggHavingCxt *cxt)
{
	if (node == NULL)
		return NULL;
	/*
	 * See if we already have a column in materialization hypertable for this
	 * expr. We do this by checking the existing targetlist
	 * entries for the query.
	 */
	ListCell *lc, *lc2;
	List *origtlist = cxt->origq_tlist;
	List *modtlist = cxt->finalizeq_tlist;
	forboth (lc, origtlist, lc2, modtlist)
	{
		TargetEntry *te = (TargetEntry *) lfirst(lc);
		TargetEntry *modte = (TargetEntry *) lfirst(lc2);
		if (equal(node, te->expr))
		{
			return (Node *) modte->expr;
		}
	}
	/*
	 * Didn't find a match in targetlist. If it is an aggregate,
	 * create a partialize column for it in materialization hypertable
	 * and return corresponding finalize expr.
	 */
	if (IsA(node, Aggref))
	{
		AggPartCxt *agg_cxt = &(cxt->agg_cxt);
		agg_cxt->added_aggref_col = false;
		Aggref *newagg = add_partialize_column((Aggref *) node, agg_cxt);
		Assert(agg_cxt->added_aggref_col == true);
		return (Node *) newagg;
	}
	return expression_tree_mutator(node, create_replace_having_qual_mutator, cxt);
}

static Node *
finalizequery_create_havingqual(FinalizeQueryInfo *inp, MatTableColumnInfo *mattblinfo)
{
	Query *orig_query = inp->final_userquery;
	if (orig_query->havingQual == NULL)
		return NULL;
	Node *havingQual = copyObject(orig_query->havingQual);
	Assert(inp->final_seltlist != NULL);
	CAggHavingCxt hcxt = { .origq_tlist = orig_query->targetList,
						   .finalizeq_tlist = inp->final_seltlist,
						   .agg_cxt.mattblinfo = mattblinfo,
						   .agg_cxt.original_query_resno = 0,
						   .agg_cxt.ignore_aggoid = get_finalize_function_oid(),
						   .agg_cxt.added_aggref_col = false,
						   .agg_cxt.var_outside_of_aggref = false,
						   .agg_cxt.orig_vars = NIL,
						   .agg_cxt.mapped_vars = NIL };
	return create_replace_having_qual_mutator(havingQual, &hcxt);
}

Node *
add_aggregate_partialize_mutator(Node *node, AggPartCxt *cxt)
{
	if (node == NULL)
		return NULL;
	/*
	 * Modify the aggref and create a partialize(aggref) expr
	 * for the materialization.
	 * Add a corresponding  columndef for the mat table.
	 * Replace the aggref with the ts_internal_cagg_final fn.
	 * using a Var for the corresponding column in the mat table.
	 * All new Vars have varno = 1 (for RTE 1).
	 */
	if (IsA(node, Aggref))
	{
		if (cxt->ignore_aggoid == ((Aggref *) node)->aggfnoid)
			return node; /* don't process this further */

		Aggref *newagg = add_partialize_column((Aggref *) node, cxt);
		return (Node *) newagg;
	}
	if (IsA(node, Var))
	{
		cxt->var_outside_of_aggref = true;
	}
	return expression_tree_mutator(node, add_aggregate_partialize_mutator, cxt);
}

/*
 * Init the finalize query data structure.
 * Parameters:
 * orig_query - the original query from user view that is being used as template for the finalize
 * query tlist_aliases - aliases for the view select list materialization table columns are created
 * . This will be returned in  the mattblinfo
 *
 * DO NOT modify orig_query. Make a copy if needed.
 * SIDE_EFFECT: the data structure in mattblinfo is modified as a side effect by adding new
 * materialize table columns and partialize exprs.
 */
void
finalizequery_init(FinalizeQueryInfo *inp, Query *orig_query, MatTableColumnInfo *mattblinfo)
{
	AggPartCxt cxt;
	ListCell *lc;
	int resno = 1;

	inp->final_userquery = copyObject(orig_query);
	inp->final_seltlist = NIL;
	inp->final_havingqual = NULL;

	/* Set up the final_seltlist and final_havingqual entries */
	cxt.mattblinfo = mattblinfo;
	cxt.ignore_aggoid = InvalidOid;

	/* Set up the left over variable mapping lists */
	cxt.orig_vars = NIL;
	cxt.mapped_vars = NIL;

	/*
	 * We want all the entries in the targetlist (resjunk or not)
	 * in the materialization  table definition so we include group-by/having clause etc.
	 * We have to do 3 things here:
	 * 1) create a column for mat table
	 * 2) partialize_expr to populate it, and
	 * 3) modify the target entry to be a finalize_expr
	 *    that selects from the materialization table.
	 */
	foreach (lc, orig_query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		TargetEntry *modte = copyObject(tle);
		cxt.added_aggref_col = false;
		cxt.var_outside_of_aggref = false;
		cxt.original_query_resno = resno;

		if (!inp->finalized)
		{
			/*
			 * If tle has aggrefs, get the corresponding
			 * finalize_agg expression and save it in modte.
			 * Also add correspong materialization table column info
			 * for the aggrefs in tle.
			 */
			modte = (TargetEntry *) expression_tree_mutator((Node *) modte,
															add_aggregate_partialize_mutator,
															&cxt);
		}

		/*
		 * We need columns for non-aggregate targets.
		 * If it is not a resjunk OR appears in the grouping clause.
		 */
		if (cxt.added_aggref_col == false && (tle->resjunk == false || tle->ressortgroupref > 0))
		{
			Var *var;
			bool skip_adding = false;
			var = mattablecolumninfo_addentry(cxt.mattblinfo,
											  (Node *) tle,
											  cxt.original_query_resno,
											  inp->finalized,
											  &skip_adding);

			/* Skip adding this column for finalized form. */
			if (skip_adding)
			{
				continue;
			}

			/* Fix the expression for the target entry. */
			modte->expr = (Expr *) var;
		}
		/* Check for left over variables (Var) of targets that contain Aggref. */
		if (cxt.added_aggref_col && cxt.var_outside_of_aggref && !inp->finalized)
		{
			modte = (TargetEntry *) expression_tree_mutator((Node *) modte, add_var_mutator, &cxt);
		}
		/*
		 * Construct the targetlist for the query on the
		 * materialization table. The TL maps 1-1 with the original query:
		 * e.g select a, min(b)+max(d) from foo group by a,timebucket(a);
		 * becomes
		 * select <a-col>,
		 * ts_internal_cagg_final(..b-col ) + ts_internal_cagg_final(..d-col)
		 * from mattbl
		 * group by a-col, timebucket(a-col)
		 */

		/*
		 * We copy the modte target entries, resnos should be the same for
		 * final_selquery and origquery. So tleSortGroupReffor the targetentry
		 * can be reused, only table info needs to be modified.
		 */
		Assert((!inp->finalized && modte->resno == resno) ||
			   (inp->finalized && modte->resno >= resno));
		resno++;
		if (IsA(modte->expr, Var))
		{
			modte->resorigcol = ((Var *) modte->expr)->varattno;
		}
		inp->final_seltlist = lappend(inp->final_seltlist, modte);
	}
	/*
	 * All grouping clause elements are in targetlist already.
	 * So let's check the having clause.
	 */
	if (!inp->finalized)
		inp->final_havingqual = finalizequery_create_havingqual(inp, mattblinfo);
}

/*
 * Create select query with the finalize aggregates
 * for the materialization table.
 * matcollist - column list for mat table
 * mattbladdress - materialization table ObjectAddress
 * This is the function responsible for creating the final
 * structures for selecting from the materialized hypertable
 * created for the Cagg which is
 * select * from _timescaldeb_internal._materialized_hypertable_<xxx>
 */
Query *
finalizequery_get_select_query(FinalizeQueryInfo *inp, List *matcollist,
							   ObjectAddress *mattbladdress, char *relname)
{
	Query *final_selquery = NULL;
	ListCell *lc;
	FromExpr *fromexpr;
	RangeTblEntry *rte;

	/*
	 * For initial cagg creation rtable will have only 1 entry,
	 * for alter table rtable will have multiple entries with our
	 * RangeTblEntry as last member.
	 * For cagg with joins, we need to create a new RTE and jointree
	 * which contains the information of the materialised hypertable
	 * that is created for this cagg.
	 */
	if (list_length(inp->final_userquery->jointree->fromlist) >=
			CONTINUOUS_AGG_MAX_JOIN_RELATIONS ||
		!IsA(linitial(inp->final_userquery->jointree->fromlist), RangeTblRef))
	{
		rte = makeNode(RangeTblEntry);
		rte->alias = makeAlias(relname, NIL);
		rte->inFromCl = true;
		rte->inh = true;
		rte->rellockmode = 1;
		rte->eref = copyObject(rte->alias);
		ListCell *l;
		foreach (l, inp->final_userquery->jointree->fromlist)
		{
			/*
			 * In case of joins, update the rte with all the join related struct.
			 */
			Node *jtnode = (Node *) lfirst(l);
			JoinExpr *join = NULL;
			if (IsA(jtnode, JoinExpr))
			{
				join = castNode(JoinExpr, jtnode);
				RangeTblEntry *jrte = rt_fetch(join->rtindex, inp->final_userquery->rtable);
				rte->joinaliasvars = jrte->joinaliasvars;
				rte->jointype = jrte->jointype;
#if PG13_GE
				rte->joinleftcols = jrte->joinleftcols;
				rte->joinrightcols = jrte->joinrightcols;
				rte->joinmergedcols = jrte->joinmergedcols;
#endif
#if PG14_GE
				rte->join_using_alias = jrte->join_using_alias;
#endif
				rte->selectedCols = jrte->selectedCols;
			}
		}
	}
	else
	{
		rte = llast_node(RangeTblEntry, inp->final_userquery->rtable);
		rte->eref->colnames = NIL;
		rte->selectedCols = NULL;
	}
	if (rte->eref->colnames == NIL)
	{
		/*
		 * We only need to do this for the case when there is no Join node in the query.
		 * In the case of join, rte->eref is already populated by jrte->eref and hence the
		 * relevant info, so need not to do this.
		 */

		/* Aliases for column names for the materialization table. */
		foreach (lc, matcollist)
		{
			ColumnDef *cdef = lfirst_node(ColumnDef, lc);
			rte->eref->colnames = lappend(rte->eref->colnames, makeString(cdef->colname));
			rte->selectedCols = bms_add_member(rte->selectedCols,
											   list_length(rte->eref->colnames) -
												   FirstLowInvalidHeapAttributeNumber);
		}
	}
	rte->relid = mattbladdress->objectId;
	rte->rtekind = RTE_RELATION;
	rte->relkind = RELKIND_RELATION;
	rte->tablesample = NULL;
	rte->requiredPerms |= ACL_SELECT;
	rte->insertedCols = NULL;
	rte->updatedCols = NULL;

	/* 2. Fixup targetlist with the correct rel information. */
	foreach (lc, inp->final_seltlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		/*
		 * In case when this is a cagg wth joins, the Var from the normal table
		 * already has resorigtbl populated and we need to use that to resolve
		 * the Var. Hence only modify the tle when resorigtbl is unset
		 * which means it is Var of the Hypertable
		 */
		if (IsA(tle->expr, Var) && !OidIsValid(tle->resorigtbl))
		{
			tle->resorigtbl = rte->relid;
			tle->resorigcol = ((Var *) tle->expr)->varattno;
		}
	}

	CAGG_MAKEQUERY(final_selquery, inp->final_userquery);
	final_selquery->hasAggs = !inp->finalized;
	if (list_length(inp->final_userquery->jointree->fromlist) >=
			CONTINUOUS_AGG_MAX_JOIN_RELATIONS ||
		!IsA(linitial(inp->final_userquery->jointree->fromlist), RangeTblRef))
	{
		RangeTblRef *rtr;
		final_selquery->rtable = list_make1(rte);
		rtr = makeNode(RangeTblRef);
		rtr->rtindex = 1;
		fromexpr = makeFromExpr(list_make1(rtr), NULL);
	}
	else
	{
		final_selquery->rtable = inp->final_userquery->rtable;
		fromexpr = inp->final_userquery->jointree;
		fromexpr->quals = NULL;
	}

	/*
	 * Fixup from list. No quals on original table should be
	 * present here - they should be on the query that populates
	 * the mattable (partial_selquery). For the Cagg with join,
	 * we can not copy the fromlist from inp->final_userquery as
	 * it has two tables in this case.
	 */
	Assert(list_length(inp->final_userquery->jointree->fromlist) <=
		   CONTINUOUS_AGG_MAX_JOIN_RELATIONS);

	final_selquery->jointree = fromexpr;
	final_selquery->targetList = inp->final_seltlist;
	final_selquery->sortClause = inp->final_userquery->sortClause;

	if (!inp->finalized)
	{
		final_selquery->groupClause = inp->final_userquery->groupClause;
		/* Copy the having clause too */
		final_selquery->havingQual = inp->final_havingqual;
	}

	return final_selquery;
}

/*
 * Add Information required to create and populate the materialization table columns
 * a) create a columndef for the materialization table
 * b) create the corresponding expr to populate the column of the materialization table (e..g for a
 *    column that is an aggref, we create a partialize_agg expr to populate the column Returns: the
 *    Var corresponding to the newly created column of the materialization table
 *
 * Notes: make sure the materialization table columns do not save
 *        values computed by mutable function.
 *
 * Notes on TargetEntry fields:
 * - (resname != NULL) means it's projected in our case
 * - (ressortgroupref > 0) means part of GROUP BY, which can be projected or not, depending of the
 *                         value of the resjunk
 * - (resjunk == true) applies for GROUP BY columns that are not projected
 *
 */
static Var *
mattablecolumninfo_addentry(MatTableColumnInfo *out, Node *input, int original_query_resno,
							bool finalized, bool *skip_adding)
{
	int matcolno = list_length(out->matcollist) + 1;
	char colbuf[NAMEDATALEN];
	char *colname;
	TargetEntry *part_te = NULL;
	ColumnDef *col;
	Var *var;
	Oid coltype, colcollation;
	int32 coltypmod;

	*skip_adding = false;

	if (contain_mutable_functions(input))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only immutable functions supported in continuous aggregate view"),
				 errhint("Make sure all functions in the continuous aggregate definition"
						 " have IMMUTABLE volatility. Note that functions or expressions"
						 " may be IMMUTABLE for one data type, but STABLE or VOLATILE for"
						 " another.")));
	}

	switch (nodeTag(input))
	{
		case T_Aggref:
		{
			FuncExpr *fexpr = get_partialize_funcexpr((Aggref *) input);
			makeMaterializeColumnName(colbuf, "agg", original_query_resno, matcolno);
			colname = colbuf;
			coltype = BYTEAOID;
			coltypmod = -1;
			colcollation = InvalidOid;
			col = makeColumnDef(colname, coltype, coltypmod, colcollation);
			part_te = makeTargetEntry((Expr *) fexpr, matcolno, pstrdup(colname), false);
		}
		break;

		case T_TargetEntry:
		{
			TargetEntry *tle = (TargetEntry *) input;
			bool timebkt_chk = false;

			if (IsA(tle->expr, FuncExpr))
				timebkt_chk = function_allowed_in_cagg_definition(((FuncExpr *) tle->expr)->funcid);

			if (tle->resname)
				colname = pstrdup(tle->resname);
			else
			{
				if (timebkt_chk)
					colname = DEFAULT_MATPARTCOLUMN_NAME;
				else
				{
					makeMaterializeColumnName(colbuf, "grp", original_query_resno, matcolno);
					colname = colbuf;

					/* For finalized form we skip adding extra group by columns. */
					*skip_adding = finalized;
				}
			}

			if (timebkt_chk)
			{
				tle->resname = pstrdup(colname);
				out->matpartcolno = matcolno;
				out->matpartcolname = pstrdup(colname);
			}
			else
			{
				/*
				 * Add indexes only for columns that are part of the GROUP BY clause
				 * and for finals form.
				 * We skip adding it because we'll not add the extra group by columns
				 * to the materialization hypertable anymore.
				 */
				if (!*skip_adding && tle->ressortgroupref > 0)
					out->mat_groupcolname_list =
						lappend(out->mat_groupcolname_list, pstrdup(colname));
			}

			coltype = exprType((Node *) tle->expr);
			coltypmod = exprTypmod((Node *) tle->expr);
			colcollation = exprCollation((Node *) tle->expr);
			col = makeColumnDef(colname, coltype, coltypmod, colcollation);
			part_te = (TargetEntry *) copyObject(input);

			/* Keep original resjunk if finalized or not time bucket. */
			if (!finalized || timebkt_chk)
			{
				/*
				 * Need to project all the partial entries so that
				 * materialization table is filled.
				 */
				part_te->resjunk = false;
			}

			part_te->resno = matcolno;

			if (timebkt_chk)
			{
				col->is_not_null = true;
			}

			if (part_te->resname == NULL)
			{
				part_te->resname = pstrdup(colname);
			}
		}
		break;

		case T_Var:
		{
			makeMaterializeColumnName(colbuf, "var", original_query_resno, matcolno);
			colname = colbuf;

			coltype = exprType(input);
			coltypmod = exprTypmod(input);
			colcollation = exprCollation(input);
			col = makeColumnDef(colname, coltype, coltypmod, colcollation);
			part_te = makeTargetEntry((Expr *) input, matcolno, pstrdup(colname), false);

			/* Need to project all the partial entries so that materialization table is filled. */
			part_te->resjunk = false;
			part_te->resno = matcolno;
		}
		break;

		default:
			elog(ERROR, "invalid node type %d", nodeTag(input));
			break;
	}
	Assert((!finalized && list_length(out->matcollist) == list_length(out->partial_seltlist)) ||
		   (finalized && list_length(out->matcollist) <= list_length(out->partial_seltlist)));
	Assert(col != NULL);
	Assert(part_te != NULL);

	if (!*skip_adding)
	{
		out->matcollist = lappend(out->matcollist, col);
	}

	out->partial_seltlist = lappend(out->partial_seltlist, part_te);

	var = makeVar(1, matcolno, coltype, coltypmod, colcollation, 0);
	return var;
}
