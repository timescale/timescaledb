/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 */
#include <postgres.h>
#include <parser/parsetree.h>
#include <optimizer/restrictinfo.h>
#include <utils/guc.h>
#include <utils/builtins.h>
#include <utils/memutils.h>
#include <utils/rel.h>
#include <access/sysattr.h>
#include <access/htup_details.h>
#include <miscadmin.h>
#include <catalog/pg_type.h>
#include <utils/syscache.h>

#include "utils.h"
#include "compat.h"
#include "remote/data_format.h"
#include "tuplefactory.h"

/*
 * Identify the attribute where data conversion fails.
 */
typedef struct ConversionLocation
{
	Relation rel;		  /* foreign table's relcache entry. */
	AttrNumber cur_attno; /* attribute number being processed, or 0 */

	/*
	 * In case of foreign join push down, fdw_scan_tlist is used to identify
	 * the Var node corresponding to the error location and
	 * ss->ps.state gives access to the RTEs of corresponding relation
	 * to get the relation name and attribute name.
	 */
	ScanState *ss;
} ConversionLocation;

typedef struct TupleFactory
{
	MemoryContext temp_mctx;
	TupleDesc tupdesc;
	Datum *values;
	bool *nulls;
	List *retrieved_attrs;
	AttConvInMetadata *attconv;
	ConversionLocation errpos;
	ErrorContextCallback errcallback;
} TupleFactory;

/*
 * Callback function which is called when error occurs during column value
 * conversion.  Print names of column and relation.
 */
static void
conversion_error_callback(void *arg)
{
	const char *attname = NULL;
	const char *relname = NULL;
	bool is_wholerow = false;
	ConversionLocation *errpos = (ConversionLocation *) arg;

	if (errpos->rel)
	{
		/* error occurred in a scan against a foreign table */
		TupleDesc tupdesc = RelationGetDescr(errpos->rel);
		Form_pg_attribute attr = TupleDescAttr(tupdesc, errpos->cur_attno - 1);

		if (errpos->cur_attno > 0 && errpos->cur_attno <= tupdesc->natts)
			attname = NameStr(attr->attname);
		else if (errpos->cur_attno == SelfItemPointerAttributeNumber)
			attname = "ctid";
		else if (errpos->cur_attno == ObjectIdAttributeNumber)
			attname = "oid";

		relname = RelationGetRelationName(errpos->rel);
	}
	else
	{
		/* error occurred in a scan against a foreign join */
		ScanState *ss = errpos->ss;
		ForeignScan *fsplan;
		EState *estate = ss->ps.state;
		TargetEntry *tle;

		if (IsA(ss->ps.plan, ForeignScan))
			fsplan = (ForeignScan *) ss->ps.plan;
		else if (IsA(ss->ps.plan, CustomScan))
		{
			CustomScan *csplan = (CustomScan *) ss->ps.plan;

			fsplan = linitial(csplan->custom_private);
		}
		else
			elog(ERROR, "unknown scan node type %u in error callback", nodeTag(ss->ps.plan));

		tle = list_nth_node(TargetEntry, fsplan->fdw_scan_tlist, errpos->cur_attno - 1);

		/*
		 * Target list can have Vars and expressions.  For Vars, we can get
		 * its relation, however for expressions we can't.  Thus for
		 * expressions, just show generic context message.
		 */
		if (IsA(tle->expr, Var))
		{
			RangeTblEntry *rte;
			Var *var = (Var *) tle->expr;

			rte = rt_fetch(var->varno, estate->es_range_table);

			if (var->varattno == 0)
				is_wholerow = true;
			else
				attname = get_attname_compat(rte->relid, var->varattno, false);

			relname = get_rel_name(rte->relid);
		}
		else
			errcontext("processing expression at position %d in select list", errpos->cur_attno);
	}

	if (relname)
	{
		if (is_wholerow)
			errcontext("whole-row reference to foreign table \"%s\"", relname);
		else if (attname)
			errcontext("column \"%s\" of foreign table \"%s\"", attname, relname);
	}
}

static TupleFactory *
tuplefactory_create(Relation rel, ScanState *ss, List *retrieved_attrs)
{
	TupleFactory *tf = palloc0(sizeof(TupleFactory));

	tf->temp_mctx = AllocSetContextCreate(CurrentMemoryContext,
										  "tuple factory temporary data",
										  ALLOCSET_SMALL_SIZES);

	if (NULL != rel)
		tf->tupdesc = RelationGetDescr(rel);
	else
	{
		Assert(ss);
		tf->tupdesc = ss->ss_ScanTupleSlot->tts_tupleDescriptor;
	}

	tf->retrieved_attrs = retrieved_attrs;
	tf->attconv = data_format_create_att_conv_in_metadata(tf->tupdesc);
	tf->values = (Datum *) palloc0(tf->tupdesc->natts * sizeof(Datum));
	tf->nulls = (bool *) palloc(tf->tupdesc->natts * sizeof(bool));

	/* Initialize to nulls for any columns not present in result */
	memset(tf->nulls, true, tf->tupdesc->natts * sizeof(bool));

	tf->errpos.rel = rel;
	tf->errpos.cur_attno = 0;
	tf->errpos.ss = ss;
	tf->errcallback.callback = conversion_error_callback;
	tf->errcallback.arg = (void *) &tf->errpos;
	tf->errcallback.previous = error_context_stack;

	return tf;
}

TupleFactory *
tuplefactory_create_for_rel(Relation rel, List *retrieved_attrs)
{
	return tuplefactory_create(rel, NULL, retrieved_attrs);
}

TupleFactory *
tuplefactory_create_for_scan(ScanState *ss, List *retrieved_attrs)
{
	return tuplefactory_create(NULL, ss, retrieved_attrs);
}

bool
tuplefactory_is_binary(TupleFactory *tf)
{
	return tf->attconv->binary;
}

HeapTuple
tuplefactory_make_tuple(TupleFactory *tf, PGresult *res, int row)
{
	HeapTuple tuple;
	ItemPointer ctid = NULL;
	Oid oid = InvalidOid;
	MemoryContext oldcontext;
	ListCell *lc;
	int j;
	int format;
	StringInfo buf;

	Assert(row < PQntuples(res));

	/*
	 * Do the following work in a temp context that we reset after each tuple.
	 * This cleans up not only the data we have direct access to, but any
	 * cruft the I/O functions might leak.
	 */
	oldcontext = MemoryContextSwitchTo(tf->temp_mctx);

	buf = makeStringInfo();

	/* Install error callback */
	tf->errcallback.previous = error_context_stack;
	error_context_stack = &tf->errcallback;

	/*
	 * i indexes columns in the relation, j indexes columns in the PGresult.
	 */
	j = 0;
	foreach (lc, tf->retrieved_attrs)
	{
		int i = lfirst_int(lc);
		char *valstr;

		resetStringInfo(buf);
		/* fetch next column's value */
		if (PQgetisnull(res, row, j))
			valstr = NULL;
		else
		{
			valstr = PQgetvalue(res, row, j);
			buf->data = valstr;
		}

		format = PQfformat(res, j);
		buf->len = PQgetlength(res, row, j);

		/*
		 * convert value to internal representation
		 *
		 * Note: we ignore system columns other than ctid and oid in result
		 */
		tf->errpos.cur_attno = i;

		if (i > 0)
		{
			/* ordinary column */
			Assert(i <= tf->tupdesc->natts);
			tf->nulls[i - 1] = (valstr == NULL);

			if (format == FORMAT_TEXT)
			{
				Assert(!tf->attconv->binary);
				/* Apply the input function even to nulls, to support domains */
				tf->values[i - 1] = InputFunctionCall(&tf->attconv->conv_funcs[i - 1],
													  valstr,
													  tf->attconv->ioparams[i - 1],
													  tf->attconv->typmods[i - 1]);
			}
			else
			{
				Assert(tf->attconv->binary);
				if (valstr != NULL)
					tf->values[i - 1] = ReceiveFunctionCall(&tf->attconv->conv_funcs[i - 1],
															buf,
															tf->attconv->ioparams[i - 1],
															tf->attconv->typmods[i - 1]);
				else
					tf->values[i - 1] = PointerGetDatum(NULL);
			}
		}
		else if (i == SelfItemPointerAttributeNumber)
		{
			/* ctid */
			if (valstr != NULL)
			{
				Datum datum;
				if (format == FORMAT_TEXT)
					datum = DirectFunctionCall1(tidin, CStringGetDatum(valstr));
				else
					datum = DirectFunctionCall1(tidrecv, PointerGetDatum(buf));
				ctid = (ItemPointer) DatumGetPointer(datum);
			}
		}
		else if (i == ObjectIdAttributeNumber)
		{
			/* oid */
			if (valstr != NULL)
			{
				Datum datum;
				if (format == FORMAT_TEXT)
					datum = DirectFunctionCall1(oidin, CStringGetDatum(valstr));
				else
					datum = DirectFunctionCall1(oidrecv, PointerGetDatum(buf));
				oid = DatumGetObjectId(datum);
			}
		}
		tf->errpos.cur_attno = 0;
		j++;
	}

	/* Uninstall error context callback. */
	error_context_stack = tf->errcallback.previous;

	/*
	 * Check we got the expected number of columns.  Note: j == 0 and
	 * PQnfields == 1 is expected, since deparse emits a NULL if no columns.
	 */
	if (j > 0 && j != PQnfields(res))
		elog(ERROR, "remote query result does not match the foreign table");

	/*
	 * Build the result tuple in caller's memory context.
	 */
	MemoryContextSwitchTo(oldcontext);

	tuple = heap_form_tuple(tf->tupdesc, tf->values, tf->nulls);

	/*
	 * If we have a CTID to return, install it in both t_self and t_ctid.
	 * t_self is the normal place, but if the tuple is converted to a
	 * composite Datum, t_self will be lost; setting t_ctid allows CTID to be
	 * preserved during EvalPlanQual re-evaluations (see ROW_MARK_COPY code).
	 */
	if (ctid)
		tuple->t_self = tuple->t_data->t_ctid = *ctid;

	/*
	 * Stomp on the xmin, xmax, and cmin fields from the tuple created by
	 * heap_form_tuple.  heap_form_tuple actually creates the tuple with
	 * DatumTupleFields, not HeapTupleFields, but the executor expects
	 * HeapTupleFields and will happily extract system columns on that
	 * assumption.  If we don't do this then, for example, the tuple length
	 * ends up in the xmin field, which isn't what we want.
	 */
	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);

	/*
	 * If we have an OID to return, install it.
	 */
	if (OidIsValid(oid))
		HeapTupleSetOid(tuple, oid);

	/* Clean up */
	MemoryContextReset(tf->temp_mctx);

	return tuple;
}
