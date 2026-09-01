/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/*
 * This file contains source code that was copied and/or modified from
 * the PostgreSQL database, which is licensed under the open-source
 * PostgreSQL License. Please see the NOTICE at the top level
 * directory for a copy of the PostgreSQL License.
 *
 * These function were copied from the PostgreSQL core planner, since
 * they were declared static in the core planner, but we need them for
 * our manipulations.
 */

#include <postgres.h>

#include "import/utils/selfuncs.h"

#include <catalog/pg_statistic.h>
#include <catalog/pg_collation_d.h>
#include <nodes/pathnodes.h>
#include <utils/datum.h>
#include <utils/lsyscache.h>

bool
ts_get_variable_range(PlannerInfo *root, VariableStatData *vardata, Oid sortop, Datum *min,
					  Datum *max)
{
	Datum tmin = 0;
	Datum tmax = 0;
	bool have_data = false;
	int16 typLen;
	bool typByVal;
	Oid opfuncoid;
	AttStatsSlot sslot;
	int i;

	/*
	 * XXX It's very tempting to try to use the actual column min and max, if
	 * we can get them relatively-cheaply with an index probe.  However, since
	 * this function is called many times during join planning, that could
	 * have unpleasant effects on planning speed.  Need more investigation
	 * before enabling this.
	 */
#ifdef NOT_USED
	if (get_actual_variable_range(root, vardata, sortop, min, max))
		return true;
#endif

	if (!HeapTupleIsValid(vardata->statsTuple))
	{
		/* no stats available, so default result */
		return false;
	}

	/*
	 * If we can't apply the sortop to the stats data, just fail.  In
	 * principle, if there's a histogram and no MCVs, we could return the
	 * histogram endpoints without ever applying the sortop ... but it's
	 * probably not worth trying, because whatever the caller wants to do with
	 * the endpoints would likely fail the security check too.
	 */
	opfuncoid = get_opcode(sortop);
	if (!statistic_proc_security_check(vardata, opfuncoid))
		return false;

	get_typlenbyval(vardata->atttype, &typLen, &typByVal);

	/*
	 * If there is a histogram, grab the first and last values.
	 *
	 * If there is a histogram that is sorted with some other operator than
	 * the one we want, fail --- this suggests that there is data we can't
	 * use.
	 */
	if (get_attstatsslot(&sslot,
						 vardata->statsTuple,
						 STATISTIC_KIND_HISTOGRAM,
						 sortop,
						 ATTSTATSSLOT_VALUES))
	{
		if (sslot.nvalues > 0)
		{
			tmin = datumCopy(sslot.values[0], typByVal, typLen);
			tmax = datumCopy(sslot.values[sslot.nvalues - 1], typByVal, typLen);
			have_data = true;
		}
		free_attstatsslot(&sslot);
	}
	else if (get_attstatsslot(&sslot, vardata->statsTuple, STATISTIC_KIND_HISTOGRAM, InvalidOid, 0))
	{
		free_attstatsslot(&sslot);
		return false;
	}

	/*
	 * If we have most-common-values info, look for extreme MCVs.  This is
	 * needed even if we also have a histogram, since the histogram excludes
	 * the MCVs.  However, usually the MCVs will not be the extreme values, so
	 * avoid unnecessary data copying.
	 */
	if (get_attstatsslot(&sslot,
						 vardata->statsTuple,
						 STATISTIC_KIND_MCV,
						 InvalidOid,
						 ATTSTATSSLOT_VALUES))
	{
		bool tmin_is_mcv = false;
		bool tmax_is_mcv = false;
		FmgrInfo opproc;

		fmgr_info(opfuncoid, &opproc);

		for (i = 0; i < sslot.nvalues; i++)
		{
			if (!have_data)
			{
				tmin = tmax = sslot.values[i];
				tmin_is_mcv = tmax_is_mcv = have_data = true;
				continue;
			}
			if (DatumGetBool(
					FunctionCall2Coll(&opproc, DEFAULT_COLLATION_OID, sslot.values[i], tmin)))
			{
				tmin = sslot.values[i];
				tmin_is_mcv = true;
			}
			if (DatumGetBool(
					FunctionCall2Coll(&opproc, DEFAULT_COLLATION_OID, tmax, sslot.values[i])))
			{
				tmax = sslot.values[i];
				tmax_is_mcv = true;
			}
		}
		if (tmin_is_mcv)
			tmin = datumCopy(tmin, typByVal, typLen);
		if (tmax_is_mcv)
			tmax = datumCopy(tmax, typByVal, typLen);
		free_attstatsslot(&sslot);
	}

	*min = tmin;
	*max = tmax;
	return have_data;
}
