/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * A generic function for aggregating a constant input. We use a very simple
 * implementation here, because aggregating a segmentby column or a column with
 * default value is a relatively rare case, but it requires a fully custom
 * implementation otherwise.
 */
static void
FUNCTION_NAME(scalar)(void *agg_state, Datum constvalue, bool constisnull, int n,
					  MemoryContext agg_extra_mctx)
{
	if (constisnull)
	{
		return;
	}

	const CTYPE value = DATUM_TO_CTYPE(constvalue);

	MemoryContext old = MemoryContextSwitchTo(agg_extra_mctx);
	for (int i = 0; i < n; i++)
	{
		FUNCTION_NAME(one)(agg_state, value);
	}
	MemoryContextSwitchTo(old);
}
