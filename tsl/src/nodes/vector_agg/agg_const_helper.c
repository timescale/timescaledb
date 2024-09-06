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
FUNCTION_NAME(const)(void *agg_state, Datum constvalue, bool constisnull, int n,
					 MemoryContext agg_extra_mctx)
{
	const uint64 valid = constisnull ? 0 : 1;
	const CTYPE value = DATUM_TO_CTYPE(constvalue);

	for (int i = 0; i < n; i++)
	{
		FUNCTION_NAME(vector_impl)(agg_state, 1, &value, &valid, NULL, agg_extra_mctx);
	}
}
