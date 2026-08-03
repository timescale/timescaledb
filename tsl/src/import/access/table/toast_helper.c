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
 *
 * This mirrors backend/access/table/toast_helper.c; see
 * import/compression_toast.h for what the fork as a whole is for, and the
 * comment above each function for which core function it copies and what
 * was changed.
 */

#include <postgres.h>

#include <access/toast_helper.h>

#include "import/compression_toast.h"

/*
 * This is a copy of toast_tuple_externalize() in backend/access/table/toast_helper.c
 * from PG 18.4, git commit sha f5cc81719e6da4cbdb1f797c48b693e91018153a. It
 * has one modification: it calls compression_toast_save_datum_multi() instead
 * of toast_save_datum().
 */
void
compression_toast_tuple_externalize(BulkWriter *writer, ToastTupleContext *ttc, int attribute)
{
	Datum *value = &ttc->ttc_values[attribute];
	Datum old_value = *value;
	ToastAttrInfo *attr = &ttc->ttc_attr[attribute];

	attr->tai_colflags |= TOASTCOL_IGNORE;
	*value = compression_toast_save_datum_multi(writer, old_value, attr->tai_oldexternal);
	if ((attr->tai_colflags & TOASTCOL_NEEDS_FREE) != 0)
	{
		pfree(DatumGetPointer(old_value));
	}
	attr->tai_colflags |= TOASTCOL_NEEDS_FREE;
	ttc->ttc_flags |= (TOAST_NEEDS_CHANGE | TOAST_NEEDS_FREE);
}
