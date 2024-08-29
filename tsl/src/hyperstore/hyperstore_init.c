/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

#include "hyperstore_init.h"

#include "config.h"
#include "hyperstore/arrow_cache_explain.h"
#include "hyperstore/attr_capture.h"
#include "nodes/columnar_scan/columnar_scan.h"

void
_hyperstore_init(void)
{
#if WITH_HYPERSTORE
	_columnar_scan_init();
	_arrow_cache_explain_init();
	_attr_capture_init();
#endif
}

void
_hyperstore_fini(void)
{
}
