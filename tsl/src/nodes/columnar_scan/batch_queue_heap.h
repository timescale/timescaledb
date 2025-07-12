/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
#pragma once

#include "batch_queue.h"

extern BatchQueue *batch_queue_heap_create(int num_compressed_cols, const List *sortinfo,
										   const TupleDesc result_tupdesc,
										   const BatchQueueFunctions *funcs);

extern const struct BatchQueueFunctions BatchQueueFunctionsHeap;
