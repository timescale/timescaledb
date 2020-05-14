/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
#include <postgres.h>
#include <fmgr.h>

#include "export.h"

#include "test_utils.h"

TS_FUNCTION_INFO_V1(ts_test_adts);

#define VEC_PREFIX int32
#define VEC_ELEMENT_TYPE int32
#define VEC_DECLARE 1
#define VEC_DEFINE 1
#define VEC_SCOPE static inline
#include <adts/vec.h>

#include <adts/bit_array.h>

static void
i32_vec_test(void)
{
	int32_vec *vec = int32_vec_create(CurrentMemoryContext, 0);
	int i;
	uint32 old_capacity;

	for (i = 0; i < 100; i++)
		int32_vec_append(vec, i);

	TestAssertInt64Eq(vec->num_elements, 100);

	if (vec->max_elements < 100)
		elog(ERROR, "vec capacity %d, should be at least 100", vec->max_elements);

	for (i = 0; i < 100; i++)
		TestAssertInt64Eq(*int32_vec_at(vec, i), i);

	TestAssertPtrEq(int32_vec_last(vec), int32_vec_at(vec, vec->num_elements - 1));

	old_capacity = vec->max_elements;
	int32_vec_delete_range(vec, 30, 19);
	TestAssertInt64Eq(vec->num_elements, 81);
	TestAssertInt64Eq(vec->max_elements, old_capacity);

	for (i = 0; i < 30; i++)
		TestAssertInt64Eq(*int32_vec_at(vec, i), i);

	for (; i < 51; i++)
		TestAssertInt64Eq(*int32_vec_at(vec, i), i + 19);

	TestAssertPtrEq(int32_vec_last(vec), int32_vec_at(vec, vec->num_elements - 1));

	int32_vec_clear(vec);
	TestAssertInt64Eq(vec->num_elements, 0);
	TestAssertInt64Eq(vec->max_elements, old_capacity);

	int32_vec_free_data(vec);
	TestAssertInt64Eq(vec->num_elements, 0);
	TestAssertInt64Eq(vec->max_elements, 0);
	TestAssertPtrEq(vec->data, NULL);

	/* free_data is idempotent */
	int32_vec_free_data(vec);
	TestAssertInt64Eq(vec->num_elements, 0);
	TestAssertInt64Eq(vec->max_elements, 0);
	TestAssertPtrEq(vec->data, NULL);

	int32_vec_free(vec);
}

/* including BitArray should give us uint64_vec */
static void
uint64_vec_test(void)
{
	uint64_vec vec;
	int i;
	uint64_vec_init(&vec, CurrentMemoryContext, 100);
	for (i = 0; i < 30; i++)
		uint64_vec_append(&vec, i + 3);

	TestAssertInt64Eq(vec.num_elements, 30);
	TestAssertInt64Eq(vec.max_elements, 100);
	for (i = 0; i < 30; i++)
		TestAssertInt64Eq(*uint64_vec_at(&vec, i), i + 3);

	uint64_vec_free_data(&vec);
	TestAssertInt64Eq(vec.num_elements, 0);
	TestAssertInt64Eq(vec.max_elements, 0);
	TestAssertPtrEq(vec.data, NULL);
}

static void
bit_array_test(void)
{
	BitArray bits;
	BitArrayIterator iter;
	int i;
	bit_array_init(&bits);

	for (i = 0; i < 65; i++)
		bit_array_append(&bits, i, i);

	bit_array_append(&bits, 0, 0);
	bit_array_append(&bits, 0, 0);
	bit_array_append(&bits, 64, 0x9069060909009090);
	bit_array_append(&bits, 1, 0);
	bit_array_append(&bits, 64, ~0x9069060909009090);
	bit_array_append(&bits, 1, 1);

	bit_array_iterator_init(&iter, &bits);
	for (i = 0; i < 65; i++)
		TestAssertInt64Eq(bit_array_iter_next(&iter, i), i);

	TestAssertInt64Eq(bit_array_iter_next(&iter, 0), 0);
	TestAssertInt64Eq(bit_array_iter_next(&iter, 0), 0);
	TestAssertInt64Eq(bit_array_iter_next(&iter, 64), 0x9069060909009090);
	TestAssertInt64Eq(bit_array_iter_next(&iter, 1), 0);
	TestAssertInt64Eq(bit_array_iter_next(&iter, 64), ~0x9069060909009090);
	TestAssertInt64Eq(bit_array_iter_next(&iter, 1), 1);

	bit_array_iterator_init_rev(&iter, &bits);
	TestAssertInt64Eq(bit_array_iter_next_rev(&iter, 1), 1);
	TestAssertInt64Eq(bit_array_iter_next_rev(&iter, 64), ~0x9069060909009090);
	TestAssertInt64Eq(bit_array_iter_next_rev(&iter, 1), 0);
	TestAssertInt64Eq(bit_array_iter_next_rev(&iter, 64), 0x9069060909009090);
	TestAssertInt64Eq(bit_array_iter_next_rev(&iter, 0), 0);
	TestAssertInt64Eq(bit_array_iter_next_rev(&iter, 0), 0);
	for (i = 64; i >= 0; i--)
		TestAssertInt64Eq(bit_array_iter_next_rev(&iter, i), i);
}

Datum
ts_test_adts(PG_FUNCTION_ARGS)
{
	i32_vec_test();
	uint64_vec_test();
	bit_array_test();
	PG_RETURN_VOID();
}
