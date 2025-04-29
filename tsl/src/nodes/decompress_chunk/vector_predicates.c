/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */

/*
 * Functions for working with vectorized predicates.
 */

#include <postgres.h>

#include <mb/pg_wchar.h>
#include <utils/date.h>
#include <utils/fmgroids.h>

#include "compression/arrow_c_data_interface.h"

#include "vector_predicates.h"

#include "compat/compat.h"
#include "compression/compression.h"
#include "debug_assert.h"

/*
 * We include all implementations of vector-const predicates here. No separate
 * declarations for them to reduce the amount of macro template magic.
 */
#include "pred_vector_const_arithmetic_all.c"

#include "pred_text.h"

/*
 * Look up the vectorized implementation for a Postgres predicate, specified by
 * its Oid in pg_proc. Note that this Oid is different from the opcode.
 */
VectorPredicate *
get_vector_const_predicate(Oid pg_predicate)
{
	switch (pg_predicate)
	{
#define GENERATE_DISPATCH_TABLE
#include "pred_vector_const_arithmetic_all.c"
#undef GENERATE_DISPATCH_TABLE

		case F_TEXTEQ:
			return vector_const_texteq;

		case F_TEXTNE:
			return vector_const_textne;

		default:
			/*
			 * More checks below, this branch is to placate the static analyzers.
			 */
			break;
	}

	if (GetDatabaseEncoding() == PG_UTF8)
	{
		/* We have some simple LIKE vectorization for case-sensitive UTF8. */
		switch (pg_predicate)
		{
			case F_TEXTLIKE:
				return vector_const_textlike_utf8;
			case F_TEXTNLIKE:
				return vector_const_textnlike_utf8;
			default:
				/*
				 * This branch is to placate the static analyzers.
				 */
				break;
		}
	}

	return NULL;
}

void
vector_nulltest(const ArrowArray *arrow, int test_type, uint64 *restrict result)
{
	const bool should_be_null = test_type == IS_NULL;

	const uint16 bitmap_words = (arrow->length + 63) / 64;
	const uint64 *validity = (const uint64 *) arrow->buffers[0];
	for (uint16 i = 0; i < bitmap_words; i++)
	{
		const uint64 validity_word = validity != NULL ? validity[i] : ~0ULL;
		if (should_be_null)
		{
			result[i] &= ~validity_word;
		}
		else
		{
			result[i] &= validity_word;
		}
	}
}

void
vector_booleantest(const ArrowArray *arrow, int test_type, uint64 *restrict result)
{
	const uint16 bitmap_words = (arrow->length + 63) / 64;
	const uint64 *restrict validity = (const uint64 *) arrow->buffers[0];
	const uint64 *restrict values = (const uint64 *) arrow->buffers[1];

	switch (test_type)
	{
		case IS_TRUE:
		{
			if (validity)
			{
				for (uint16 i = 0; i < bitmap_words; i++)
				{
					result[i] &= validity[i] & values[i];
				}
			}
			else
			{
				for (uint16 i = 0; i < bitmap_words; i++)
				{
					result[i] &= values[i];
				}
			}
			break;
		}
		case IS_NOT_TRUE:
		{
			if (validity)
			{
				for (uint16 i = 0; i < bitmap_words; i++)
				{
					result[i] &= (~validity[i] | ~values[i]);
				}
			}
			else
			{
				for (uint16 i = 0; i < bitmap_words; i++)
				{
					result[i] &= ~values[i];
				}
			}
			break;
		}
		case IS_FALSE:
		{
			if (validity)
			{
				for (uint16 i = 0; i < bitmap_words; i++)
				{
					result[i] &= validity[i] & ~values[i];
				}
			}
			else
			{
				for (uint16 i = 0; i < bitmap_words; i++)
				{
					result[i] &= ~values[i];
				}
			}
			break;
		}
		case IS_NOT_FALSE:
		{
			if (validity)
			{
				for (uint16 i = 0; i < bitmap_words; i++)
				{
					result[i] &= (~validity[i] | values[i]);
				}
			}
			else
			{
				for (uint16 i = 0; i < bitmap_words; i++)
				{
					result[i] &= values[i];
				}
			}
			break;
		}
		case IS_UNKNOWN:
		{
			if (validity)
			{
				for (uint16 i = 0; i < bitmap_words; i++)
				{
					result[i] &= ~validity[i];
				}
			}
			else
			{
				/* No validity, so all rows are valid and all result rows are filtered out. */
				memset(result, 0, bitmap_words * sizeof(uint64));
			}
			break;
		}
		case IS_NOT_UNKNOWN:
		{
			if (validity)
			{
				for (uint16 i = 0; i < bitmap_words; i++)
				{
					result[i] &= validity[i];
				}
			}
			break;
		}
		default:
			Assert(false);
			break;
	}
}
