/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */

/* NOTE header guard deliberately omitted, as it is valid to include this header
 * multiple times in a single file
 */

/*
 *	  To generate a vector and associated functions for a use case several
 *	  macros have to be #define'ed before this file is included.  Including
 *	  the file #undef's all those, so a new vectors can be generated afterwards.
 *	  The relevant parameters are:
 *	  - VEC_PREFIX - prefix for all symbol names generated. A prefix of 'foo'
 *		    will result in vector table type 'foo_vec' and functions like
 *		    'foo_vec_append'/'foo_vec_at' and so forth. This is usually the
 *          name of the stored element type
 *	  - VEC_ELEMENT_TYPE - type of the contained elements.
 *	  - VEC_DECLARE - if defined function prototypes and type declarations are
 *		    generated
 *	  - VEC_DEFINE - if defined function definitions are generated
 *	  - VEC_SCOPE - in which scope (e.g. extern, static inline) do function
 *		    declarations reside
 */

#define VEC_MAKE_PREFIX(a) CppConcat(a, _)
#define VEC_MAKE_NAME(name) VEC_MAKE_NAME_(VEC_MAKE_PREFIX(VEC_PREFIX), name)
#define VEC_MAKE_NAME_(a, b) CppConcat(a, b)

/* name macros for: */

/* type declarations */
#define VEC_TYPE VEC_MAKE_NAME(vec)

/* function declarations */
// TODO add INSERT for inserting into the middle of a vec
#define VEC_INIT VEC_MAKE_NAME(vec_init)
#define VEC_CREATE VEC_MAKE_NAME(vec_create)
#define VEC_FREE_DATA VEC_MAKE_NAME(vec_free_data)
#define VEC_CLEAR VEC_MAKE_NAME(vec_clear)
#define VEC_AT VEC_MAKE_NAME(vec_at)
#define VEC_GET VEC_MAKE_NAME(vec_get)
#define VEC_LAST VEC_MAKE_NAME(vec_last)
#define VEC_APPEND VEC_MAKE_NAME(vec_append)
#define VEC_APPEND_ARRAY VEC_MAKE_NAME(vec_append_array)
#define VEC_APPEND_ZEROS VEC_MAKE_NAME(vec_append_zeros)
#define VEC_DELETE VEC_MAKE_NAME(vec_delete)
#define VEC_DELETE_RANGE VEC_MAKE_NAME(vec_delete_range)
#define VEC_RESERVE VEC_MAKE_NAME(vec_reserve)
#define VEC_FREE VEC_MAKE_NAME(vec_free)

/* generate forward declarations necessary to use the vector */
#ifdef VEC_DECLARE

/* type definitions */
typedef struct VEC_TYPE
{
	/* size of the elements array */
	uint32 max_elements;
	/* number of elements currently used */
	uint32 num_elements;

	/* the actual data */
	VEC_ELEMENT_TYPE *data;

	/* memory context to use for allocations */
	MemoryContext ctx;
} VEC_TYPE;

/* externally visible function prototypes */
VEC_SCOPE void VEC_INIT(VEC_TYPE *vec, MemoryContext ctx, uint32 nelements);
VEC_SCOPE VEC_TYPE *VEC_CREATE(MemoryContext ctx, uint32 nelements);
VEC_SCOPE void VEC_FREE_DATA(VEC_TYPE *vec);
VEC_SCOPE void VEC_CLEAR(VEC_TYPE *vec);
VEC_SCOPE VEC_ELEMENT_TYPE *VEC_AT(VEC_TYPE *vec, uint32 index);
VEC_SCOPE const VEC_ELEMENT_TYPE *VEC_GET(const VEC_TYPE *vec, uint32 index);
VEC_SCOPE VEC_ELEMENT_TYPE *VEC_LAST(VEC_TYPE *vec);
VEC_SCOPE void VEC_APPEND(VEC_TYPE *vec, VEC_ELEMENT_TYPE element);
VEC_SCOPE VEC_ELEMENT_TYPE *VEC_APPEND_ARRAY(VEC_TYPE *vec, VEC_ELEMENT_TYPE *elements,
											 uint32 num_elements);
VEC_SCOPE VEC_ELEMENT_TYPE *VEC_APPEND_ZEROS(VEC_TYPE *vec, uint32 num_elements);
VEC_SCOPE void VEC_DELETE(VEC_TYPE *vec, uint32 index);
VEC_SCOPE void VEC_DELETE_RANGE(VEC_TYPE *vec, uint32 start, uint32 len);
VEC_SCOPE void VEC_RESERVE(VEC_TYPE *vec, uint32 additional);
VEC_SCOPE void VEC_FREE(VEC_TYPE *vec);

#endif /* VEC_DECLARE */

/* generate implementation of the vector */
#ifdef VEC_DEFINE

#include <utils/memutils.h>

/*
 * Allocate space so the vector can store least `additional` new elements.
 *
 * Usually this will automatically be called by appends/inserts, when
 * necessary. But resizing to the exact input size can be advantageous
 * performance-wise, when known at some point.
 */
VEC_SCOPE void
VEC_RESERVE(VEC_TYPE *vec, uint32 additional)
{
	uint64 num_new_elements = additional;
	uint64 num_elements;
	uint64 num_bytes;

	// TODO handle overflow, huge allocations, if desired
	if (num_new_elements == 0 || vec->num_elements + num_new_elements <= vec->max_elements)
		return;

	if (num_new_elements < vec->num_elements / 2)
		num_new_elements = vec->num_elements / 2;

	num_elements = vec->num_elements + num_new_elements;
	Assert(num_elements > vec->num_elements);
	if (num_elements >= PG_UINT32_MAX / sizeof(VEC_ELEMENT_TYPE))
		elog(ERROR, "vector allocation overflow");
	vec->max_elements = num_elements;

	num_bytes = vec->max_elements * sizeof(VEC_ELEMENT_TYPE);
	if (vec->data == NULL)
		vec->data = MemoryContextAlloc(vec->ctx, num_bytes);
	else
		vec->data = repalloc(vec->data, num_bytes);
}

/*
 * Initialize a vector with enough space for `nelements`. Memory is allocated
 * from the passed-in context.
 */
VEC_SCOPE void
VEC_INIT(VEC_TYPE *vec, MemoryContext ctx, uint32 nelements)
{
	*vec = (VEC_TYPE){
		.ctx = ctx,
	};
	if (nelements > 0)
		VEC_RESERVE(vec, nelements);
}

/*
 * Create a vector with enough space for `nelements`. Memory for the vector, and
 * its elements, is allocated from the passed-in context.
 */
VEC_SCOPE VEC_TYPE *
VEC_CREATE(MemoryContext ctx, uint32 nelements)
{
	VEC_TYPE *vec = MemoryContextAlloc(ctx, sizeof(*vec));
	VEC_INIT(vec, ctx, nelements);
	return vec;
}

/* free the underlying array */
VEC_SCOPE void
VEC_FREE_DATA(VEC_TYPE *vec)
{
	if (vec->data != NULL)
		pfree(vec->data);
	/* zero out all the vec data except the memory context so it can be reused */
	*vec = (VEC_TYPE){
		.ctx = vec->ctx,
	};
}

/* free an allocated vector, and its data */
VEC_SCOPE void
VEC_FREE(VEC_TYPE *vec)
{
	if (vec == NULL)
		return;
	VEC_FREE_DATA(vec);
	pfree(vec);
}

/* clear a vector, but don't free the underlying array */
VEC_SCOPE void
VEC_CLEAR(VEC_TYPE *vec)
{
	vec->num_elements = 0;
}

VEC_SCOPE VEC_ELEMENT_TYPE *
VEC_AT(VEC_TYPE *vec, uint32 index)
{
	Assert(index < vec->num_elements);
	return &vec->data[index];
}

VEC_SCOPE const VEC_ELEMENT_TYPE *
VEC_GET(const VEC_TYPE *vec, uint32 index)
{
	Assert(index < vec->num_elements);
	return &vec->data[index];
}

/* return a pointer to the last element in the vector */
VEC_SCOPE VEC_ELEMENT_TYPE *
VEC_LAST(VEC_TYPE *vec)
{
	Assert(vec->num_elements > 0);
	return VEC_AT(vec, vec->num_elements - 1);
}

VEC_SCOPE VEC_ELEMENT_TYPE *
VEC_APPEND_ARRAY(VEC_TYPE *vec, VEC_ELEMENT_TYPE *elements, uint32 num_elements)
{
	VEC_ELEMENT_TYPE *first_new_element;
	VEC_RESERVE(vec, num_elements);
	Assert(vec->num_elements < vec->max_elements);
	first_new_element = vec->data + vec->num_elements;
	memcpy(first_new_element, elements, sizeof(*elements) * num_elements);
	vec->num_elements += num_elements;
	return first_new_element;
}

VEC_SCOPE void
VEC_APPEND(VEC_TYPE *vec, VEC_ELEMENT_TYPE element)
{
	VEC_APPEND_ARRAY(vec, &element, 1);
}

VEC_SCOPE VEC_ELEMENT_TYPE *
VEC_APPEND_ZEROS(VEC_TYPE *vec, uint32 num_elements)
{
	VEC_ELEMENT_TYPE *first_new_element;
	VEC_RESERVE(vec, num_elements);
	Assert(vec->num_elements + num_elements <= vec->max_elements);
	first_new_element = vec->data + vec->num_elements;
	memset(first_new_element, 0, sizeof(*first_new_element) * num_elements);
	vec->num_elements += num_elements;
	return first_new_element;
}

VEC_SCOPE void
VEC_DELETE_RANGE(VEC_TYPE *vec, uint32 start, uint32 len)
{
	if (start > vec->num_elements)
		elog(ERROR, "trying to delete starting past the end of a vector");
	if (start + (uint64) len > (uint64) vec->num_elements)
		elog(ERROR, "trying to delete past the end of a vector");

	if (start + (uint64) len < (uint64) vec->num_elements)
	{
		/* backshift the elements after the deletion that still remain */
		uint64 backshit_elements = vec->num_elements - (start + len);
		uint64 backshit_bytes = backshit_elements * sizeof(*vec->data);
		memmove(&vec->data[start], &vec->data[start + len], backshit_bytes);
	}
	vec->num_elements -= len;
}

VEC_SCOPE void
VEC_DELETE(VEC_TYPE *vec, uint32 index)
{
	VEC_DELETE_RANGE(vec, index, 1);
}

#endif

/* undefine external parameters, so next vector can be defined */
#undef VEC_PREFIX
#undef VEC_ELEMENT_TYPE
#undef VEC_SCOPE
#undef VEC_DECLARE
#undef VEC_DEFINE

/* undefine locally declared macros */
#undef VEC_MAKE_PREFIX
#undef VEC_MAKE_NAME
#undef VEC_MAKE_NAME

/* types */
#undef VEC_TYPE

/* external function names */
#undef VEC_INIT
#undef VEC_CREATE
#undef VEC_FREE_DATA
#undef VEC_CLEAR
#undef VEC_AT
#undef VEC_GET
#undef VEC_LAST
#undef VEC_APPEND
#undef VEC_APPEND_ARRAY
#undef VEC_APPEND_ZEROS
#undef VEC_DELETE
#undef VEC_DELETE_RANGE
#undef VEC_RESERVE
#undef VEC_FREE
