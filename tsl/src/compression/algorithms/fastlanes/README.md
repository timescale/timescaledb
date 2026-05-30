# FastLanes reimplementation in C

## What is this library

This is mostly a header only, C library that implements FastLanes inspired bit-packing using C
macros and a very thin C wrapper over these. The library only uses portable C and the main
objective is to allow compilers generate efficient SIMD code from this. This is not a compression
library. It is a building block for higher level compression algorithms.

## Credits and introduction

[The FastLanes Compression Layout:
Decoding >100 Billion Integers per Second with Scalar Code](https://doi.org/10.14778/3598581.3598587)

That paper is the main inspiration basis of the implementation. There are other C++ and Rust
implementations out there, like [cwida/fastlanes](https://github.com/cwida/fastlanes) and
[spiraldb/fastlanes](https://github.com/spiraldb/fastlanes).

FastLanes is used as a building block for higher level compression algorithms, but not as a standalone
compression algorithm. The main use of FastLanes is to pack integers. The callers need to do extra work
to make this useful. For example, the callers need to determine the bit-width of the integers, or the
base value for the FFOR packing. See the below sections for more info.

The idea in FastLanes is to store the integers in a transposed format and the conversion between the
original and the transposed format is done with clever bit arithmetic operations. In the original paper,
the authors defined a virtual 1024 bit register and used logical bit operations for transposing the
register. In our implementation we define a series of smaller width registers and use operations defined
on these registers. Our operations are not exactly the same as those in the paper but heavily inspired
by them.

FastLanes lays N values out across S parallel lanes. Each lane stores N/S values, dense-packed at W bits
per slot. The "trick" is that the lanes are interleaved in memory so potentially a single SIMD instruction
can pack or unpack S values from S adjacent lanes at once — values stay contiguous within a lane (so
unpacking is just bit-extracts), but adjacent values in the input land in different lanes (so SIMD
parallelism is free).

So why do we want to do this at all? The reason is that we want to tightly pack same bitwidth integers together.
In case of odd bitwidths, for example 5 bits, and we want to pack them into a 64 bit integer, we have two options:

 1) either we pack 12 integers and waste 4 bits, or
 2) we split the 13th integer and handle the overflow

None of the options are ideal. The beauty of the FastLanes approach is that we can pack odd width integers when we
have 'enough' of them. In the compression context we tend to have many integers so the numbers are not an issue.

The second advantage of the FastLanes approach is that it takes advantage of the 'enough' numbers to pack, and
developed a set of instructions that allows to transpose the data with SIMD instructions. This is a huge
performance boost as compared to the sequential encoding and decoding of the integers.

Finally, FastLanes instructions are defined in portable C (in our case, and C++ in the original paper), so
there are no platform specific features needed. This allows us to use the same code in a wide variety of
platforms and we can rely on the compiler to generate efficient SIMD code.

## Overview

This implementation needs to be compiled as part of the TimescaleDB extension in C. For this reason integrating
the existing Rust or C++ implementations would cause a disproportionate amount of headache for us.

Another difference is that in TimescaleDB we need to support input sizes different than 1024. The current version
of TimescaleDB (2.27) targets 1000 as batch size. A further complication is that the database needs to support
NULL values, in which case the underlying compression will only store the non-NULL values, so we can easily end
up with way less than 1000 non-NULL values. Finally, there are situations specific to TimescaleDB where we
naturally have smaller batch sizes.

The problem with targeting a fixed size of 1024 elements (as in the original paper) when we often have much
smaller batches is that it leads to a lot of wasted space which will erode the packing advantages of FastLanes.
The natural pack boundary of FL1024 is 128 bytes, so the smallest amount we can truncate from the output is
128 bytes. This causes too much waste for smaller batches.

### Supporting vectorized execution

The FastLanes approach was chosen because of its speed advantages. These advantages are coming from the
vectorized instructions that allow to pack and unpack multiple values in parallel. This library tries to
hide the details of the internal mechanics of the FastLanes packing and unpacking, but still it requires
some level of cooperation from the caller. The three areas where it is required are:

- the input and output buffers need to be aligned as instructed by the library, because the SIMD instructions have alignment requirements
- the input buffer needs to have space for a certain number of elements, potentially more than the actual number of elements being packed
- the packed buffer needs to be allocated with a certain size, potentially more than the actual size of the packed data, and the caller needs to use the returned truncated size to determine the result size

Imagine that we have 249 integers to pack. It is faster to pad it to 256 elements and use AVX2 instructions
than use SSE2 to pack 128 and handle the remaining 121 separately. The requirements the library has are
designed along these lines, so we maximize speed at the expense of some extra buffer capacity.

The library provides functions to inform the user about these requirements, and there is a section below, that
goes into more details. From the high level perspective, the alignment and sizing requirements all derives from
these three parameters:

- the number of elements to pack (N)
- the width of the input elements (T)
- the actual bit-width of the packed elements (W)

Where W is <= T, and N is <= 256. These determine the actual packing method the library uses and the requirements
for the buffers.

'N', 'W' and 'T' are the central concepts in the FastLanes library and they are often referred to by only these
initials. The number of parallel lanes (S) is also a central concept but it is derived from T and the tier bits,
so it is not as commonly used as N, W and T. 'S' is an internal variable that is not exposed to the caller.

The library uses `fl_elem_width_t` enum to represent the element width (T) in bits, and it has values for 8, 16,
32 and 64 bits.

## Tiered FastLanes (FL)

The solution for the FL1024 waste is that we introduce the concept of "Tiered FastLanes". The idea is to implement
FastLanes with a set of 'virtual register sizes' and use the smallest one that fits. This saves space but reduces
SIMD performance. We have 8, 16, 32, 64, 128 and 256 bits FastLanes. This reduces the number of wasted bytes
significantly for smaller batches. The tradeoff is slightly reduced SIMD performance.

Another way to look at this, is that if we pack a small number of integers into a format that is targeted at
1024 integers, then we will execute a transpose operation that is designed for 1024 integers, so much of the
work is wasted. Even if the bigger register width allows better SIMD efficiency, the wasted work offsets the
performance gain, and using a smaller FL tier becomes a better performance deal.

The tiered FL packing is used in many places in the compression algorithm, not only to store the compressed bit
values but also for smaller integer arrays, for example the new dictionary compression has a smaller array of the
dictionary entries, or in the PFOR compression the exceptions are stored as small integer arrays. In these cases
we easily end up storing only a handful of elements and this is why the very small tiers are useful. The small
tiers don't offer much SIMD performance but they save significant amount of space.

The library provides wrappers over the FL tiers in the `fastlanes/fastlanes.h` file and the internal
tiers are not exposed.

## The C implementation

Compared to the C++ implementation, where the kernel is auto-generated code and C++ templates or the Rust
implementation which uses Rust macros, the C implementation heavily relies on C macros. The implementation
still uses portable C and avoids any platform specific features. The macros are structured such that the C
compiler can vectorize the code as much as possible.

## Truncation

The truncation term appears in several places in this doc and also in the code itself. This refers to the action when
the number of elements we pass to an FL tier is less than it can maximally pack. This results in unused space in
the output buffer. We can truncate the output buffer by removing this unused space. The amount of truncation
depends on the tier (the virtual register size). For example, if we store 179 (N=179) five bits (W=5) elements in
FL256, we will be able to truncate the output buffer from 160 bytes (allocated) to 128 bytes (truncated), so
in case of FL256, multiples of 32 bytes.

Here is the truncation formula for reference:

```
S = tier_bits / T
rows = ceil(N / S)
truncated_size = ceil(rows * W / T) * tier_bytes
```

## Allocation, alignment and input sizing

The SIMD operations mandate certain alignment and padding of the data. This includes both the input and the
output buffers. For example, in case of the input buffer for the packing operation we may over-read the
input data beyond the useful elements and it needs to be properly sized. For the output data, the encoder
may overwrite adjacent memory if not properly sized. Similarly, during unpacking we must carefully size the
output buffer and ensure that the input buffer is aligned and padded by zeros.

The exact amount of alignment and padding depends on the FL tier. For example FL256 operates on 32 byte aligned
data and it requires the tail padding to be 32 bytes as well. The other tiers have different (smaller)
requirements. The natural truncation point of the output also depends on the tier.

The [fastlanes/fastlanes.h](./fastlanes.h) header provides functions to size, pack, and unpack data.
The right internal tier is automatically selected based on the input parameters.

The 'fastlanes/fastlanes.h' header has functions to determine the required alignment and allocation
size based on the (N, W, T) triple. These functions are:

``` C
/*
 * Tells how many bytes to allocate for the pack/unpack buffer based on the (N, W, T) triple.
 * This function determines the right FL tier based on the input parameters and returns the required
 * allocation size based on the parameters.
 */
size_t fl_required_bytes(uint32_t n, uint8_t w, fl_elem_width_t t);

/*
 * The function determines the result size in the output buffer. With this we can determine the
 * part of the output buffer that is actually used. Remember, this is needed because the FL tiers
 * operate on a fixed number of elements which is larger or equal to the actual number of elements
 * we pack (for SIMD efficiency).
 */
size_t fl_result_bytes(uint32_t n, uint8_t w, fl_elem_width_t t);

/*
 * The fl_alignment function returns the required alignment for the buffers based on the
 * (N, T) pair. The same alignment is required for both the input and output buffers.
 *
 * Note: The same alignment is optimal for both buffers. For the pack/unpack buffer it's a hard
 *  correctness requirement (UB if violated). For the input buffer it's a performance recommendation:
 *  the kernel works correctly with natural alignment but pays a penalty on hot decode loops.
 */
size_t fl_alignment(uint32_t n, fl_elem_width_t t);

/*
 * The number of input elements of the `values` to be packed. The library expects
 * this many elements and it is the caller's responsibility to provide it. As the
 * library uses SIMD instructions, it needs to operate on fixed sized element chunks.
 */
uint32_t fl_input_count(uint32_t n, fl_elem_width_t t);

/*
 * The number of bytes to read from the input buffer. This is a convenience alternative
 * to multiplying the `fl_input_count` by the element width and calculate the byte size.
 */
size_t fl_input_bytes(uint32_t n, fl_elem_width_t t);
```

| Buffer         | Min size                        | Alignment                                   |
|----------------|---------------------------------|---------------------------------------------|
| values (input) | `fl_input_count` elements       | `fl_alignment` recommended, minimum natural |
| packed         | `fl_required_bytes` bytes       | `fl_alignment` mandatory                    |
| unpack output  | `fl_input_count` elements       | `fl_alignment` recommended, minimum natural |

## Unpack tail padding

The input of the unpack functions must be padded with zeros up to `fl_required_bytes`. This
is because the SIMD instructions will read beyond the actual packed data, up to the allocated size.

## Interface contract

This is a concise recap of the previous sections.

- the buffer for the input values MUST have at least `fl_input_count` elements (not bytes) and it SHOULD be aligned to `fl_alignment` bytes, and it MUST be aligned naturally
- the packed buffer (pack output / unpack input) MUST be aligned to `fl_alignment` bytes
- the output of the unpack function holding the unpacked values SHOULD be aligned to `fl_alignment` bytes, and MUST be aligned naturally
- the size of the pack/unpack buffer must be `fl_required_bytes` bytes
- the unpack input buffer must be padded with zeros up to `fl_required_bytes` bytes
- `fl_pack` and `fl_pack_ffor` return the packed size in bytes, which is the same as `fl_result_bytes`
- the input size `n` MUST NOT exceed 256

## Simple packing vs FFOR packing

The simple packing function `fl_pack` packs the integers as they are. The caller is expected
to determine the bit-width of the integers and pass it as an argument. Similarly, the unpacking
function `fl_unpack` unpacks the integers as they are and the caller is expected to know the
bit-width of the integers to unpack.

The FFOR (Frame of Reference) packing function `fl_pack_ffor` packs the integers based on a
`base` value. The base value is subtracted from the integers before packing, so the packed integers
are the difference between the original integers and the base value. This allows to pack integers
with smaller bit-width. The `w` parameter in the FFOR packing is the bit-width of the integers
_after_ subtracting the base value. This is the caller's responsibility to determine.

`base` must fit the same input bit-width as the values, otherwise it will be truncated and
the behavior is undefined.

The FFOR functions don't add much functionality on top of the simple packing functions, as
the caller could do the same subtraction and then call the simple packing functions. The FFOR
functions does these subtractions in the same loop as the packing, so it is more efficient
than doing the subtraction in a separate loop and then calling the simple packing functions.
The same applies to the unpacking functions.

The pack function masks the input based on the `w` parameter, so it is essential to pass the
correct value, otherwise the input will be truncated.

The FFOR pack function does not store the `base` in the results and the unpack function
expects the caller to pass the `base` value as a parameter. As a consequence, it is the
caller's responsibility to store the `base` value, similar to `w` and `n`.

## Batch sizing

The maximum number of elements the FL tiers can handle is 256 elements (FL256 tier). It is the
caller's responsibility to manage the inputs and potentially split it such that it fits. This
is a deliberate decision, because in practice, using the same bitwidth for more than 256 elements
is rarely economical because of the outliers.

When passing less than 256 elements, the library chooses the best tier based on performance
and packing efficiency.

## Signed integers and input validation

The library treats all inputs as unsigned integers. It is the caller's responsibility to
make sure that the inputs are non-negative, or in case of FFOR packing, the base value
is chosen such that the subtracted values are non-negative. The library does not validate
the inputs, so if the caller passes negative values, the behavior is undefined.

Similarly, the library doesn't validate the 'W' parameter to be <= T, or N <= 256, so
it is the caller's responsibility to ensure that these conditions are met.

## The W=0 special case

There is one case that can happen during a real compressor, when the bit-width of the
integers is 0. This happens when all the integers are the same. Both unpack functions fill
only the first N positions (not up to `fl_input_count`). The FFOR variant fills with the base
value; the simple variant fills with zeros.

The pack functions in both cases will return 0, and they handle this special case as no-op,
and with that, the functions ignore the buffers and passing NULLs as buffers is safe. The
unpack functions need a valid result buffer. Also the below holds:

``` C
fl_required_bytes(n, 0, t) == fl_result_bytes(n, 0, t) == 0
```

## Test and usage examples

Example usage is provided in the `test_fastlanes.c` file. The purpose of this file is to
demonstrate the usage of the library.
