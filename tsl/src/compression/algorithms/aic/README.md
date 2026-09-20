# Adaptive Integer Compressor (AIC)

This compressor works on maximum 64bit integers. The input is divided
into blocks and the compressor chooses an encoding method for each
block separately. This allows similar flexibility to what the simple8b
compressor has, when it can mix run length encoded and individual
integers within the same stream.

The design goals of the Adaptive Integer Compressor (AIC) are partly
defined as a comparison to its predecessor, the deltadelta (DD)
algorithm, and partly to utilize new compression methods.
The design goals are:

- better decompression speed than DD
- utilize vectorizable algorithms like PFOR, DFOR, FastLanes
- better handle a wide range of data distributions than DD
- better tolerate small batch sizes than DD
- better compression speed than DD

The AIC compressor heavily utilizes the fastlanes library
and this determines the maximum block size (256 elements) for fixed
length blocks. The `RLE` and `DELTA_RLE` blocks' repetition counts are only
limited by the maximum element count in the batch (`GLOBAL_MAX_ROWS_PER_COMPRESSION`).

The compressor separates the NULLs and the valid values into two separate
streams. When there are no NULLs, the NULL stream (called validity) is
completely omitted. The valid value stream only contains valid values,
so when NULLs are present, the NULL stream has a bit for every input,
while the value stream only contains the non-NULL entries.

During decompression, the decompressor takes the NULL (validity) and the
value stream and merges them back via back-scatter.

## SIMD operations

I paid particular attention to test, measure, disassemble and tune
the algorithms so they allow the compiler to generate vectorized
instructions without using intrinsics or assembly instructions. This
design is heavily inspired by the FastLanes paper/library.

## Operation modes

In the generic case (NORMAL mode), the compressor analyzes the content
of the current block when it is full (or end of stream) and decides
what encoder to use. There are also two earlier checks at 'append time'
where we are buffering the elements.

1. when AIC_RLE_CHECKPOINT (128) elements are buffered we check if all
values are the same, so the analysis can be skipped, because the best
encoding is RLE

2. at AIC_DELTA_RLE_CHECKPOINT (32) elements we check if the subsequent
values are a monotonically changing series with a constant delta
difference. In this case we can also skip the analysis, because the best
encoding mode is DELTA_RLE

Once we switched the operation mode from NORMAL to one of the other RLE
modes, the mode stays until we see an item that doesn't match the
sequence. When the compressor exits the sequence, it appends the RLE
block, switches back to NORMAL mode and starts buffering the new elements
from an empty buffer.

## NULL storage

Contrary to the values, the NULLs are not stored in blocks. The NULL
storage represents the validity bits of the entire batch (all blocks).

There are two NULL storage formats used and a batch-level global flag
tells which one we use. The two formats are:

- RAW: this is a sequence of bits, each one representing one entry in
the final stream. A bit set means there is a valid value present in
the value stream.

- SUMMARY: this is a two-level structure that saves storage of fully set
(or unset) validity flags at the granularity of 64 flags (bits).
The first level is a set of 8-bit values, where each bit of these values
represents a block of 64 bits in the second level. When the bit is unset, the
second level detail is present; if it is set then the detail block is
skipped. The skipped detail block's content is determined by the ratio
of the total number of NULLs vs values. When NULLs dominate, the 'fill'
value is NULL bits, otherwise it is valid bits.

During compression the encoder builds the SUMMARY format and only keeps
it if it is smaller than the RAW format.

## Common shorthands

Before we further dive into the analysis, let's cover some commonly used
terms and shorthands:

- `N` — number of elements in a block (1..256 for fixed-length blocks)
- `W` — bit width the values are packed at (0..T; W=0 means every packed value is zero)
- `T` — element width in bits (16, 32 or 64), from the column type
- `FOR` — frame of reference: subtract a base, pack the residuals at W bits
- `DFOR` — delta FOR: strided deltas packed with fastlanes, the first stride packed separately as lane bases
- `PFOR` — patched FOR: the values packed at a narrow width, the few outliers patched from a separate exception stream
- `DICT` — dictionary: the distinct values packed once as keys, the elements as indexes into them
- `K` — number of distinct values in a DICT block
- `residuals` — value minus the base, what FOR and PFOR actually pack
- `delta residuals` — strided delta minus the delta residual base, what DFOR packs
- `exceptions` — the high bits of PFOR outliers, stored with their positions
- DFOR `stride length` — the position distance between the values to take the deltas
- `AIC_TY` - the unsigned type the generic function is parametrized with (`uint16`/`uint32`/`uint64`)
- `AIC_STY` - the matching signed type define (`int16`/`int32`/`int64`)

## The encoders

- The `RLE` and `DELTA_RLE` encoders encode simple sequences of
values, where `RLE` has all constant values and `DELTA_RLE` has a
starting base and a repeating offset (step) so these are encoded
as RLE (base, repetition) and DELTA_RLE (base, step, repetition).

- The `FOR` encoder is the simple application of the FastLanes format
where we bitpack the values with an optional base value subtracted.

- The `DFOR` encoder divides the block into parallel delta streams.
Each parallel delta stream starts with a base anchor and a sequence
of delta values. This allows us to utilize parallel decoding of these
delta streams. The base values are stored separately from the delta
stream. They are both encoded with the FastLanes library. The delta
stream is checked for mixed signs and if zigzag encoding is
needed. Both streams can have a base value subtracted to further
reduce the storage size.

- The `DICT` encoder uses an array of keys and a separate array of
indices to store the block data. Both the `keys` and the `indices`
are stored with the FastLanes encoding. Depending on the content
of the `indices` sometimes it is better to store them as a zigzag
encoded delta stream.

- The `PFOR` encoder stores the residuals as a narrow bitpacked array
and a separate array of exception values. The `PFOR` encoder can
handle a limited number of outliers in the `FOR` encoded residuals.
These outliers are partly stored in the packed residuals and the
higher bits beyond the chosen bit width will go to the separate
exception array, together with their positions.

There are cases where more than one encoder can do the job and
it is the analyzer's job to choose the cheapest one. There are
interesting edge cases, like when we have a batch with a few
non-zero values and the rest are zero. Depending on the ratio
we may use the PFOR encoder where the threshold becomes zero
and the real values are all encoded as exceptions. Or if the
cardinality of the non-zeros are small enough, we may use the
DICT encoder. These decisions are all driven by the cost
prediction of the analyzer.

## The analyzer

The task of the analyzer is to decide which encoder to use for
a block of values. Both for the analysis and for the encoding there
are intermediate results that can be reused later. For this reason
the analyzer is designed to be incremental and the sequence of analysis
is organized such that we can skip more expensive tests when previous
results tell that they are not going to win.

The choice of the encoder is based on the best compression ratio
(and not the best decoding speed or other factors). The analyzer
tracks the `best cost` so far and it uses it to guide/limit the
further tests.

The sequence of the analysis checks is determined by the cost and
the wider applicability of the tests. For example, the analysis starts
with determining the min/max and `W` values of the block which is
needed for all encoders. This determines the cost of the `FOR`
encoder. It continues with calculating the `DFOR` cost.

After the `DFOR` cost, we have the first strong signal that tells
us that we may be able to skip the expensive `PFOR` analysis.

This `DFOR` check is followed by the `DICT` check, and it uses the
accumulated `best cost` because at that cost we can tell the maximum
number of `K` that makes sense. When the `DICT` check
finds more than this limit, it stops the probe and saves time.

The `PFOR` check only runs when we didn't rule it out previously,
because that is the most expensive check. It will reuse some of
the intermediate data, but it is still slower than the other checks.

At the end the analyzer returns its choice of encoder and the
staged data that may be useful for the encoder.

## Type dependent implementation

There are multiple places in the compressor, where a type dependent,
generic implementation is used so we take advantage of specializing
for the type. These include:

 - reduced memory allocation
 - allow more efficient SIMD code generation
 - correct type semantics, bounds and arithmetics

The type dependent functions are emitted from inclusion-driven
template headers, following the pattern used in the fastlanes
library (`fastlanes/fastlanes_tier_*_impl.h`). The templates contain
normal C functions, parametrized by preprocessor defines set before
the inclusion point:

 - `AIC_TY` — the unsigned type (`uint16`/`uint32`/`uint64`)
 - `AIC_STY` — the matching signed type (`int16`/`int32`/`int64`)
 - `AIC_FL_T` — the matching `fl_elem_width_t` (only `aic_impl.h`)
