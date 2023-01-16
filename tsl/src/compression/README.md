# Compression Algorithms

This is a collection of compression algorithms that are used to compress data of different types.
The algorithms are optimized for time-series use-cases; many of them assume that adjacent rows will have "similar" values.

## API

Each compression algorithm the API is divided into two parts: a _compressor_ and a _decompression iterator_. The compressor
is used to compress new data.

- `<algorithm name>_compressor_alloc` - creates the compressor
- `<algorithm_name>_compressor_append_null` - appends a null
- `<algorithm_name>_compressor_append_value` - appends a non-null value
- `<agorithm_name>_compressor_finish` - finalizes the compression and returns the compressed data

Data can be read back out using the decompression iterator. An iterator can operate backwards or forwards.
There is no random access. The api is

- `<algorithm_name>_decompression_iterator_from_datum_<forward|reverse>` - create a new DatumIterator in the forward or reverse direction.
- a DatumIterator has a function pointer called `try_next` that returns the next `DecompressResult`.

A `DecompressResult` can either be a decompressed value datum, null, or a done marker to indicate that the iterator is done.

Each decompression algorithm also contains send and recv function to get the external binary representations.

`CompressionAlgorithmDefinition` is a structure that defines function pointers to get forward and reverse iterators
as well as send and recv functions. The `definitions` array in  `compression.c` contains a `CompressionAlgorithmDefinition`
for each compression algorithm.

## Base algorithms

The `simple8b rle` algorithm is a building block for many of the compression algorithms.
It compresses a series of `uint64` values. It compresses the data by packing the values into the least
amount of bits necessary for the magnitude of the int values, using run-length-encoding for large numbers of repeated values,
A complete description is in the header file. Note that this is a header-only implementation as performance
is paramount here as it is used as a primitive in all the other compression algorithms.

## Compression Algorithms

### DeltaDelta

for each integer, it takes the delta-of-deltas with the pervious integer,
zigzag encodes this deltadelta, then finally simple8b_rle encodes this
zigzagged result. This algorithm performs very well when the magnitude of the
delta between adjacent values tends not to vary much, and is optimal for
fixed rate-of-change.


### Gorilla

`gorilla` encodes floats using the Facebook gorilla algorithm. It stores the
compressed xors of adjacent values. It is one of the few simple algorithms
that compresses floating point numbers reasonably well.

### Dictionary

The dictionary mechanism stores data in two parts: a "dictionary" storing
each unique value in the dataset (stored as an array, see below) and
simple8b_rle compressed list of indexes into the dictionary, ordered by row.
This scheme can store any type of data, but will only be a space improvement
if the data set is of relatively low cardinality.

### Array

The array "compression" method simply stores the data in an array-like
structure and does not actually compress it (though TOAST-based compression
can be applied on top). It is the compression mechanism used when no other
compression mechanism works. It can store any type of data.

# Merging chunks while compressing #

## Setup ## 

Chunks will be merged during compression if we specify the `compress_chunk_time_interval` parameter.
This value will be used to merge chunks adjacent on the time dimension if possible. This allows usage
of smaller chunk intervals which are rolled into bigger compressed chunks. 

## Operation ##

Compression itself is altered by changing the destination compressed chunk from a newly created one to
an already existing chunk which satisfies the necessary requirements (is adjacent to the compressed chunk
and chunk interval can be increased not to go over compress chunk time interval).

After compression completes, catalog is updated by dropping the compressed chunk and increasing the chunk
interval of the adjacent chunk to include its time dimension slice. Chunk constraints are updated as necessary.

## Compression setup where time dimension is not the first column on order by ## 

When merging such chunks, due to the nature of sequence number ordering, we will inherently be left with
chunks where the sequence numbers are not correctly ordered. In order to mitigate this issue, chunks are 
recompressed immediately. This has obvious performance implications which might make merging chunks
not optimal for certain setups.
