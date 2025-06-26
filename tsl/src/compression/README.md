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

### Bool Compressor

The bool compressor is a simple compression algorithm that stores boolean values
using the simple8b_rle algorithm only, without any additional processing. During
decompression it decompresses the data and stores it in memory as a bitmap. The
row based iterators then walk through the bitmap. The bool compressor differs from
the other compressors in that it stores the last non-value as a place holder for
the null values. This is done to make vectorization easier.

### UUID Compressor

The uuid compressor is a compression algorithm that aims at storing UUID v7 values
compressed as much as possible by taking advantage of the timestamp values being
present in the UUID.

The first part of the UUID where the timestamp resides is stored using the delta-delta
algorithm. The second part of the UUID is stored without compression, as a sequence of
uint64 values.

The algorithm checks the cardinality of the values in the compressed batch and based on
the cardinality it decides wether it is worth to recompress the batch using the dictionary
compression algorithm. In that case it recompresses and stores the UUIDs as a dictionary.

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

# Picking default for `segment_by` and `order_by`.

We have two functions to determine the columns for `timescaledb.compress_segmentby` and `timescaledb.compress_orderby` . These functions can be called
by the UI to give good defaults. They can also be called internally when a hypertable has compression enabled
but no values are provided to specify these options.

## `_timescaledb_functions.get_segmentby_defaults`

This function determines a segment-by column to use. It returns a JSONB with the following top-level keys:
- columns: an array of column names that should be used for segment by. Right now it always returns a single column.
- confidence: a number between 0 and 10 (most confident) indicating how sure we are.
- message: a message that should be shown to the user to evaluate the result.

The intuition is as follows:

we use 3 criterias:
- We want to pick an "important" column for querying. We measure "importance", in terms of how early the column comes in an index (i.e. leading columns are very important, others less so). If there are no indexes, all columns will be considered if statistics are populated
- The column has many rows for the same column value so that the segments will have many rows. We establish that a column will have many values if (i) it is not a dimension and (ii) either statistics tell us so (via `stadistinct` > 1) or, if statistics aren't populated, we check whether the column is a generated identity or serial column.
- When we have multiple qualifying rows, we select the column where rows are spread most evenly across the distinct values. 

Naturally, statistics give us more confidence that the column has enough rows per segment. In this case we break ties by preferring columns from unique indexes. Otherwise, we prefer columns from non-unique indexes (we are less likely to run into a unique column there).

Thus, our preference is based on the whether the column is from a unique or regular index as well as the position of the column in the index. Given these preferences, we think ties happened rarely but will be resolved arbitrarily.

One final point: a number of tables don't have any indexed columns that aren't dimensions or serial columns. In this case, we have medium confidence that an empty segment by is correct.

## `_timescaledb_functions.get_orderby_defaults`

This function determines which order by columns to use. It returns a JSONB with the following top-level keys:

- clauses: an array of column names and sort order key words that shold be used for order by.
- confidence: a number between 0 and 10 (most confident) indicating how sure we are.
- message: a message that should be shown to the user to evaluate the result.

The order by is built in three steps:
1) Use the column order in a unique index (removing the segment_by columns).
2) Add any dimension columns
3) Add the first attribute of any other index (to establish min-max filters on those columns).

All non-dimension columns are returned without a sort specifier (thus using `ASC` as default). The dimension columns use `DESC`.