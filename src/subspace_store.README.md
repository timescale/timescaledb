A subspace store allows you to save data associated with a
multidimensional-subspace. We use this to cache per-chunk values, such as
`chunk`s or `chunk_insert_state`. Subspaces are defined conceptually via a
Hypercube (that is a collection of slices -- one for each dimension). Thus, a
subspace is a "rectangular" cutout in a multidimensional space.

that is given a hypertable with (ts Timestamp, i int) with intervals of one hour
and 2 hash-partitions the subspaces could be:

```
     00:00   01:00   02:00   03:00
---|-------|-------|-------|-------|--- - -
 1 |       |       |       |       |
---|-------|-------|-------|-------|--- - -
 2 |       |       |       |       |
---|-------|-------|-------|-------|--- - -
 3 |       |       |       |       |
---|-------|-------|-------|-------|--- - -
```

Each subspace is cached in a tree structure, with each level of the tree
corresponding to a dimension the hypertable is partitioned on, with the first
level always being an open (time) dimension time dimension. Thus, the
aforementioned hypertable will have the following tree:

```
SubspaceStore
    |
    V
SubspaceStoreInternalNode (time)
       | (.vector)
       V
  |  o  | ... | ... | ... |
     |
     V
  DimensionSlice (00:00 - 01:00)
     |
     V
    SubspaceStoreInternalNode (dim 1)
     |
     V
     .
     .
     .
     |
     V
    ChunkInsertState (or other leaf object)
```

Each `SubspaceStoreInternalNode` has a field `descendants` storing a count of
the number of leaf objects for that subtree, which we used to ensure
`SubspaceStore`s don't grow beyond their maximum size. Currently our strategy
when adding to a full `SubspaceStore` is to evict the all elements referenced
from the first entry of the top-most vector. The assumption is that the topmost
vector indexes based on time, and that the first element stores state for those
chunks with the earliest time. If we usually perform operations in time-order,
these are the elements least likely to be reused. This eviction-strategy is the
only reason that the first level of a `SubspaceStore` is always a open (time)
dimension.
