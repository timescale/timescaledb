# compact_chunk

Merges overlapping compressed batches within a chunk. Only touches batches
that need fixing — correctly ordered batches are left as-is.

Overlap detection reads the firstlast sparse metadata, which stores the exact
orderby values of each batch's first and last rows. Two adjacent batches overlap
when the current batch's first row sorts before the previous batch's last row.
Because the metadata holds the real boundary rows for every orderby column, no
decompression is needed, even for multi-column orderby.

## How It Works

```
 Phase 1: FIND          Phase 2: RECOMPRESS       Phase 3: VERIFY
 ┌──────────────┐       ┌──────────────────┐      ┌────────────────┐
 │ Index scan   │──────▶│ Decompress+merge │─────▶│ Re-scan with   │
 │ Stop at      │       │ overlapping      │      │ fresh snapshot │
 │ first issue  │       │ batches, continue│      │ Clear UNORDERED│
 └──────────────┘       │ scanning for more│      │ if clean       │
                        └──────────────────┘      └────────────────┘
```

## Handling specific compression and batch configurations

### 1. No overlaps (no-op)

```
 ┌────────┐  ┌────────┐  ┌────────┐       ┌────────┐  ┌────────┐  ┌────────┐
 │ 1..100 │  │101..200│  │201..300│ ───▶  │ 1..100 │  │101..200│  │201..300│
 └────────┘  └────────┘  └────────┘       └────────┘  └────────┘  └────────┘
                                           (unchanged, UNORDERED cleared)
```

### 2. Overlapping batches

```
 ┌──────────┐                              ┌───────┐┌───────┐┌───────┐┌──┐┌────────┐
 │  1..100  │                              │ 1..50 ││51..100││101    ││  ││201..300│
 └──────────┘                              │       ││       ││ ..150 ││… │└────────┘
    ┌──────────┐               ───▶        └───────┘└───────┘└───────┘└──┘
    │ 50..150  │                           ◄────── merged + re-sorted ──►  ◄ kept ►
    └──────────┘
       ┌────────┐
       │201..300│
       └────────┘
```

### 3. Segmentby — independent per segment

```
 d1: ┌──────┐ ┌───────┐       d1: ┌──────────────┐
     │1..100│ │50..200│  ──▶      │ 1..200 merged│
     └──────┘ └───────┘           └──────────────┘
 d2: ┌──────┐ ┌───────┐       d2: ┌──────────────┐
     │1..100│ │50..200│  ──▶      │ 1..200 merged│
     └──────┘ └───────┘           └──────────────┘
```

### 4. DESC orderby

```
 orderby='time DESC'       max◄────────────────────►min

 ┌──────────┐                   ┌──────────────────┐
 │ 200..100 │                   │ 200..........100 │
 └──────────┘              ──▶  │     merged       │
    ┌──────────┐                └──────────────────┘
    │ 150..50  │
    └──────────┘
```

### 5. Multi-column orderby — boundary tie resolution

When col1 first/last tie, compare the secondary columns straight from the
first/last metadata — no decompression.

```
 orderby='device,time'     Both batches tie on device (first=d2, last=d2)

 Batch 1 last row:  (d2, 08:20)     ◄─ from last metadata
 Batch 2 first row: (d2, 08:21)     ◄─ from first metadata
                    08:20 < 08:21 → no overlap ✓

 Batch 1 last row:  (d2, 08:20)
 Batch 2 first row: (d2, 08:11)
                    08:20 > 08:11 → OVERLAP → merge
```

### 6. Mixed-null batch overlaps a neighbor

A batch with both NULL and non-NULL values in the first orderby column has a
NULL boundary row. With NULLS LAST its last row is NULL, which sorts after a
following non-null batch — so the two batches overlap. The merge re-sorts the
rows and the NULLs settle at the end (NULLS LAST). (A mixed-null batch with no
neighbor to overlap is already ordered and is left as-is.)

```
 orderby='value NULLS LAST'   last row of batch 1 is NULL, sorts after batch 2

 ┌─────────────────────┐  ┌──────────┐      ┌────────────────────────────────┐
 │ 1001..1800, NULL×200│  │1801..2800│ ──▶  │ 1001..2800 re-sorted, NULL×200 │
 └─────────────────────┘  └──────────┘      └────────────────────────────────┘
  first=1001, last=NULL ──▶ overlap          merged, NULLs at end (NULLS LAST)
```

### 7. Overlap merge preserving NULLs

When overlapping batches with nullable first orderby are merged, the re-sort
keeps the NULL rows in their correct ordered position.

```
 orderby='value NULLS LAST'

 ┌──────────────────┐                    ┌──────────────────────────┐
 │ 1..400, NULL×100 │               ──▶  │ 1..699 re-sorted, NULL×100│
 └──────────────────┘                    └──────────────────────────┘
    ┌──────────┐        overlap           NULLs kept at end (NULLS LAST)
    │ 200..699 │        on 200..400
    └──────────┘
```

### 8. Secondary column NULLs at boundary tie

The boundary comparison uses the column's sort order, which already places NULLs
per its NULLS FIRST/LAST setting, so a NULL boundary value compares like any
other value.

```
 orderby='time, value NULLS LAST'

 Batch 1 last row:  (08:20, NULL)         CORRECT: NULL with NULLS LAST
 Batch 2 first row: (08:20, 1001)         means NULL > 1001 → OVERLAP → merge
                                          

 ┌──────────────────┐  ┌──────────────────┐       ┌──────────────────────────┐
 │ ..., (08:20,NULL)│  │(08:20,1001), ... │  ──▶  │ merged + correctly sorted│
 └──────────────────┘  └──────────────────┘       └──────────────────────────┘
```
