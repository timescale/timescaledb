# TimescaleDB Optimizations

TimescaleDB has a number of optimizations to improve performance of
query execution.

- [Skip scan](skip_scan/README.md) optimize queries involving `DISTINCT`
- [Gapfill](gapfill/README.md) supports gapfilling time-series using LOCF and interpolation
