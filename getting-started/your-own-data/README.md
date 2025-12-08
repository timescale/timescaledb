# Bring Your Own Data to TimescaleDB

**Status: Placeholder - To be implemented**

This guide will help you bring your own time-series data to TimescaleDB.

## What You'll Learn

- How to design optimal schemas for time-series data
- Choosing partition_column, segmentby, and orderby parameters
- Loading data efficiently with direct to columnstore
- Migration patterns from vanilla PostgreSQL
- Best practices for indexing and query optimization

## Planned Guides

- **README.md** - Overview and getting started (this file)
- **schema-design-guide.md** - How to design schemas (to be added)
- **migration-guide.md** - Migrating from PostgreSQL (to be added)

## Before You Start

Try our examples first to understand TimescaleDB patterns:
- [Quick Start Guide](../quickstart.md)
- [Examples](../examples/)

## Quick Tips

1. **Identify your time column** - Every hypertable needs a time-based partition column
2. **Choose segmentby wisely** - Pick low-to-medium cardinality columns frequently used in queries
3. **Enable columnstore** - Use `tsdb.enable_columnstore=true` for analytical workloads
4. **Load efficiently** - Use direct to columnstore for bulk loads: `SET timescaledb.enable_direct_compress_copy = on;`

More detailed guides coming soon.
