# Creating a downgrade script

# Notes

## Handling views

Only the last version of the view should be installed at each
point. Using `CREATE OR REPLACE VIEW` does not allow the names of the
columns to change, they have to be the same or an error will be given.

## Handling functions

Functions that use the shared library need to be replaced completely.
The following query will get a list of functions that are in the
specific library that you want to remove, in this case
`timescaledb-2.2.0.so`. Note that the language check is redundant
since `probin` is NULL if it is not a C function, but this makes it
very explicit that we only list C functions.

```sql
SELECT proname, lanname, prosrc, probin
  FROM pg_proc JOIN pg_language lang ON lang.oid = prolang
 WHERE lanname = 'c' AND probin = '$libdir/timescaledb-2.2.0.so';
```

To install the old functions with the correct library, it is necessary
to check out
