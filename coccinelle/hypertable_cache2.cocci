// find use after free bugs due to premature cache releases
// this will find bugs of the following form:
// ht = ts_hypertable_cache_get_entry(cache)
// dim = hyperspace_get_open_dimension(ht->space, ...)
// ts_cache_release(cache)
// usage of dim after cache release
@ cache_get @
expression cache, htid, relid, rv, schema, table, flags;
identifier dim, ht;
position p;
@@
(
ht = ts_hypertable_cache_get_entry(cache,...)
|
ht = ts_hypertable_cache_get_cache_and_entry(relid, flags, &cache)
|
ht = ts_hypertable_cache_get_entry_rv(cache, rv)
|
ht = ts_hypertable_cache_get_entry_with_table(cache, relid, schema, table, flags)
|
ht = ts_hypertable_cache_get_entry_by_id(cache, htid)
)
...
(
dim = hyperspace_get_open_dimension(ht->space, ...)
|
dim = ts_hyperspace_get_dimension(ht->space, ...)
|
dim = ts_hyperspace_get_dimension_by_name(ht->space, ...)
|
dim = ts_hyperspace_get_mutable_dimension_by_name(ht->space, ...)
|
dim = ts_hyperspace_get_dimension_by_id(ht->space, ...)
)
...
ts_cache_release(cache)@p;
...
dim
@ m1 depends on cache_get @
expression cache_get.cache;
position cache_get.p;
@@
ts_cache_release(cache)@p;
PG_RETURN_DATUM
@ m2 depends on cache_get && !m1 @
expression cache_get.cache;
identifier cache_get.dim;
position cache_get.p;
@@
- ts_cache_release(cache)@p;
...
+ /* dim used after calling ts_cache_release */
dim
