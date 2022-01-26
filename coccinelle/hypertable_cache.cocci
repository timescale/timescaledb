// find hypertable uses after cache release
@ cache_get @
expression cache, ht, htid, relid, rv, schema, table, flags;
position p;
@@
(
ht = ts_hypertable_cache_get_entry(cache,...);
|
ht = ts_hypertable_cache_get_cache_and_entry(relid, flags, &cache);
|
ht = ts_hypertable_cache_get_entry_rv(cache, rv);
|
ht = ts_hypertable_cache_get_entry_with_table(cache, relid, schema, table, flags);
|
ht = ts_hypertable_cache_get_entry_by_id(cache, htid);
)
...
ts_cache_release(cache)@p;
...
ht
@ safelist1 depends on cache_get @
expression cache_get.cache;
position cache_get.p;
@@
ts_cache_release(cache)@p;
(
PG_RETURN_DATUM
|
aclcheck_error(...)
|
ereport(ERROR,...)
)
//
// if variable gets reassigned it's not use after free
//
@ safelist2 depends on cache_get && !safelist1 @
expression cache_get.cache;
expression cache_get.ht;
position cache_get.p;
@@
ts_cache_release(cache)@p;
...
(
ht = ts_hypertable_cache_get_entry(...);
|
ht = ts_hypertable_cache_get_cache_and_entry(...);
)
// print context of matched use after free
@ match depends on cache_get && !safelist1 && !safelist2 @
expression cache_get.cache;
expression cache_get.ht;
position cache_get.p;
position m;
@@
* ts_cache_release(cache)@p;
...
* ht@m
