// Switching the current user must go through the helpers in src/utils.c.
//
// Use `ts_security_context_enter()` instead of calling
// `SetUserIdAndSecContext()` directly.
//
// Only `ts_relation_define()` may switch to the catalog owner, so no other
// function may read `owner_uid` of `CatalogDatabaseInfo`.

@@
identifier fn != { ts_security_context_enter, ts_security_context_exit };
expression E1, E2;
@@
fn(...)
{
<...
- SetUserIdAndSecContext(E1, E2);
+ /* use ts_security_context_enter() to switch user */
+ SetUserIdAndSecContext(E1, E2);
...>
}

@@
identifier fn != { ts_relation_define, catalog_database_info_init };
expression E;
@@
fn(...)
{
<...
- E->owner_uid
+ /* only ts_relation_define() may switch to the catalog owner */
+ E->owner_uid
...>
}
