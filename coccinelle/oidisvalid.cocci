//
// find comparisons against `InvalidOid`
//
// Postgres has `OidIsValid()` macro to check if a given Oid is valid or not
// (see https://github.com/postgres/postgres/blob/master/src/include/c.h).
//
// This script look for comparisons to `InvalidOid` and recommend the usage of
// `OidIsValid()` instead.
//
// For example:
//    `oid != InvalidOid` should be `OidIsValid(oid)`
//    `oid == InvalidOid` should be `!OidIsValid(oid)`
//
@@
symbol InvalidOid;
expression oid;
@@

- (oid != InvalidOid)
+ /* use OidIsValid() instead of comparing against InvalidOid */
+ OidIsValid(oid)

@@
symbol InvalidOid;
expression oid;
@@

- (InvalidOid != oid)
+ /* use OidIsValid() instead of comparing against InvalidOid */
+ OidIsValid(oid)

@@
symbol InvalidOid;
expression oid;
@@

- (oid == InvalidOid)
+ /* use OidIsValid() instead of comparing against InvalidOid */
+ !OidIsValid(oid)

@@
symbol InvalidOid;
expression oid;
@@

- (InvalidOid == oid)
+ /* use OidIsValid() instead of comparing against InvalidOid */
+ !OidIsValid(oid)

