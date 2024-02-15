//
// find missing `AttrNumberGetAttrOffset` usage
//
// Postgres has `AttrNumberGetAttrOffset()` macro to proper access Datum array members
// (see https://github.com/postgres/postgres/blob/master/src/include/access/attnum.h).
//
// For example:
//    `datum[attrno - 1]` should be `datum[AttrNumberGetAttrOffset(attrno)]`
//

@@
typedef Datum;
expression attrno;
Datum [] datum;
@@

- datum[attrno - 1]
+ /* use AttrNumberGetAttrOffset() for acessing Datum array members */
+ datum[AttrNumberGetAttrOffset(attrno)]

@@
typedef bool;
expression attrno;
bool [] isnull;
@@

- isnull[attrno - 1]
+ /* use AttrNumberGetAttrOffset() for acessing bool array members */
+ isnull[AttrNumberGetAttrOffset(attrno)]

