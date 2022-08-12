// find hash_create calls without HASH_CONTEXT flag
//
// hash_create calls without HASH_CONTEXT flag will create the hash table in
// TopMemoryContext which can introduce memory leaks if not intended. We want
// to be explicit about the memory context our hash tables live in so we enforce
// usage of the flag.
@ hash_create @
position p;
@@

hash_create@p(...)

@safelist@
expression arg1, arg2, arg3;
expression w1, w2;
position hash_create.p;
@@
(
hash_create@p(arg1,arg2,arg3, w1 | HASH_CONTEXT | w2)
|
hash_create@p(arg1,arg2,arg3, w1 | HASH_CONTEXT)
|
hash_create@p(arg1,arg2,arg3, HASH_CONTEXT | w2 )
)
@safelist2@
expression res;
expression arg1, arg2, arg3;
expression flags;
position hash_create.p;
@@
Assert(flags & HASH_CONTEXT);
res = hash_create@p(arg1,arg2,arg3, flags);
@ depends on !safelist && !safelist2 @
position hash_create.p;
@@

+ /* hash_create without HASH_CONTEXT flag */
  hash_create@p(...)

