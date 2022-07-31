// find hash_create calls without HASH_CONTEXT flag
//
// hash_create calls without HASH_CONTEXT flag will create the hash table in
// TopMemoryContext which can introduce memory leaks if not intended. We want
// to be explicit about the memory context our hash tables live in so we enforce
// usage of the flag.
@ hash_create @
expression res;
position p;
@@

res@p = hash_create(...);

@safelist@
expression res;
expression arg1, arg2, arg3;
expression w1, w2;
expression flags;
position hash_create.p;
@@
(
res@p = hash_create(arg1,arg2,arg3, w1 | HASH_CONTEXT | w2);
|
res@p = hash_create(arg1,arg2,arg3, w1 | HASH_CONTEXT);
|
res@p = hash_create(arg1,arg2,arg3, HASH_CONTEXT | w2 );
|
Assert(flags & HASH_CONTEXT);
res@p = hash_create(arg1,arg2,arg3, flags);
)
@ depends on !safelist @
expression res;
position hash_create.p;
@@

+ /* hash_create without HASH_CONTEXT flag */
  res@p = hash_create(...);

