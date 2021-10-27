// find heap_form_tuple with missing heap_freetuple calls
@ heap_form_tuple @
identifier tuple;
position p;
@@

tuple@p = heap_form_tuple(...);

@safelist@
expression tuple;
position heap_form_tuple.p;
@@

tuple@p = heap_form_tuple(...);
...
(
return tuple;
|
heap_freetuple(tuple)
|
HeapTupleGetDatum(tuple)
)
@depends on !safelist@
expression tuple;
position heap_form_tuple.p;
@@

+ /* heap_form_tuple with missing heap_freetuple call */
  tuple@p = heap_form_tuple(...);
