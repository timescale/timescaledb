@r1@
expression mcxt;
identifier oldmcxt;
@@
 {
-MemoryContext oldmcxt;
...
-oldmcxt = MemoryContextSwitchTo(mcxt);
+TS_WITH_MEMORY_CONTEXT(mcxt, WRAP(
...
-MemoryContextSwitchTo(oldmcxt);
+));
 ...
 }

@r2@
expression mcxt;
identifier oldmcxt;
@@
 {
-MemoryContext oldmcxt = MemoryContextSwitchTo(mcxt);
+/* Replace the WRAP macro with braces */
+TS_WITH_MEMORY_CONTEXT(mcxt, WRAP(
 ...
-MemoryContextSwitchTo(oldmcxt);
+));
+/* End of WRAP macro */
 ...
 }
