This directory contains scripts to check the codebase for defective
programming patterns, eg use after free or not freeing resources.

Coccinelle is a static code analysis program. It uses a semantic patch
language which resembles unified diff output. The semantic patches may
inline python or ocaml code for more advanced use cases.

A coccinelle patch file consists of multiple blocks.
Example block header:
```
@ name @
Expression var1;
Expression var2;
@@
```
The block header may contain variable definitions. It may also contain
required matches or non-matches in previous blocks.
Examples for blocks with required previous matches:
```
@ b2 depends on name @
@@
@ b3 depends on name && !b2 @
@@
```
Variables inside a block can also reference matches from previous blocks.
```
@ b2 depends on name @
Expression name.var1;
@@
```
var1 inside this block will be the match from the `name` block.

https://github.com/coccinelle/coccinelle
https://coccinelle.gitlabpages.inria.fr/website/
https://coccinelle.gitlabpages.inria.fr/website/docs/main_grammar.html
