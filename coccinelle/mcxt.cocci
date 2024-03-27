// find MemoryContextSwitchTo missing a context switch back
@ MemoryContextSwitch @
local idexpression oldctx;
position p;
@@
oldctx@p = MemoryContextSwitchTo(...);

@safelist@
local idexpression MemoryContextSwitch.oldctx;
position MemoryContextSwitch.p;
@@
oldctx@p = MemoryContextSwitchTo(...);
...
MemoryContextSwitchTo(oldctx)

@depends on !safelist@
expression oldctx;
position MemoryContextSwitch.p;
@@
+ /* MemoryContextSwitch missing context switch back */
  oldctx@p = MemoryContextSwitchTo(...);
