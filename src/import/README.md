Sometimes we need to call some Postgres code in a way that is not accessible through its public interface.
Often, we resort to making a copy of this Postgres code in our tree. The `src/import` directories
are where most of this copied code lives. We actually use two import directories, one in the Apache and 
one in the TSL part of the extension, depending on where the copied function is needed.
Below you will find some guidelines on how to import a function.

### How to Copy Functions from Postgres

* Prefer not to copy functions at all, because this creates maintenance burden.

* If you have to call a static function from Postgres unmodified, copy it here.

* If you have to slightly modify the logic of a public Postgres function, copy it here and
rename it. Having two functions named the same but doing different things creates confusion.

* The `.c` source file that contains a copy must have the same relative path to `src/import` as the Postgres
`.c` file to the `src/backend`.

* When copying multiple functions from the same file, they must have the same
relative order as in the Postgres file.

* The header files in Postgres have different layout from the source files. Since
they mostly contain just function declarations, it's not necessary to follow the
precise Postgres names or relative paths for header files.

* Do not introduce any formatting or linter differences to the copies. The files in this directory
are excluded from additional linter checks at the CMake level.

* You can mechanically compare this directory to the Postgres source by using a
diff tool of your choice, for example, `meld pg/src/backend ts/src/import`. Follow the rules above to
keep this comparison approach viable.
