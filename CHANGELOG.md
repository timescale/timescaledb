# TimescaleDB Changelog

**Please note: When updating your database, you should connect using
`psql` with the `-X` flag to prevent any `.psqlrc` commands from
accidentally triggering the load of a previous DB version.**

## 2.21.3 (2025-08-12)

This release contains performance improvements and bug fixes since the 2.21.2 release. We recommend that you upgrade at the next available opportunity.

**Highlighted features in TimescaleDB v2.21.3**
* 

**Features**

**Bugfixes**
* 

**GUCs**
* 

**Thanks**

* [c3f930f] Add `time_bucket` functions
* [b128ac2] Fix bug with `INSERT INTO...SELECT`
* [e20edf8] Add better error checking for index creation.
* [72f754a] use PostgreSQL's own `hash_any` function as default partfunc (thanks @robin900)
* [39f4c0f] Remove sample data instructions and point to docs site
* [9015314] Revised the `get_general_index_definition` function to handle cases where indexes have definitions other than just `CREATE INDEX` (thanks @bricklen)