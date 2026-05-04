## General principles for statements in update/downgrade scripts

1. The `search_path` for these scripts will be locked down to
  `pg_catalog, pg_temp`. Locking down `search_path` happens in
  `header.sql`. Therefore all object references need to be fully
  qualified unless they reference objects from `pg_catalog`.
  Use `@extschema@` to refer to the target schema of the installation
  (resolves to `public` by default).
2. All functions should have explicit `search_path`. Setting explicit
  `search_path` will prevent SQL function inlining for functions and
  transaction control for procedures so for some functions/procedures
  it is acceptable to not have explicit `search_path`. Special care
  needs to be taken with those functions/procedures by either setting
  `search_path` in function body or having only fully qualified object
  references including operators.
3. When generating the install scripts `CREATE OR REPLACE` will be
  changed to `CREATE` to prevent users from precreating extension
  objects. Since we need `CREATE OR REPLACE` for update scripts and
  we don't want to maintain two versions of the sql files containing
  the function definitions we use `CREATE OR REPLACE` in those.
4. When adding or removing columns from catalog tables the tables
  need to be completely rebuilt as the C code relies on the physical
  layout of the tables being the same and must not have dropped
  columns. The catalog tables are mapped to C structs which would break
  with dropped columns.

## Extension updates

This directory contains "modfiles" (SQL scripts) with modifications
that are applied when updating from one version of the extension to
another.

The actual update scripts are compiled from modfiles by concatenating
them with the current source code (which should come at the end of the
resulting update script). Update scripts can "jump" several versions
by using multiple modfiles in order. There are two types of modfiles:

* Transition modfiles named `<from>-<to>.sql`, where `from` and `to`
  indicate the (adjacent) versions transitioning between. Transition
  modfiles are concatenated to form the lineage from an origin version
  to any later version.
* Origin modfiles named <version>.sql, which are included only in
  update scripts that origin at the particular version given in the
  name. So, for instance, `0.7.0.sql` is only included in the script
  moving from `0.7.0` to the current version, but not in, e.g., the
  update script for `0.4.0` to the current version. These files
  typically contain fixes for bugs that are specific to the origin
  version, but are no longer present in the transition modfiles.

Notes on post_update.sql
The scripts in post_update.sql are executed as part of the `ALTER
EXTENSION` stmt.

Note that modfiles that contain no changes need not exist as a
file. Transition modfiles must, however, be listed in the
`CMakeLists.txt` file in the parent directory for an update script to
be built for that version.

## Extension downgrades

You can enable the generation of a downgrade file by setting
`GENERATE_DOWNGRADE_SCRIPT` to `ON`, for example:

```
./bootstrap -DGENERATE_DOWNGRADE_SCRIPT=ON
```

To support downgrades to previous versions of the extension, it is
necessary to execute CMake from a Git repository since the generation
of a downgrade script requires access to the previous version files
that are used to generate an update script. In addition, we only
generate a downgrade script to the immediate preceeding version and
not to any other preceeding versions.

The source and target versions are found in be found in the file
`version.config` file in the root of the source tree, where `version`
is the source version and `previous_version` is the target
version. Note that we have a separate field for the downgrade.

A downgrade file consists of:
- A prolog that is retrieved from the target version.
- A version-specific piece of code that exists on the source version.
- An epilog that is retrieved from the target version.

The prolog consists of the files mentioned in the `PRE_UPDATE_FILES`
variable in the target version of `cmake/ScriptFiles.cmake`.

The version-specific code is found in the source version of the file
`sql/updates/reverse-dev.sql`.

The epilog consists of the files in variables `SOURCE_FILES` and
`POST_UPDATE_FILES`.

Note that, in contrast to update scripts, downgrade scripts are not
built by composing several downgrade scripts into a more extensive
downgrade script. We only build a downgrade script to the immediate
preceeding version. To downgrade multiple versions multiple downgrades
need to be chained.

