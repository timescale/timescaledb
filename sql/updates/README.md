## General principles for statements in update/downgrade scripts

1. The `search_path` for these scripts will be locked down to
  `pg_catalog`. Locking down `search_path` happens in `pre-update.sql`.
  Therefore all object references need to be fully qualified unless
  they reference objects from `pg_catalog`. Use `@extschema@` to refer
  to the target schema of the installation (resolves to `public` by
  default).
2. Creating objects must not use IF NOT EXISTS as this will
  introduce privilege escalation vulnerabilities.
3. All functions should have explicit `search_path`. Setting explicit
  `search_path` will prevent SQL function inlining for functions and
  transaction control for procedures so for some functions/procedures
  it is acceptable to not have explicit `search_path`. Special care
  needs to be taken with those functions/procedures by either setting
  `search_path` in function body or having only fully qualified object
  references including operators.
4. When generating the install scripts `CREATE OR REPLACE` will be
  changed to `CREATE` to prevent users from precreating extension
  objects. Since we need `CREATE OR REPLACE` for update scripts and
  we don't want to maintain two versions of the sql files containing
  the function definitions we use `CREATE OR REPLACE` in those.
5. Any object added in a new version needs to have an equivalent
  `CREATE` statement in the update script without `OR REPLACE` to
  prevent precreation of the object.
6. The creation of new metadata tables need to be part of modfiles,
   similar to `ALTER`s of such tables. Otherwise, later modfiles
   cannot rely on those tables being present.

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
   We use a special config var (timescaledb.update_script_stage )
to notify that dependencies have been setup and now timescaledb
specific queries can be enabled. This is useful if we want to,
for example, modify objects that need timescaledb specific syntax as
part of the extension update).
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
is the source version and `downgrade_to_version` is the target
version. Note that we have a separate field for the downgrade.

A downgrade file consists of:
- A prolog that is retrieved from the target version.
- A version-specific piece of code that exists on the source version.
- An epilog that is retrieved from the target version.

The prolog consists of the files mentioned in the `PRE_UPDATE_FILES`
variable in the target version of `cmake/ScriptFiles.cmake`.

The version-specific code is found in the source version of the file
`sql/updates/reverse-dev.sql`.

The epilog consists of the files in variables `SOURCE_FILES`,
`SET_POST_UPDATE_STAGE`, `POST_UPDATE_FILES`, and `UNSET_UPDATE_STAGE`
in that order.

For downgrades to work correctly, some rules need to be followed:

1. If you add new objects in `sql/updates/latest-dev.sql`, you need to
   remove them in the version-specific downgrade file. The
   `sql/updates/pre-update.sql` in the target version do not know
   about objects created in the source version, so they need to be
   dropped explicitly.
2. Since `sql/updates/pre-update.sql` can be executed on a later
   version of the extension, it might be that some objects have been
   removed and do not exist. Hence `DROP` calls need to use `IF NOT
   EXISTS`.

Note that, in contrast to update scripts, downgrade scripts are not
built by composing several downgrade scripts into a more extensive
downgrade script. The downgrade scripts are intended to be use only in
special cases and are not intended to be use to move up and down
between versions at will, which is why we only generate a downgrade
script to the immediately preceeding version.

### When releasing a new version

When releasing a new version, please rename the file `reverse-dev.sql`
to `<version>--<downgrade_to_version>.sql` and add that name to
`REV_FILES` variable in the `sql/CMakeLists.txt`. This will allow
generation of downgrade scripts for any version in that list, but it
is currently not added.
