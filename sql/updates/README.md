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

To ensure that this update process works, there are a few principles
to consider.

1. Modfiles should, in most cases, only contain `ALTER` or `DROP`
   commands that change or remove objects. In some cases,
   modifications of metadata are also necessary.
2. `DROP FUNCTION` needs to be idempotent. In most cases that means
   commands should have an `IF EXISTS` clause. The reason is that
   some modfiles might try to, e.g., `DROP` functions that aren't
   present because they only exist in an intermediate version of the
   database, which is skipped over.
3. Modfiles cannot rely on objects or functions that are present in a
   previous version of the extension. This is because a particular
   modfile should work when upgrading from any previous version of the
   extension, where those functions or objects aren't present yet.
4. The creation of new metadata tables need to be part of modfiles,
   similar to `ALTER`s of such tables. Otherwise, later modfiles
   cannot rely on those tables being present.
5. When creating a new aggregate, the `CREATE` statement should be
   added to both aggregate.sql AND an update file. aggregate.sql is
   run once when TimescaleDB is installed so adding a definition in
   an update file is the only way to ensure that upgrading users get
   the new function.

Note that modfiles that contain no changes need not exist as a
file. Transition modfiles must, however, be listed in the
`CMakeLists.txt` file in the parent directory for an update script to
be built for that version.
