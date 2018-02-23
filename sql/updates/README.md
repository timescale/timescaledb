## Extension updates

This directory contains SQL files (modfiles) with idempotent
modifications that happen when updating from one version to another.

The actual update scripts are compiled from modfiles by concatenating
them with the current source code. Modfiles should always be
first. Update scripts can "jump" several versions by using multiple
modfiles in order.

To ensure that this update process works, there are a few principles
to considered.

1. Modfiles should, in most cases, only contain `ALTER` or `DROP`
   commands that change or remove objects. In some cases,
   modifications of metadata are also necessary.
2. DROP FUNCTION needs to be idempotent. In most cases that means
   commands should have an `IF NOT EXISTS` clause. The reason is that
   some modfiles might try to, e.g., DROP functions that aren't
   present because they only exist in an intermediate version of the
   database, which is skipped over.
3. Modfiles cannot rely on objects or functions that are present in a
   previous version of the extension. This is because a particular
   modfile should work when upgrading from any previous version of the
   extension, where those functions or objects aren't present yet.
4. The creation of new metadata tables need to be part of modfiles,
   similar to ALTERs of such tables. Otherwise, later modfiles cannot
   rely on those tables being present.

Note that modfiles that contain no changes need not exist as a
file. They need, however, to be listed in the `CMakeLists.txt` file in
the parent directory for an update script to be built for that
version.
