Folder for update SQL-scripts named pre-oldversion--newversion or post-oldversion--newversion

pre files are run before reloading the functions. These should include new
tables, constraints, indexes, etc.

post functions are run after reloading the functions. These include
any entities depending on functions-- e.g. triggers.

It is assumed that there is only one file for pre or post matching the newversion.
