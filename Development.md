### Errors

All Errors raised in plpgsql have to use an ERRCODE that is registered in common/errors.sql.


### C Development Tips

compiling postgres:
```
  #recomended to add -Og to CFLAGS but doesn't seem to work on my sys
  ./configure --enable-cassert --enable-debug CFLAGS="-ggdb -g3 -fno-omit-frame-pointer"
  make
  sudo make install
  cd contrib/hstore
  make
  sudo make install
```

compile and install extension (inside /extension directory):
```
  make install
```

run:
```
  PGDATA=/usr/local/pgsql/data/ /usr/local/pgsql/bin/postgres \
  -D /usr/local/pgsql/data/ \
  -cshared_preload_libraries="iobeamdb" \
  -clog_min_duration_statement=-1  \
  -clog_line_prefix="%m [%p]: [%l-1] %u@%d"
```

connect lldb (mac version gdb)(pid gotten from pg\_backend\_pid()):
```
  lldb -p <pid>
```

viewing log:
```
   tail /usr/local/pgsql/data/pg_log/postgresql.log
```
