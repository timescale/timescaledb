### Building from source

#### Building from source (Unix-based systems)

If you are building from source for **non-development purposes**
(i.e., you want to run TimescaleDB, not submit a patch), you should
**always use a release-tagged commit and not build from `master`**.
See the Releases tab for the latest release.

**Prerequisites**:

- A standard PostgreSQL 14, 13.2+ or 12 installation with development
environment (header files) (e.g., `postgresql-server-dev-13` package
for Linux, Postgres.app for MacOS)
- C compiler (e.g., gcc or clang)
- [CMake](https://cmake.org/) version 3.4 or greater

```bash
git clone git@github.com:timescale/timescaledb.git
cd timescaledb
# Find the latest release and checkout, e.g. for 2.5.0:
git checkout 2.5.0
# Bootstrap the build system
./bootstrap
# To build the extension
cd build && make
# To install
make install
```

Note, if you have multiple versions of PostgreSQL installed you can specify the path to `pg_config`
that should be used by using `./bootstrap -DPG_CONFIG=/path/to/pg_config`.

Please see our [additional configuration instructions](https://docs.timescale.com/getting-started/installation).

#### Building from source (Windows)

If you are building from source for **non-development purposes**
(i.e., you want to run TimescaleDB, not submit a patch), you should
**always use a release-tagged commit and not build from `master`**.
See the Releases tab for the latest release.

**Prerequisites**:

- A standard [PostgreSQL 14, 13.2+ or 12 64-bit installation](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads#windows)
- OpenSSL for Windows
- Microsoft Visual Studio 2017 with CMake and Git components
- OR Visual Studio 2015/2016 with [CMake](https://cmake.org/) version 3.4 or greater and Git
- Make sure all relevant binaries are in your PATH: `pg_config` and `cmake`

If using Visual Studio 2017 with the CMake and Git components, you
should be able to simply clone the repo and open the folder in
Visual Studio which will take care of the rest.

If you are using an earlier version of Visual Studio, then it can
be built in the following way:
```bash
git clone git@github.com:timescale/timescaledb.git
cd timescaledb

# Find the latest release and checkout, e.g. for 2.5.0:
git checkout 2.5.0
# Bootstrap the build system
bootstrap.bat
# To build the extension from command line
cmake --build ./build --config Release
# To install
cmake --build ./build --config Release --target install

# Alternatively, build in Visual Studio via its built-in support for
# CMake or by opening the generated build/timescaledb.sln solution file.
```
