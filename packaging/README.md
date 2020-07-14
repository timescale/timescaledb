# Creating packages of TimescaleDB

TimescaleDB source is intended to be easy to create package for and
will be default create packages for the PostgreSQL installation
available on the system. This means that you can create a package for
a particular system by starting a Docker container with the operating
system, make the source available in the container, and build packages
for that installation.

The packaging system will figure out package names, dependencies,
version information, etc. and build a package for the system.

## Creating a package

By default, package can be created for the installation you have using
`make package`. This will build a package for the distribution that
you're on and with the installation prefix that you used. Typically,
building a package from source can be done with something like this:

```bash
git clone https://github.com/timescale/timescaledb.git
mkdir timescaledb/build-package
cd timescaledb/build-package
cmake .. -DREGRESS_CHECK=OFF
make package
```

If it is not possible to build for the installation you have, you will
get an error. Please consider adding support for your distribution and
operating system, or report an issue so that we can fix it.

## Package file name

The package file name is slightly different dependent on what kind of
packaging system it is. In all cases, the packaging build files
figures out the following for each component to build a package for:

`packname`
: is the package name for a component, which contain some basic
  information about what it is build from in addition to the name
  prefix. We store this in the variable
  `CPACK_<component>_PACKAGE_NAME` for each component.

`distid`
: is the distribution id that can be retrieved using `lsb_release
  --short --id`. The result of the output is lower-cased before being
  used. You can set this explicitly using the CMake variable
  `CMAKE_DISTRO_NAME`.

`distrel`
: is the distribution release and is retrieved using `lsb_release
  --short --release`. You can set this explicitly using the CMake
  variable `CMAKE_DISTRO_RELEASE`.

`pgmajor`, `pgminor` and `pgpatch`
: is the major, minor, and patch
  version of the PostgreSQL the package is built for. These are stored
  in the CMake variables `PG_VERSION_MAJOR`, `PG_VERSION_MINOR`, and
  `PG_VERSION_PATCH` respectively.

`relno`
: is the release number. Typically 1, but if you have to make
  several releases of the same package, you can change this using the
  CMake variable `TSDB_RELNO`.

`arch`
: is the architecture as given by `uname -m`.

The file name for an RPM package is typically
`<packname>-<version>.<arch>.rpm` and for a Debian package it is
`<packname>_<version>_<arch>.deb`.

See description of package name (`packname`) and version name
(`version`) below.

### Package name

The package name is used as prefix for the package file name, but also
use for some directories in the package. If the major PG version is
12, the three names under consideration are:

- The loader package name is `timescaledb-loader-postgresql-12`.
- The OSS package name is `timescaledb-oss-postgresql-12`
- The TSL package name is `timescaledb-postgresql-12`.

### Version

The version consists of the version number followed by release
information.

For the Debian package, the format is typically
`<version>-<relno><distid>~<distrel>` and the `distid` and `distrel`
is retrieved from `lsb_release`. For Debian it is also possible to add
an epoch, but we do not use that.

For the RPM package, the name is built as
`<version>-<relno>%{?dist}`. That is, the traditional `%{?dist}` is
added to the release number and used as release information. This will
then be filled in by `rpmbuild`.

### Examples

`timescaledb-postgresql-12-1.7.2-1ubuntu~19.10.amd64.deb`
: is the package name for an Debian package containing Timescale 1.7.2
  build for PostgreSQL 12 on Ubuntu 19.10 with a 64-bit architecture.

`timescaledb-postgresql-11-2.1.0-1.el8.x86_64.rpm`
: is an RPM package for TimescaleDB 2.1.0 built for PostgreSQL 11 on
  CentOS/RHEL 8 with a 64-bit architecture. CentOS 8 packages are the
  same as RHEL8 packages, which is why `el8` is used.

# Build Dependencies

Depending on the OS, you need different things installed. Look into
the build file examples in this directory. In particular, have a
dependency on CMake 3.11 for the build system.

# Linux systems

To build packages on Linux, you need to have the following installed:

- `lsb_release` is used to figure out the distribution id and release.
- `rpmbuild` to build RPM packages.
- `dpkg-dev` to build Debian packages.
- `git` to extract version information from the repository.

# Components

The files of the repository is partitioned into four components that
are used to build the packages.

- `loader` are the main files of the extension, meaning the control
  file and the loader shared library and also copyright for the loader
  package.
- The `basic` component contains all the files related to the open-source
  package under Apache license.
- The `full` component contains all the files related to the Timescale
  package, except the files in the `basic` package.
- The `testing` component contains all the testing libraries and
  files.

# Notes

## CentOS

To install `lsb_release`:

```
yum install redhat-lsb-core
```

To install PostgreSQL from PGDG (here for CentOS 8):

```
yum -y install https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm
```

For CentOS 8 and later, DNF is used instead of YUM, so to install
PostgreSQL from PGDG, you need to disable the installed version:

```
dnf -qy module disable postgresql
```

