#!/bin/bash

# Create a package for a particular operating system. This assumes
# that there is a docker image with that name. This script mainly
# serves as a litmus test that we can create packages the way we
# intend.
#
# It will mount the root of the source directory as /mnt inside the
# Docker container (these always exist) and the build directory as
# /srv (this also always exists, and are usually used to export files
# from a machine). This means that the artefacts will be stored in the
# build directory.
#
# Note that you need to create an empty build directory first where
# the products will be placed, that is, something like this.
#
#    mkdir build-rpm
#    bash packaging/docker-build-rpm.sh centos $PWD $PWD/build-rpm
#
# TimescaleDB requires at least CMake 3.11 to build. These RH-based
# Linux images have CMake 3.11 or later:
#
# - centos:8
#
# This is a somewhat simplistic build: it might be possible to rely on
# existing PostgreSQL images, but some research is needed to make sure
# that the dependencies work well and the images are updated
# regularly.
#
# Use this script as inspiration for creating packages and CI/CD
# scripts.

if [[ $# -lt 3 ]]; then
    echo "Usage: $(basename $0) <image> <source> <binary>" 1>&2
    exit 2
fi

image=${1?Provide a Docker Hub image name}
source=${2?Provide source directory}
binary=${3?Provide binary directory}

import_dir=/mnt
export_dir=/srv
mount_src=--mount=type=bind,src=$source,dst=$import_dir
mount_dst=--mount=type=bind,src=$binary,dst=$export_dir

PG_VER=11
PG_PATH=/usr/pgsql-${PG_VER}

if ! [[ -d $source ]]; then
    echo "Source directory $source does not exist" 1>&2
    exit 1
fi

declare -a PKG_EXTRAS

case $image in
    "centos:8"|"centos"|"centos:latest")
	PGDG="https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm"
	;;
    "centos:7")
	PGDG="https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm"
	;;
esac

set -e
set -o pipefail

if ! [[ -d $binary ]]; then
    mkdir $binary
fi

# Create container.
#
# Note that we place the source and binary directories at the
# locations that we want in the final build and that we use absolute
# paths for these. We use /srv for build stuff, and /mnt for the
# source.
container=`docker run -id $mount_src $mount_dst $image /bin/bash`

trap remove_container EXIT

remove_container () {
    docker container rm -f $container
}



# Install dependencies and create a user as root.
#
# The use of dnf instead of yum here is very CentOS/RHEL
# 8-specific. In CentOS/RHEL 7 you use yum, and it works for
# CentOS/RHEL 8 as well.
docker exec -i -uroot -e PG_VER=$PG_VER -e PGDG=$PGDG -e USER=$USER $container /bin/bash -ex <<"EOF"
yum -y update
yum -y install redhat-lsb-core cmake krb5-devel openssl-devel gcc rpm-build git
lsb_release --all
DISTID=`lsb_release -si`:`lsb_release -sr`
case "$DISTID" in
     *:8.*)
          dnf -qy module disable postgresql
	  ;;
     *:7.*)
          yum -y install epel-release centos-release-scl
	  ;;
esac
yum -y install $PGDG
yum -y update
yum -y install postgresql${PG_VER} postgresql${PG_VER}-devel postgresql${PG_VER}-server
useradd -M -d/ $USER
/usr/pgsql-${PG_VER}/bin/pg_config
EOF

# Build the package as the logged in user so that packages are given
# the correct user and can be read without issues.
docker exec -i -u$USER $container /bin/bash -ex <<EOF
cd $export_dir
cmake $import_dir -DCMAKE_BUILD_TYPE=RelWithDebugInfo -DREGRESS_CHECKS=OFF -DPG_PATH=${PG_PATH}
make
make package
EOF
