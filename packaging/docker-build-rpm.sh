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

image=${1?Provide a Docker Hub image name}
source=${2?Provide source directory}
binary=${3?Provide binary directory}

import_dir=/mnt
export_dir=/srv
mount_src=--mount=type=bind,src=$source,dst=$import_dir
mount_dst=--mount=type=bind,src=$binary,dst=$export_dir

trap remove_container EXIT

remove_container () {
    docker container rm -f $container
}

# Create container.
#
# Note that we place the source and binary directories at the
# locations that we want in the final build and that we use absolute
# paths for these. We use /srv for build stuff, and /mnt for the
# source.
container=`docker run -id $mount_src $mount_dst $image /bin/bash`

get_repo () {
    case $1 in
	"centos:8")
	    echo "https://download.postgresql.org/pub/repos/yum/reporpms/EL-8-x86_64/pgdg-redhat-repo-latest.noarch.rpm"
	    ;;
	"centos:7")
	    echo "https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm"
	    ;;
    esac
}

PGDG=`get_repo $image`

# Install dependencies and create a user as root.
#
# The use of dnf instead of yum here is very CentOS/RHEL
# 8-specific. In CentOS/RHEL 7 you use yum.
docker exec -i -uroot $container /bin/bash -x <<EOF
yum update -y
yum -y install $PGDG
yum -y update
dnf -y install redhat-lsb-core cmake3 krb5-devel openssl-devel gcc llvm-devel rpm-build git
dnf -qy module disable postgresql
dnf -y install postgresql11 postgresql11-devel postgresql11-server postgresql11-libs
useradd -M -d/ $USER
EOF

# Build the package as the logged in user so that packages are given
# the correct user and can be read without issues.
docker exec -i -u$USER $container /bin/bash -x <<EOF
cd $export_dir
cmake $import_dir -DCMAKE_BUILD_TYPE=RelWithDebugInfo -DREGRESS_CHECKS=OFF -DPG_PATH=/usr/pgsql-11
make
make package
EOF
