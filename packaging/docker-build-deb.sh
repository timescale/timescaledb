#!/bin/bash

# Create a Debian package for a Linux distribution. This assumes that
# there is a docker image with that name. This script mainly serves as
# a litmus test that we can create packages the way we intend.
#
# It will mount the root of the source directory as /mnt inside the
# Docker container (these always exist) and the build directory as
# /srv (this also always exists, and are usually used to export files
# from a machine). This means that the artefacts will be stored in the
# build directory.
#
# Note that you need to create an empty build directory first where
# the products will be placed, that is, something like this:
#
#    mkdir build-deb
#    bash packaging/docker-build-deb.sh ubuntu:19.10 $PWD $PWD/build-deb
#
# TimescaleDB requires at least CMake 3.11 to build. These Debian
# Linux images have CMake 3.11 or later:
#
# - ubuntu:19.10
# - ubuntu:20.04
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

function remove_container () {
    docker container rm -f $container
}

# Create container
container=`docker run -id $mount_src $mount_dst $image /bin/bash`

# Install dependencies and create a user as root
docker exec -i -uroot $container /bin/bash -x <<EOF
export DEBIAN_FRONTEND=noninteractive
export DEBCONF_NONINTERACTIVE_SEEN=true
export APT_KEY_DONT_WARN_ON_DANGEROUS_USAGE=1

set -o pipefail

apt-get update -qq -y
apt-get install -qq -y lsb-release cmake libssl-dev libkrb5-dev wget gnupg2 apt-utils >/dev/null
apt-get install -qq -y postgresql-client postgresql-server-dev-all postgresql
pg_config

useradd -M -d/ $USER
EOF

# Build the package as the logged in user so that packages are given
# the correct user and can be read without issues even in the host
# environment.
docker exec -i -u$USER $container /bin/bash -x <<EOF
cd $export_dir
cmake $import_dir -DCMAKE_BUILD_TYPE=RelWithDebugInfo -DREGRESS_CHECKS=OFF
make package
EOF
