#!/bin/bash
set -eu

# Folder, where we have cloned repositories' sources
SOURCES_DIR="timescaledb"

FORK_DIR="timescaledb"

echo "---- Deriving the release related versions from main ----"

cd ~/"$SOURCES_DIR"/"$FORK_DIR"
git fetch --all

NEW_PATCH_VERSION="0"

NEW_VERSION=$(head -1 version.config | cut -d ' ' -f 3 | cut -d '-' -f 1)

RELEASE_BRANCH="${NEW_VERSION/%.$NEW_PATCH_VERSION/.x}"

echo "RELEASE_BRANCH is $RELEASE_BRANCH"
echo "NEW_VERSION is $NEW_VERSION"

echo "---- Creating the version branch from main ----"

git fetch --all
git checkout -b "$RELEASE_BRANCH" origin/main
git push origin "$RELEASE_BRANCH":"$RELEASE_BRANCH"

