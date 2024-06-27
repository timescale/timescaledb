#!/bin/bash
set -eu


args=("$@")

# GITHUB_USERNAMES
GH_EMAIL="$1"
GH_USERNAME="$2"
GH_FULL_USERNAME="$3"

echo "GH_EMAIL is $GH_EMAIL"
echo "GH_USERNAME is $GH_USERNAME"
echo "GH_FULL_USERNAME is $GH_FULL_USERNAME"

# Folder, where we have cloned repositories' sources
SOURCES_DIR="sources"

# Derived Variables
FORK_DIR="$GH_USERNAME-timescaledb"
PWD=$(pwd)
SCRIPTS_DIR="$PWD"

echo "---- Updating fork with upstream for user $GH_USERNAME ----"

gh repo sync "$GH_USERNAME/timescaledb" -b main


echo "---- Cloning the fork to $FORK_DIR ----"

cd
cd "$SOURCES_DIR"
rm -rf "$FORK_DIR"
git clone git@github.com:"$GH_USERNAME"/timescaledb.git "$FORK_DIR"
cd "$FORK_DIR"
git branch
git pull && git diff HEAD
git log -n 2

echo "---- Configuring the fork for commit ----"

git config user.name "$GH_FULL_USERNAME"
git config user.email "$GH_EMAIL"
git remote add upstream git@github.com:timescale/timescaledb.git
git config -l
git remote -v


echo "---- Updating tags from upstream on the fork ----"

git fetch --tags upstream
git push --tags origin main
# Check the needed branch name here - could it be 2.14.x ?
# git push -f --tags origin main

cd "$SCRIPTS_DIR"
sed -i.bak "s/GH_USERNAME/"$GH_USERNAME"/g" create_release_PR_commit.sh
rm create_release_PR_commit.sh.bak
