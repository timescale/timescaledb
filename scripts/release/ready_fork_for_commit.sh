#!/bin/bash
set -eu


echo "---- Setting git user parameters as per current global git configuration ----"

# GITHUB_USERNAMES
GH_EMAIL=$(git config user.email)
GH_FULL_USERNAME=$(git config user.name)
GH_USERNAME=$(gh auth status | grep 'Logged in to' |cut -d ' ' -f 9)

echo "GH_EMAIL is $GH_EMAIL"
echo "GH_FULL_USERNAME is $GH_FULL_USERNAME"
echo "GH_USERNAME is $GH_USERNAME"

# Folder, where we have cloned repositories' sources
SOURCES_DIR="sources"

# Derived Variables
FORK_DIR="$GH_USERNAME-timescaledb"

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

