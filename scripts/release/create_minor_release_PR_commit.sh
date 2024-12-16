#!/bin/bash
set -eu

# Folder, where we have cloned repositories' sources
SOURCES_DIR="sources"

GH_USERNAME=$(gh auth status | grep 'Logged in to' |cut -d ' ' -f 9)

FORK_DIR="$GH_USERNAME-timescaledb"

echo "---- Deriving the release related versions from main ----"

cd ~/"$SOURCES_DIR"/"$FORK_DIR"
git fetch --all

NEW_PATCH_VERSION="0"
NEW_VERSION=$(head -1 version.config | cut -d ' ' -f 3 | cut -d '-' -f 1)
RELEASE_BRANCH="${NEW_VERSION/%.$NEW_PATCH_VERSION/.x}"
CURRENT_VERSION=$(tail -1 version.config | cut -d ' ' -f 3)
CURRENT_MINOR_VERSION=$(echo $NEW_VERSION | cut -d '.' -f 2)
NEW_MINOR_VERSION=$((CURRENT_MINOR_VERSION + 1))
cd sql/updates

for f in ./*
do
  case $f in
    *$CURRENT_VERSION.sql) LAST_UPDATE_FILE=$f;;
    *) true;;
  esac
done
LAST_VERSION=$(echo "$LAST_UPDATE_FILE" |cut -d '-' -f 1 |cut -d '/' -f 2)

echo "CURRENT_VERSION is $CURRENT_VERSION"
echo "LAST_VERSION is $LAST_VERSION"
echo "RELEASE_BRANCH is $RELEASE_BRANCH"
echo "NEW_VERSION is $NEW_VERSION"
cd ~/"$SOURCES_DIR"/"$FORK_DIR"


# Derived Variables
RELEASE_PR_BRANCH="release-$NEW_VERSION"
UPDATE_FILE="$CURRENT_VERSION--$NEW_VERSION.sql"
DOWNGRADE_FILE="$NEW_VERSION--$CURRENT_VERSION.sql"
LAST_UPDATE_FILE="$LAST_VERSION--$CURRENT_VERSION.sql"
LAST_DOWNGRADE_FILE="$CURRENT_VERSION--$LAST_VERSION.sql"

RELEASE_BRANCH_EXISTS=$(git branch -a |grep -c 'upstream/$RELEASE_BRANCH'|cut -d ' ' -f 1)

if [[ $RELEASE_BRANCH_EXISTS == '0' ]]; then
  echo "git branch '$RELEASE_BRANCH' does not exist in the remote repository, yet"
  echo "We want to raise this PR against main"
  RELEASE_BRANCH="main"
  RELEASE_PR_BRANCH="$RELEASE_PR_BRANCH-main"
fi

echo "final RELEASE_BRANCH is $RELEASE_BRANCH"
echo "RELEASE_PR_BRANCH is $RELEASE_PR_BRANCH"

echo "---- Creating release branch $RELEASE_PR_BRANCH from $RELEASE_BRANCH, on the fork ----"

git checkout -b "$RELEASE_PR_BRANCH" upstream/"$RELEASE_BRANCH"
git branch
git pull && git diff HEAD


echo "---- Modifying version.config to the new versions ----"

sed -i.bak "s/-dev//g" version.config
rm version.config.bak


echo "---- Creating update SQL file $UPDATE_FILE ----"

cd sql/updates
cp latest-dev.sql "$UPDATE_FILE"
git add "$UPDATE_FILE"
truncate -s 0 latest-dev.sql


echo "---- Creating downgrade SQL file $DOWNGRADE_FILE ----"

cp reverse-dev.sql "$DOWNGRADE_FILE"
git add "$DOWNGRADE_FILE"
truncate -s 0 reverse-dev.sql


echo "---- Adding update sql file to CMakeLists.txt  ----"

cd ..
gawk -i inplace '/'$LAST_UPDATE_FILE')/ { print; print "    updates/'$UPDATE_FILE')"; next }1' CMakeLists.txt
sed -i.bak "s/${LAST_UPDATE_FILE})/${LAST_UPDATE_FILE}/g" CMakeLists.txt
rm CMakeLists.txt.bak


echo "---- Adding downgrade sql file to CMakeLists.txt  ----"

gawk -i inplace '/  '$LAST_DOWNGRADE_FILE')/ { print; print "    '$DOWNGRADE_FILE')"; next }1' CMakeLists.txt
sed -i.bak "s/  ${LAST_DOWNGRADE_FILE})/  ${LAST_DOWNGRADE_FILE}/g" CMakeLists.txt
rm CMakeLists.txt.bak

sed -i.bak "s/FILE reverse-dev.sql)/FILE ${DOWNGRADE_FILE})/g" CMakeLists.txt
rm CMakeLists.txt.bak


echo "---- Creating CHANGELOG_$NEW_VERSION.md file ----"

rm -f ~/CHANGELOG_"$NEW_VERSION".md

cd ~/"$SOURCES_DIR"/"$FORK_DIR"
./scripts/merge_changelogs.sh > ~/CHANGELOG_"$NEW_VERSION".md

echo "---- Editing the CHANGELOG.md file with the contents of CHANGELOG_$NEW_VERSION.md file. ----"

cd ~/"$SOURCES_DIR"/"$FORK_DIR"
RELEASE_NOTE_START=$(grep -n $CURRENT_VERSION CHANGELOG.md | cut -d ':' -f 1 | head -1)
CHANGELOG_HEADER_LINES=$((RELEASE_NOTE_START - 1))

mv CHANGELOG.md CHANGELOG.md.tmp
head -n $CHANGELOG_HEADER_LINES CHANGELOG.md.tmp > CHANGELOG.md
cat ~/CHANGELOG_"$NEW_VERSION".md >> CHANGELOG.md
CHANGELOG_LENGTH=$(wc -l CHANGELOG.md.tmp | cut -d ' ' -f 5)
CHANGELOG_ENTRIES=$((CHANGELOG_LENGTH-CHANGELOG_HEADER_LINES))
tail -n "$CHANGELOG_ENTRIES" CHANGELOG.md.tmp >> CHANGELOG.md
rm CHANGELOG.md.tmp


echo "---- Deleting all unreleased pr_* , fix_* , ... style files, except template ones ----"

cd .unreleased

for f in ./*
do
  case $f in
    *template.*) true;;
    *RELEASE_NOTES*) true;;
    *) git rm "$f";;
  esac
done

cd ..


if [[ $RELEASE_BRANCH_EXISTS == '0' ]]; then
  echo "---- Modifying version.config to the new versions , if current PR is for main----"
  sed -i.bak "s/${NEW_VERSION}/${NEW_VERSION}-dev/g" version.config
  sed -i.bak "s/${CURRENT_MINOR_VERSION}/${NEW_MINOR_VERSION}/g" version.config
  sed -i.bak "s/${CURRENT_VERSION}/${NEW_VERSION}/g" version.config
  rm version.config.bak
fi

git diff HEAD --name-only


echo "---- Committing the Release PR to fork ----"

#Remove date from the intermediate CHANGELOG file.

cut -d '(' -f1 < ~/CHANGELOG_"$NEW_VERSION".md > ~/CHANGELOG_"$NEW_VERSION".md.tmp
mv ~/CHANGELOG_"$NEW_VERSION".md.tmp ~/CHANGELOG_"$NEW_VERSION".md

git commit --no-verify -a -F ~/CHANGELOG_"$NEW_VERSION".md

git push origin $RELEASE_PR_BRANCH
