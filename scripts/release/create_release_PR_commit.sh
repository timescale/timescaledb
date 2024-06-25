#!/bin/bash
set -eu

#Versions
#Expected to change more often , when we run this script.
NEW_VERSION="2.15.3"
CURRENT_VERSION="2.15.2"
LAST_VERSION="2.15.1"
RELEASE_BRANCH="2.15.x"
RELEASE_ISSUE="613"

# GITHUB_USERNAMES
GH_USERNAME="pallavisontakke"

# Folder, where we have cloned repositories' sources
SOURCES_DIR="sources"

# Derived Variables
FORK_DIR="$GH_USERNAME-timescaledb"
RELEASE_PR_BRANCH="release-$NEW_VERSION"
UPDATE_FILE="$CURRENT_VERSION--$NEW_VERSION.sql"
DOWNGRADE_FILE="$NEW_VERSION--$CURRENT_VERSION.sql"
LAST_UPDATE_FILE="$LAST_VERSION--$CURRENT_VERSION.sql"
LAST_DOWNGRADE_FILE="$CURRENT_VERSION--$LAST_VERSION.sql"
CHANGELOG_HEADER_LINES="6"


echo "---- Creating release branch $RELEASE_PR_BRANCH from $RELEASE_BRANCH, on the fork ----"

cd ~/"$SOURCES_DIR"/"$FORK_DIR"
git fetch --all
git checkout -b "$RELEASE_PR_BRANCH" upstream/"$RELEASE_BRANCH"
git branch
git pull && git diff HEAD


echo "---- Modifying version.config to the new versions ----"

sed -i.bak "s/"$CURRENT_VERSION"/"$NEW_VERSION"/g" version.config
sed -i.bak "s/"$LAST_VERSION"/"$CURRENT_VERSION"/g" version.config
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
sed -i.bak "s/"$LAST_UPDATE_FILE")/"$LAST_UPDATE_FILE"/g" CMakeLists.txt
rm CMakeLists.txt.bak


echo "---- Adding downgrade sql file to CMakeLists.txt  ----"

gawk -i inplace '/  '$LAST_DOWNGRADE_FILE')/ { print; print "    '$DOWNGRADE_FILE')"; next }1' CMakeLists.txt
sed -i.bak "s/  "$LAST_DOWNGRADE_FILE")/  "$LAST_DOWNGRADE_FILE"/g" CMakeLists.txt
rm CMakeLists.txt.bak

sed -i.bak "s/FILE "$LAST_DOWNGRADE_FILE")/FILE "$DOWNGRADE_FILE")/g" CMakeLists.txt
rm CMakeLists.txt.bak


#delete Features section from the templates and create this version's CHANGELOG.md
#Undo the deletion, from the template files.
echo "---- Creating CHANGELOG_"$NEW_VERSION".md file ----"

cd ~/"$SOURCES_DIR"/"$FORK_DIR"
cd .unreleased

head -n 12 RELEASE_NOTES_HEADER.md.j2 > RELEASE_NOTES_HEADER.md.j2.tmp
mv RELEASE_NOTES_HEADER.md.j2.tmp RELEASE_NOTES_HEADER.md.j2

head -n 12 template.release_notes_header.j2 > template.release_notes_header.j2.tmp
mv template.release_notes_header.j2.tmp template.release_notes_header.j2

rm -f ~/CHANGELOG_"$NEW_VERSION".md

cd ~/"$SOURCES_DIR"/"$FORK_DIR"
./scripts/merge_changelogs.sh > ~/CHANGELOG_"$NEW_VERSION".md

cd .unreleased
rm RELEASE_NOTES_HEADER.md.j2 template.release_notes_header.j2
git checkout RELEASE_NOTES_HEADER.md.j2 template.release_notes_header.j2


echo "---- Editing the CHANGELOG.md file with the contents of CHANGELOG_"$NEW_VERSION".md file. ----"

cd ~/"$SOURCES_DIR"/"$FORK_DIR"
mv CHANGELOG.md CHANGELOG.md.tmp
head -n 6 CHANGELOG.md.tmp > CHANGELOG.md
cat ~/CHANGELOG_"$NEW_VERSION".md >> CHANGELOG.md
CHANGELOG_LENGTH=$(wc -l CHANGELOG.md.tmp | cut -d ' ' -f 5)
CHANGELOG_ENTRIES=$((CHANGELOG_LENGTH-CHANGELOG_HEADER_LINES))
tail -n "$CHANGELOG_ENTRIES" CHANGELOG.md.tmp >> CHANGELOG.md
rm CHANGELOG.md.tmp


echo "---- Deleting fix_* & pr_* files ----"

rm -f .unreleased/fix_*
rm -f .unreleased/pr_*

git diff HEAD --name-only


echo "---- Commiting the Release PR to fork ----"

#Remove date from the intermediate CHANGELOG file.

cut -d '(' -f1 < ~/CHANGELOG_"$NEW_VERSION".md > ~/CHANGELOG_"$NEW_VERSION".md.tmp
mv ~/CHANGELOG_"$NEW_VERSION".md.tmp ~/CHANGELOG_"$NEW_VERSION".md

git commit -a -F ~/CHANGELOG_"$NEW_VERSION".md

git push origin $RELEASE_PR_BRANCH
