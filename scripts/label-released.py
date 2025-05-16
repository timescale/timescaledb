#!/usr/bin/env python3
"""
Look at commits between the given release and the previous one, and label all
PRs that made these commits with the "released-..." label.
"""
import os
import sys
import argparse
import re
import subprocess
import requests
import github  # This is PyGithub.
import more_itertools

OWNER = "timescale"
REPO = "timescaledb"
TOKEN = os.environ.get("GH_TOKEN")
if not TOKEN:
    print("Specify the GitHub token in GH_TOKEN environment variable.", file=sys.stderr)
    sys.exit(1)


def git_check(cmd: str):
    subprocess.run(f"git {cmd}", shell=True, check=True)


def git_output(cmd: str) -> str:
    return subprocess.check_output(f"git {cmd}", shell=True, text=True).strip()


def run_query(query, params):
    """A simple function to use requests.post to make the GraphQL API call."""

    request = requests.post(
        "https://api.github.com/graphql",
        json={"query": query, "variables": params},
        headers={"Authorization": f"Bearer {TOKEN}"} if TOKEN else None,
        timeout=20,
    )
    response = request.json()

    # Have to work around the unique GraphQL convention of returning 200 for errors.
    if request.status_code != 200 or "errors" in response:
        raise ValueError(
            f"Query failed to run by returning code of {request.status_code}."
            f"\nQuery: '{query}'"
            f"\nResponse: '{request.json()}'"
        )

    return response["data"]


parser = argparse.ArgumentParser()
parser.add_argument("--release", required=True, help="Tag to process, e.g. 2.20.0")
parser.add_argument("--dry-run", action="store_true")
args = parser.parse_args()
target_tag = args.release
dry_run = args.dry_run

# Create the label if needed
gh = github.Github(TOKEN)
repo = gh.get_repo(f"{OWNER}/{REPO}")
label_name = f"released-{target_tag}"
try:
    pygithub_label = repo.get_label(label_name)
    label_id = pygithub_label.raw_data["node_id"]
except github.UnknownObjectException:
    if not args.dry_run:
        pygithub_label = repo.create_label(
            label_name, "d3d3d3", f"Released in {target_tag}"
        )
        label_id = pygithub_label.raw_data["node_id"]
    else:
        label_id = "<dry run>"
print(f"Label will be: {label_name}")

# Make sure the branches are present in the repository
git_check("fetch --depth=1000 origin main:refs/remotes/origin/main")
git_check(f"fetch --depth=1000 origin tag {target_tag}")

# Read the previous release from version.config.
try:
    cfg = git_output(f"show {target_tag}:version.config")
except subprocess.CalledProcessError:
    sys.exit(f"Error: cannot read version.config at '{target_tag}'")

m = re.search(r"^(?:previous_version|update_from_version)\s*=\s*(\S+)", cfg, re.M)
if not m:
    sys.exit("Error: version.config missing previous_version/update_from_version")
prev_tag = m.group(1)
git_check(f"fetch --depth=1000 origin tag {prev_tag}")

print(f"Comparing tags: {prev_tag} → {target_tag}")
if dry_run:
    print("[dry-run] no changes will be made")


def fetch_commits_with_prs(starting_commit, cutoff_date):
    """
    Fetch the commits starting from the given one until the cutoff
    date, with the associated PRs, using paginated GraphQL request.
    """
    GQL_COMMITS = """
    query CommitsWithPRs(
      $owner: String!, $repo: String!,
      $expr: String!, $since: GitTimestamp!,
      $first: Int!, $after: String
    ) {
      repository(owner:$owner,name:$repo) {
        object(expression:$expr) {
          ... on Commit {
            history(first:$first, since:$since, after:$after) {
              pageInfo { hasNextPage endCursor }
              nodes {
                oid
                messageHeadline
                # We only handle the commits with one PR, so fetch at most two.
                associatedPullRequests(first:2) {
                  nodes { id number title baseRefName }
                }
              }
            }
          }
        }
      }
    }
    """

    nodes = []
    after = None
    while True:
        params = {
            "owner": OWNER,
            "repo": REPO,
            "expr": starting_commit,
            "since": cutoff_date,
            "first": 100,
            "after": after,
        }
        hist = run_query(GQL_COMMITS, params)["repository"]["object"]["history"]

        nodes.extend(hist["nodes"])
        page_info = hist["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        after = page_info["endCursor"]
        print("Fetching next page...")

    print(f"Fetched {len(nodes)} commits with PR info starting from {starting_commit}")
    return nodes


target_release = git_output(f"rev-parse {target_tag}^0")
print(f"Target release commit {git_output(f'log -1 --oneline {target_release}')}")
prev_release = git_output(f"rev-parse {prev_tag}^0")
print(f"Prev release commit {git_output(f'log -1 --oneline {prev_release}')}")
prev_release_fork_sha = git_output(f"merge-base {target_release} {prev_release}")
print(
    f"Prev release fork point {git_output(f'log -1 --oneline {prev_release_fork_sha}')}"
)
prev_release_fork_date = git_output(f"show -s --format=%cI {prev_release_fork_sha}")
print(f"Prev release fork date {prev_release_fork_date}")
main_fork_sha = git_output(f"merge-base {target_release} origin/main")
print(f"Main fork point {git_output(f'log -1 --oneline {main_fork_sha}')}")
main_fork_date = git_output(f"show -s --format=%cI {main_fork_sha}")
print(f"Main fork date {main_fork_date}")


# This is the relationship between the various refs we've built above:
# For a patch release:
# (current release branch)  -(backports)---prev_release_fork_sha---target_release--->
#                          /  ^ ^ ^
# (main) >----main_fork_sha---------------->
#
# For a minor release:
# (prev release branch)        prev_release-->
#                             /
# (current release branch)   /                 -(backports)-target_release-->
#                           /                 /  ^ ^ ^
# (main) >-prev_release_fork_sha---main_fork_sha--------------->
#
# We can't use the commit SHAs for the commit lookups due to API limitations, so
# we use the dates instead.
# Now, perform the lookups for release branch commits and the potentially
# backported main commits with the respective PRs.

branch_commit_nodes = fetch_commits_with_prs(target_release, prev_release_fork_date)

main_commit_nodes = fetch_commits_with_prs("main", main_fork_date)

# We're going to match the commits by title to account for backports.
main_commit_title_to_pr = {
    node["messageHeadline"]: prs[0]
    for node in main_commit_nodes
    if (prs := node["associatedPullRequests"]["nodes"]) and len(prs) == 1
}

print(f"On main ({len(main_commit_title_to_pr)} PRs):")
print(
    "\n".join(
        [
            f'#{pr["number"]}: {pr["title"]} <- {title}'
            for title, pr in main_commit_title_to_pr.items()
        ]
    )
)
print()

branch_commit_title_to_pr = {
    node["messageHeadline"]: prs[0]
    for node in branch_commit_nodes
    if (prs := node["associatedPullRequests"]["nodes"]) and len(prs) == 1
}

print(f"On branch ({len(branch_commit_title_to_pr)} PRs):")
print(
    "\n".join(
        [
            f'#{pr["number"]}: {pr["title"]} <- {title}'
            for title, pr in branch_commit_title_to_pr.items()
        ]
    )
)
print()

# The commits with same titles in main and release branch, but with different PRs,
# are backported commits. We have to label the PR to main in that case, and not
# the backport PR.
backported_titles = {
    title: pr
    for title, pr in main_commit_title_to_pr.items()
    if title in branch_commit_title_to_pr
    and pr["number"] != branch_commit_title_to_pr[title]["number"]
}

print(f"Backported ({len(backported_titles)} PRs):")
print(
    "\n".join([f'#{pr["number"]}: {pr["title"]}' for pr in backported_titles.values()])
)
print()

branch_commit_title_to_pr.update(backported_titles)
print(f"To label as {label_name} ({len(branch_commit_title_to_pr)} PRs):")
print(
    "\n".join(
        [f'#{pr["number"]}: {pr["title"]}' for pr in branch_commit_title_to_pr.values()]
    )
)
print()

# Label the PRs in bulk using GraphQL.
ids = list({pr["id"] for pr in branch_commit_title_to_pr.values()})
for chunk in more_itertools.chunked(ids, 20):
    parts = [
        f'p{j}: addLabelsToLabelable(input: {{labelableId:"{nid}",labelIds:["{label_id}"]}}) {{ clientMutationId }}'
        for j, nid in enumerate(chunk)
    ]
    gql = "mutation BulkLabel {\n" + "\n".join(parts) + "\n}"
    if dry_run:
        print(f"\nDry-run for {len(chunk)} PRs:\n{gql}")
    else:
        run_query(gql, {})
        print(f"Labeled {len(chunk)} PRs.")
