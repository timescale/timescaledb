#!/usr/bin/env python3

import os
import re
import string
import subprocess
import sys

from github import Github  # This is PyGithub.
import requests


# Limit our history search and fetch depth to this value, not to get stuck in
# case of a bug.
HISTORY_DEPTH = 1000


def run_query(query):
    """A simple function to use requests.post to make the GraphQL API call."""

    request = requests.post(
        "https://api.github.com/graphql",
        json={"query": query},
        headers={"Authorization": f'Bearer {os.environ.get("GITHUB_TOKEN")}'},
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

    return response


def get_referenced_issue(pr_number):
    """Get the number of issue fixed by the given pull request.
    Returns None if no issue is fixed, or more than one issue"""

    ref_result = run_query(
        string.Template(
            """
        query {
            repository(owner: "timescale", name: "timescaledb") {
              pullRequest(number: $pr_number) {
                closingIssuesReferences(first: 1) {
                  edges {
                    node {
                      number
                    }
                  }
                }
              }
            }
          }
          """
        ).substitute({"pr_number": pr_number})
    )

    # The above returns {'data': {'repository': {'pullRequest': {'closingIssuesReferences': {'edges': [{'node': {'number': 4944}}]}}}}}

    ref_edges = ref_result["data"]["repository"]["pullRequest"][
        "closingIssuesReferences"
    ]["edges"]

    if ref_edges and len(ref_edges) == 1:
        return ref_edges[0]["node"]["number"]

    return None


def set_auto_merge(pr_number):
    """Enable auto-merge for the given PR"""

    owner, name = target_repo_name.split("/")
    # We first have to find out the PR id, which is some base64 string, different
    # from its number.
    query = string.Template(
        """query {
          repository(owner: "$owner", name: "$name") {
            pullRequest(number: $pr_number) {
              id
            }
          }
        }"""
    ).substitute(pr_number=pr_number, owner=owner, name=name)
    result = run_query(query)
    pr_id = result["data"]["repository"]["pullRequest"]["id"]

    query = string.Template(
        """mutation {
            enablePullRequestAutoMerge(
                input: {
                    pullRequestId: "$pr_id",
                    mergeMethod: REBASE
                }
            ) {
                clientMutationId
            }
        }"""
    ).substitute(pr_id=pr_id)
    run_query(query)


def git_output(command):
    """Get output from the git command, checking for the successful exit code"""
    return subprocess.check_output(f"git {command}", shell=True, text=True)


def git_check(command):
    """Run a git command, checking for the successful exit code"""
    subprocess.run(f"git {command}", shell=True, check=True)


def git_returncode(command):
    """Run a git command, returning the exit code"""
    return subprocess.run(f"git {command}", shell=True, check=False).returncode


# The token has to have the "access public repositories" permission, or else creating a PR returns 404.
github = Github(os.environ.get("GITHUB_TOKEN"))

# If we are running inside Github Action, will modify the main repo.
source_remote = "origin"
source_repo_name = os.environ.get("GITHUB_REPOSITORY")
target_remote = source_remote
target_repo_name = source_repo_name

if not source_repo_name:
    # We are running manually for debugging, probably want to modify a fork.
    source_repo_name = "timescale/timescaledb"
    target_repo_name = os.environ.get("BACKPORT_TARGET_REPO")
    target_remote = os.environ.get("BACKPORT_TARGET_REMOTE")
    if not target_repo_name or not target_remote:
        print(
            "Please specify the target repositories for debugging, using the "
            "environment variables BACKPORT_TARGET_REPO (e.g. `timescale/timescaledb`) "
            "and BACKPORT_TARGET_REMOTE (e.g. `origin`).",
            file=sys.stderr,
        )
        sys.exit(1)

print(
    f"Will look at '{source_repo_name}' (git remote '{source_remote}') for bug fixes, "
    f"and create the backport PRs in '{target_repo_name}' (git remote '{target_remote}')."
)

source_repo = github.get_repo(source_repo_name)
target_repo = github.get_repo(target_repo_name)

# Set git name and email corresponding to the token user.
token_user = github.get_user()
os.environ["GIT_COMMITTER_NAME"] = token_user.name

# This is an email that is used by Github when you opt to hide your real email
# address. It is required so that the commits are recognized by Github as made
# by the user. That is, if you use a wrong e-mail, there won't be a clickable
# profile picture next to the commit in the Github interface.
os.environ[
    "GIT_COMMITTER_EMAIL"
] = f"{token_user.id}+{token_user.login}@users.noreply.github.com"
print(
    f"Will commit as {os.environ['GIT_COMMITTER_NAME']} <{os.environ['GIT_COMMITTER_EMAIL']}>"
)

# Fetch the main branch. Apparently the local repo can be shallow in some cases
# in Github Actions, so specify the depth. --unshallow will complain on normal
# repositories, this is why we don't use it here.
git_check(
    f"fetch --quiet --depth={HISTORY_DEPTH} {source_remote} main:refs/remotes/{source_remote}/main"
)

# Find out what is the branch corresponding to the previous version compared to
# main. We will backport to that branch.
version_config = dict(
    [
        re.match(r"^(.+)\s+=\s+(.+)$", line).group(1, 2)
        for line in git_output(f"show {source_remote}/main:version.config").splitlines()
        if line
    ]
)

previous_version = version_config["update_from_version"]
previous_version_parts = previous_version.split(".")
previous_version_parts[-1] = "x"
backport_target = ".".join(previous_version_parts)
backported_label = f"backported-{backport_target}"

print(f"Will backport to {backport_target}.")

# Fetch the target branch. Apparently the local repo can be shallow in some cases
# in Github Actions, so specify the depth. --unshallow will complain on normal
# repositories, this is why we don't use it here.
git_check(
    f"fetch --quiet --depth={HISTORY_DEPTH} {target_remote} {backport_target}:refs/remotes/{target_remote}/{backport_target}"
)

# Also fetch all branches from the target repository, because we use the presence
# of the backport branches to determine that a backport exists. It's not convenient
# to query for branch existence through the PyGithub API.
git_check(f"fetch {target_remote}")

# Find out which commits are unique to main and target branch. Also build sets of
# the titles of these commits. We will compare the titles to check whether a
# commit was backported.
main_commits = [
    line.split("\t")
    for line in git_output(
        f'log -{HISTORY_DEPTH} --pretty="format:%h\t%s" {target_remote}/{backport_target}..{source_remote}/main'
    ).splitlines()
    if line
]

print(f"Have {len(main_commits)} new commits in the main branch.")

branch_commits = [
    line.split("\t")
    for line in git_output(
        f'log -{HISTORY_DEPTH} --pretty="format:%h\t%s" {source_remote}/main..{target_remote}/{backport_target}'
    ).splitlines()
    if line
]
branch_commit_titles = {x[1] for x in branch_commits}

# We will do backports per-PR, because one PR, though not often, might contain
# many commits. So as the first step, go through the commits unique to main, find
# out which of them have to be backported, and remember the corresponding PRs.
# We also have to remember which commits to backport. The list from PR itself is
# not what we need, these are the original commits from the PR branch, and we
# need the resulting commits in master.
class PRInfo:
    """Information about the PR to be backported."""

    def __init__(self, pygithub_pr_, issue_number_):
        self.pygithub_pr = pygithub_pr_
        self.pygithub_commits = []
        self.issue_number = issue_number_


def should_backport_by_labels(pygithub_object):
    """Should we backport the given PR/issue, judging by the labels?
    Note that this works in ternary logic:
    True means we must,
    False means we must not (tags to disable backport take precedence),
    and None means weak no (no tags to either request or disable backport)"""
    labels = {label.name for label in pygithub_object.labels}
    stopper_labels = labels.intersection(
        ["disable-auto-backport", "auto-backport-not-done"]
    )
    if stopper_labels:
        print(
            f"#{pygithub_object.number} '{pygithub_object.title}' is labeled as '{list(stopper_labels)[0]}' which prevents automated backporting."
        )
        return False

    force_labels = labels.intersection(["bug", "force-auto-backport"])
    if force_labels:
        print(
            f"#{pygithub_object.number} '{pygithub_object.title}' is labeled as '{list(force_labels)[0]}' which requests automated backporting."
        )
        return True

    return None


# Go through the commits unique to main, and build a dict(pr number -> PRInfo)
# of PRs that we will consider for backporting.
prs_to_backport = {}
for commit_sha, commit_title in main_commits:
    print()

    pygithub_commit = source_repo.get_commit(sha=commit_sha)

    pulls = pygithub_commit.get_pulls()
    if not pulls:
        print(f"{commit_sha[:9]} '{commit_title}' does not belong to a PR.")
        continue

    if pulls.totalCount > 1:
        # What would that mean? Just play it safe and skip it.
        print(
            f"{commit_sha[:9]} '{commit_title}' references multiple PRs: {', '.join([pull.number for pull in pulls])}"
        )
        continue

    pull = pulls[0]

    # If a commit with the same title is already in the branch, mark the PR with
    # a corresponding tag. This makes it easier to check what was backported
    # when looking at the release milestone. Note that we do this before other
    # checks -- maybe it was backported manually regardless of the usual
    # conditions.
    if commit_title in branch_commit_titles:
        print(f"{commit_sha[:9]} '{commit_title}' is already in the branch.")

        if backported_label not in {label.name for label in pull.labels}:
            pull.add_to_labels(backported_label)

        continue

    # Next, we're going to look at the labels of both the PR and the linked
    # issue, if any, to understand whether we should backport the fix. We have
    # labels to request backport like "bug", and labels to prevent backport
    # like "disable-auto-backport", on both issue and the PR. We're going to use
    # the ternary False/None/True logic to combine them properly.
    issue_number = get_referenced_issue(pull.number)
    if not issue_number:
        should_backport_issue_ternary = None
        print(
            f"{commit_sha[:9]} belongs to the PR #{pull.number} '{pull.title}' that does not close an issue."
        )
    else:
        issue = source_repo.get_issue(number=issue_number)
        should_backport_issue_ternary = should_backport_by_labels(issue)
        print(
            f"{commit_sha[:9]} belongs to the PR #{pull.number} '{pull.title}' "
            f"that references the issue #{issue.number} '{issue.title}'."
        )
    should_backport_pr_ternary = should_backport_by_labels(pull)

    # We backport if either the PR or the issue labels request the backport, and
    # none of them prevent it. I'm writing it with `is True` because I don't
    # remember python rules for ternary logic with None (do you?).
    if (
        should_backport_pr_ternary is True or should_backport_issue_ternary is True
    ) and (
        should_backport_pr_ternary is not False
        and should_backport_issue_ternary is not False
    ):
        print(f"{commit_sha[:9]} '{commit_title}' will be considered for backporting.")
    else:
        continue

    # Remember the PR and the corresponding resulting commit in main.
    if pull.number not in prs_to_backport:
        prs_to_backport[pull.number] = PRInfo(pull, issue_number)

    # We're traversing the history backwards, and want to have the list of
    # commits in forward order.
    prs_to_backport[pull.number].pygithub_commits.insert(0, pygithub_commit)


def report_backport_not_done(original_pr, reason, details=None):
    """If something prevents us from backporting the PR automatically,
    report it in a comment to original PR, and add a label preventing
    further attempts."""
    print(
        f"Will not backport the PR #{original_pr.number} '{original_pr.title}': {reason}"
    )

    github_comment = f"Automated backport to {backport_target} not done: {reason}."

    if details:
        github_comment += f"\n\n{details}"

    # Link to the job if we're running in the Github Action environment.
    if "GITHUB_REPOSITORY" in os.environ:
        github_comment += (
            "\n\n"
            f"[Job log](https://github.com/{os.environ.get('GITHUB_REPOSITORY')}"
            f"/actions/runs/{os.environ.get('GITHUB_RUN_ID')}"
            f"/attempts/{os.environ.get('GITHUB_RUN_ATTEMPT')})"
        )

    original_pr.create_issue_comment(github_comment)
    original_pr.add_to_labels("auto-backport-not-done")


# Now, go over the list of PRs that we have collected, and try to backport
# each of them.
print(f"Have {len(prs_to_backport)} PRs to backport.")
for index, pr_info in enumerate(prs_to_backport.values()):
    print()

    # Don't want to have an endless loop that modifies the repository in an
    # unattended script. The already backported/conflicted PRs shouldn't even
    # get into this list, so the low number is OK, it will still make progress.
    if index > 5:
        print(f"{index} PRs processed, stopping as a precaution.")
        sys.exit(0)

    original_pr = pr_info.pygithub_pr
    backport_branch = f"backport/{backport_target}/{original_pr.number}"

    # If there is already a backport branch for this PR, this probably means
    # that we already created the backport PR. Skip it.
    if (
        git_returncode(f"rev-parse {target_remote}/{backport_branch} > /dev/null 2>&1")
        == 0
    ):
        print(
            f'Backport branch {backport_branch} for PR #{original_pr.number}: "{original_pr.title}" already exists. Skipping.'
        )
        continue

    # Try to cherry-pick the commits.
    git_check(
        f"checkout --quiet --detach {target_remote}/{backport_target} > /dev/null"
    )

    commit_shas = [commit.sha for commit in pr_info.pygithub_commits]
    if git_returncode(f"cherry-pick --quiet -m 1 -x {' '.join(commit_shas)}") != 0:
        details = f"### Git status\n\n```\n{git_output('status')}\n```"
        git_check("cherry-pick --abort")
        report_backport_not_done(original_pr, "cherry-pick failed", details)
        continue

    # We don't have the permission to modify workflows
    changed_files = {file.filename for file in original_pr.get_files()}
    changed_workflow_files = {
        filename
        for filename in changed_files
        if filename.startswith(".github/workflows/")
    }
    if changed_workflow_files:
        details = (
            f"The PR touches a workflow file '{list(changed_workflow_files)[0]}' "
            " and cannot be backported automatically"
        )
        report_backport_not_done(original_pr, "backport failed", details)
        continue

    # Push the backport branch.
    git_check(f"push --quiet {target_remote} @:refs/heads/{backport_branch}")

    # Prepare description for the backport PR.
    backport_description = (
        f"This is an automated backport of #{original_pr.number}: {original_pr.title}."
    )

    if pr_info.issue_number:
        backport_description += f"\nThe original issue is #{pr_info.issue_number}."

    # Do not merge the PR automatically if it changes some particularly
    # conflict-prone files that are better to review manually. Also mention this
    # in the description.
    stopper_files = changed_files.intersection(
        ["sql/updates/latest-dev.sql", "sql/updates/reverse-dev.sql"]
    )
    if stopper_files:
        backport_description += (
            "\n"
            f"This PR will not be merged automatically, because it modifies '{list(stopper_files)[0]}' "
            "which is conflict-prone. Please review these changes manually."
        )
    else:
        backport_description += (
            "\n"
            "This PR will be merged automatically after all the relevant CI checks pass."
        )

    backport_description += (
        " If this fix should not be backported, or will be backported manually, "
        "just close this PR. You can use the backport branch to add your "
        "changes, it won't be modified automatically anymore."
        "\n"
        "\n"
        "For more details, please see the [documentation]"
        "(https://github.com/timescale/eng-database/wiki/Releasing-TimescaleDB#automated-cherry-picking-of-bug-fixes)"
    )

    # Add original PR description. Comment out the Github issue reference
    # keywords like 'Fixes #1234', to avoid having multiple PRs saying they fix
    # a given issue. The backport PR is going to reference the fixed issue as
    # "Original issue #xxxx".
    original_description = re.sub(
        r"((fix|clos|resolv)[esd]+)(\s+#[0-9]+)",
        r"`\1`\3",
        original_pr.body,
        flags=re.IGNORECASE,
    )
    backport_description += (
        "\n"
        "\n"
        "## Original description"
        "\n"
        f"### {original_pr.title}"
        "\n"
        f"{original_description}"
    )

    # Create the backport PR.
    backport_pr = target_repo.create_pull(
        title=f"Backport to {backport_target}: #{original_pr.number}: {original_pr.title}",
        body=backport_description,
        head=backport_branch,
        base=backport_target,
    )
    backport_pr.add_to_labels("is-auto-backport")
    backport_pr.add_to_assignees(original_pr.user.login)
    if not stopper_files:
        set_auto_merge(backport_pr.number)

    print(
        f"Created backport PR #{backport_pr.number} for #{original_pr.number}: {original_pr.title}"
    )
