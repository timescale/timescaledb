#!/usr/bin/env python3

import sys
import re
import os

import github  # this is PyGithub.

import requests
import string


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


def get_referenced_issues(pr_number):
    """Get the numbers of issue fixed by the given pull request."""

    ref_result = run_query(
        string.Template(
            """
        query {
            repository(owner: "timescale", name: "timescaledb") {
              pullRequest(number: $pr_number) {
                closingIssuesReferences(first: 100) {
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

    if not ref_edges:
        return []

    return [edge["node"]["number"] for edge in ref_edges if edge]


# Check if a line matches any of the specified patterns
def is_valid_line(line):
    patterns = [r"^Fixes:\s*.*$", r"^Implements:\s*.*$", r"^Thanks:\s*.*$"]
    for pattern in patterns:
        if re.match(pattern, line):
            return True
    return False


def main():
    github_token = os.environ.get("GITHUB_TOKEN")

    if not github_token:
        print("Please populate the GITHUB_TOKEN environment variable.")
        sys.exit(1)

    github_obj = github.Github(github_token)
    repo = github_obj.get_repo("timescale/timescaledb")
    # Get the file name from the command line argument
    if len(sys.argv) != 2:
        print("Please provide a file name as a command-line argument.")
        sys.exit(1)

    file_name = sys.argv[1]
    this_pr_number = int(os.environ["PR_NUMBER"])
    pr_issues = set(get_referenced_issues(this_pr_number))

    # Read the file and check non-empty lines
    changelog_issues = set()
    with open(file_name, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not is_valid_line(line):
                print(f'Invalid entry in change log: "{line}"')
                sys.exit(1)

            # The referenced issue number should be valid.
            for issue_number in re.findall("#([0-9]+)", line):
                issue_number = int(issue_number)
                try:
                    issue = repo.get_issue(number=issue_number)
                except github.UnknownObjectException:
                    print(
                        f"The changelog entry references an invalid issue #{issue_number}:\n{line}"
                    )
                    sys.exit(1)

                as_pr = None
                try:
                    as_pr = issue.as_pull_request()
                except github.UnknownObjectException:
                    # Not a pull request
                    pass

                # Accept references to PR itself.
                if as_pr:
                    if issue_number != this_pr_number:
                        print(
                            f"The changelog for PR #{this_pr_number} references another PR #{issue_number}"
                        )
                        sys.exit(1)
                    changelog_issues = pr_issues
                else:
                    changelog_issues.add(issue_number)

    if changelog_issues != pr_issues:
        print(
            "Instead of "
            + (f"the issues {pr_issues}" if pr_issues else "no issues")
            + f" linked to the PR #{this_pr_number}, the changelog references "
            + (f"the issues {changelog_issues}" if changelog_issues else "no issues")
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
