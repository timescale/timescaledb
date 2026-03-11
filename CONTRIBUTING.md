# Contributing to TimescaleDB

We appreciate any help the community can provide to make TimescaleDB better!  

You can help in different ways:

* Open an [issue](https://github.com/timescale/timescaledb/issues) with a
  bug report, build issue, feature request, suggestion, etc.

* Fork this repository and submit a pull request

For any particular improvement you want to make, it can be beneficial to
begin discussion on the GitHub issues page. This is the best place to
discuss your proposed improvement (and its implementation) with the core
development team.

Before we accept any code contributions, Timescale contributors need to
sign the [Contributor License Agreement](https://cla-assistant.io/timescale/timescaledb) (CLA). By signing a CLA, we can
ensure that the community is free and confident in its ability to use your
contributions.

## Getting and building TimescaleDB

Please follow our README for [instructions on installing from source](https://github.com/timescale/timescaledb/blob/main/docs/BuildSource.md).

## Style guide

Before submitting any contributions, please ensure that it adheres to
our [Style Guide](https://github.com/timescale/timescaledb/blob/main/docs/StyleGuide.md).

## Code review workflow

* Sign the [Contributor License Agreement](https://cla-assistant.io/timescale/timescaledb) (CLA) if you're a new contributor.

* Develop on your local branch:

    * Fork the repository and create a local feature branch to do work on,
      ideally on one thing at a time.  Don't mix bug fixes with unrelated
      feature enhancements or stylistical changes.

    * Hack away. Add tests for non-trivial changes.

    * Run the [test suite](#testing) and make sure everything passes.

    * When committing, be sure to write good commit messages according to [these
      seven rules](https://chris.beams.io/posts/git-commit/#seven-rules). Doing 
      `git commit` prints a message if any of the rules is violated. 
      Stylistically,
      we use commit message titles in the imperative tense, e.g., `Add
      merge-append query optimization for time aggregate`.  In the case of
      non-trivial changes, include a longer description in the commit message
      body explaining and detailing the changes.  That is, a commit message
      should have a short title, followed by a empty line, and then
      followed by the longer description.

    * When committing, link which GitHub issue of [this 
      repository](https://github.com/timescale/timescaledb/issues) is fixed or 
      closed by the commit with a [linking keyword recognised by 
      GitHub](https://docs.github.com/en/github/managing-your-work-on-github/linking-a-pull-request-to-an-issue#linking-a-pull-request-to-an-issue-using-a-keyword). 
      For example, if the commit fixes bug 123, add a line at the end of the 
      commit message with  `Fixes #123`, if the commit implements feature 
      request 321, add a line at the end of the commit message `Closes #321`.
      This will be recognized by GitHub. It will close the corresponding issue 
      and place a hyperlink under the number.

* Push your changes to an upstream branch:

    * Make sure that each commit in the pull request will represent a
      logical change to the code, will compile, and will pass tests.

    * Make sure that the pull request message contains all important 
      information from the commit messages including which issues are
      fixed and closed. If a pull request contains one commit only, then
      repeating the commit message is preferred, which is done automatically
      by GitHub when it creates the pull request.

    * Rebase your local feature branch against main (`git fetch origin`,
      then `git rebase origin/main`) to make sure you're
      submitting your changes on top of the newest version of our code.

    * When finalizing your PR (i.e., it has been approved for merging),
      aim for the fewest number of commits that
      make sense. That is, squash any "fix up" commits into the commit they
      fix rather than keep them separate. Each commit should represent a
      clean, logical change and include a descriptive commit message.

    * Push your commit to your upstream feature branch: `git push -u <yourfork> my-feature-branch`

* Create and manage pull request:

    * [Create a pull request using GitHub](https://help.github.com/articles/creating-a-pull-request).
      If you know a core developer well suited to reviewing your pull
      request, either mention them (preferably by GitHub name) in the PR's
      body or [assign them as a reviewer](https://help.github.com/articles/assigning-issues-and-pull-requests-to-other-github-users/).

    * If you get a test failure in the CI, check them under [Github Actions](https://github.com/timescale/timescaledb/actions)

    * Address feedback by amending your commit(s). If your change contains
      multiple commits, address each piece of feedback by amending that
      commit to which the particular feedback is aimed.

    * The PR is marked as accepted when the reviewer thinks it's ready to be
      merged.  Most new contributors aren't allowed to merge themselves; in
      that case, we'll do it for you.

## Testing

Every non-trivial change to the code base should be accompanied by a
relevant addition to or modification of the test suite.

Please check that the full test suite (including your test additions
or changes) passes successfully on your local machine **before you
open a pull request**.

If you are running locally:
```bash
# Use Debug build mode for full battery of tests
./bootstrap -DCMAKE_BUILD_TYPE=Debug
cd build && make
make installcheck
```

All submitted pull requests are also automatically
run against our test suite via [Github Actions](https://github.com/timescale/timescaledb/actions)
(that link shows the latest build status of the repository).

## Reviewing and accepting your contribution

We appreciate everyone who is investing time in contributing to TimescaleDB and regret that we sometimes have to reject contributions even when they might appear to add value. 
If the contribution is accepted, we will merge the changes, acknowledge your contribution in the release, and take care of the backporting to the relevant branches, if necessary.
Before you start, please discuss your change in a GitHub issue before spending much time on its implementation. We sometimes have to reject contributions that duplicate other efforts, take the wrong approach to solving a problem, or solve a problem which does not need solving. An up-front discussion often saves your time.

A contribution is expected to address one specific change. Pull requests are expected to be small, if a PR is deemed too large or touches too many disparate parts of the system, you will be required to break it down into a series of smaller, digestible PRs before reviewing continues. Avoid adding unnecessary stylistic changes.

TimescaleDB is complex system, requiring a deep understanding of Postgres internals, the system architecture and the potential secondary effects of changes.
While we highly value community input, the core development team's primary responsibility is maintaining the health of the project. 
We reserve the right to respectfully close the PR. This is not a reflection of your skills as an engineer, but rather a necessity of resource allocation to keep the project stable and performant.

We reserved the right to reject contributions, if the time required on reviews would outweigh the benefits of a change by preventing us from working on other beneficial changes instead.

We sometimes reject contributions due to the low quality of the submission since low-quality submissions tend to take unreasonable effort to review properly. 
Quality is rather subjective so it is hard to describe exactly how to avoid this, but there are some basic steps you can take to reduce the chances of rejection:

* Unit Tests: Every new function or modified logic path must have accompanying unit tests.
* Integration Tests: Features that touch the storage engine, query planner, or sub system must include integration tests.
* Edge Cases: You are expected to proactively test for edge cases, concurrency hazards, and out-of-memory (OOM) scenarios.
* No regressions: Your code must pass all existing CI/CD pipelines, including e.g. fuzzing, without degrading current metrics.
* Style Guide: Your code must be formatted according to the projects [style guide](#style-guide).

We welcome the use of AI coding assistants (Copilot, Gemini, etc.) to enhance your productivity. However, AI-generated contributions adhere to the exact same rigorous standards as human-written code. You are fully responsible for the accuracy, safety, and performance of any AI-generated code you submit.
If a PR appears to be low quality AI outputs without reflecting a proficient understanding of the change, we will close the PR immediately.

We expect you to follow up on review comments, but recognise that everyone has many priorities for their time and may not be able to respond for several days. We will understand if you find yourself without the time to complete your contribution, but please let us know that you have stopped working on it and we can conclude how to handle the contribution.
After two weeks of inactivity, we will reject the contribution, unless we complete it. 

If your contribution is rejected, we will close the pull request with a comment explaining why.

## License headers

We require license headers on all C and SQL files, unless explicitly instructed otherwise.

All C and SQL files in the [tsl](https://github.com/timescale/timescaledb/tree/main/tsl) directory require the following short license header of the [Timescale License Agreement](https://github.com/timescale/timescaledb/blob/main/tsl/LICENSE-TIMESCALE):

```
/*
 * This file and its contents are licensed under the Timescale License.
 * Please see the included NOTICE for copyright information and
 * LICENSE-TIMESCALE for a copy of the license.
 */
```

All C and SQL files in the [src](https://github.com/timescale/timescaledb/tree/main/src) directory require the following short license header of the [Apache License](https://github.com/timescale/timescaledb/blob/main/LICENSE-APACHE):

```
/*
 * This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE-APACHE for a copy of the license.
 */
```
