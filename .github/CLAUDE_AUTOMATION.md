# Claude Automation

This document describes the Claude-powered automation workflows for analyzing test failures and responding to PR review feedback.

## Overview

Two automated workflows use Claude Code to assist with maintenance tasks:

| Workflow | Purpose | Trigger |
|----------|---------|---------|
| **Nightly Test Fix** | Analyze CI failures and create fix PRs | Failed nightly tests or manual |
| **PR Review Handler** | Address review feedback on Claude PRs | PR review submitted |

Both workflows:
- Use a GitHub App for authentication (not personal tokens)
- Are disabled by default in forks (require explicit opt-in)
- Only operate on PRs they created (author verification)
- Refuse to modify workflow files (`.github/workflows/`)

---

## Nightly Test Fix

### What It Does

1. Detects failed nightly CI runs
2. Downloads failure artifacts (logs, regression diffs, stack traces)
3. Invokes Claude Code to analyze failures and implement fixes
4. Creates a separate PR for each identified test failure
5. Optionally sends Slack notifications

### Design

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Nightly Tests  │────▶│  workflow_call   │────▶│  Claude Code    │
│  (scheduled)    │     │  (on failure)    │     │  analyzes &     │
└─────────────────┘     └──────────────────┘     │  creates PRs    │
                                                  └─────────────────┘
```

The nightly fix workflow is called directly from the Regression workflow when tests fail, using `workflow_call`. This approach:
- Only triggers on actual failures (no phantom workflow runs)
- Keeps the fix attempt visible in the same workflow run
- Passes the run ID directly without needing to query the API

**Workflow file**: `.github/workflows/claude-nightly-fix.yaml`
**Script**: `.github/scripts/claude-analyze-failures.sh`
**Caller**: `.github/workflows/linux-build-and-test.yaml` (trigger-claude-fix job)

### Triggers

| Trigger | Description |
|---------|-------------|
| `workflow_call` | Called by Regression workflow when nightly tests fail |
| `workflow_dispatch` | Manual trigger with required `run_id` input |

### Duplicate Prevention

Before creating a fix, the script checks for existing open PRs with:
- The `claude-code` label
- Title or body mentioning the same test name

This prevents creating duplicate PRs for the same failure.

---

## PR Review Handler

### What It Does

1. Triggered on PR review submissions (not standalone comments)
2. Checks PR has `claude-code` label and review state is `changes_requested` or `commented`
3. Verifies PR author matches the GitHub App bot
4. Fetches review comments and feedback
5. Invokes Claude Code to address the feedback
6. Commits and pushes changes, or posts a comment if no changes needed

Note: This only triggers on formal review submissions (when reviewer clicks "Submit review"), not on inline diff comments or general PR conversation comments.

### Design

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Review on PR   │────▶│  Label check     │────▶│  Author check    │────▶│  Claude Code    │
│                 │     │  (claude-code)   │     │  (via app-slug)  │     │  addresses      │
└─────────────────┘     └──────────────────┘     └──────────────────┘     │  feedback       │
                                                                           └─────────────────┘
```

The label check allows users to disable review handling on a PR by removing the `claude-code` label.

**Workflow file**: `.github/workflows/claude-pr-review-handler.yaml`
**Script**: `.github/scripts/claude-handle-pr-review.sh`

### Triggers

| Trigger | Description |
|---------|-------------|
| `pull_request_review` | Fires when review submitted on PR with `claude-code` label |
| `workflow_dispatch` | Manual trigger with required `pr_number` input |

**Note**: Manual triggers (`workflow_dispatch`) bypass reviewer trust checks since the person triggering the workflow is assumed to be authorized. Author verification still applies.

For automatic triggers (`pull_request_review`):
- PR must have `claude-code` label (user-controllable filter)
- Review state must be `changes_requested` or `commented`
- PR author must match the GitHub App bot (verified using app-slug)
- Reviewer must NOT be the bot itself (prevents infinite loops)
- Reviewer must have trusted association: OWNER, MEMBER, or COLLABORATOR

---

## Configuration

### Repository Variables

Set these in **Settings > Secrets and variables > Actions > Variables**:

| Variable | Workflow | Required | Description |
|----------|----------|----------|-------------|
| `CLAUDE_NIGHTLY_FIX_SOURCE_REPOSITORY` | Nightly Fix | Yes | Source repository (e.g., `timescale/timescaledb`) |
| `CLAUDE_PR_REVIEW_HANDLER_ENABLED` | PR Review | Yes | Set to any value to enable |
| `CLAUDE_GIT_AUTHOR_NAME` | Both | No | Git author name (default: `<app-slug>[bot]`) |
| `CLAUDE_GIT_AUTHOR_EMAIL` | Both | No | Git author email (default: `<app-id>+<app-slug>[bot]@users.noreply.github.com`) |

### Repository Secrets

Set these in **Settings > Secrets and variables > Actions > Secrets**:

| Secret | Required | Description |
|--------|----------|-------------|
| `ANTHROPIC_API_KEY` | Yes | API key for Claude |
| `CLAUDE_APP_ID` | Yes | GitHub App ID |
| `CLAUDE_APP_PRIVATE_KEY` | Yes | GitHub App private key (PEM format) |
| `CLAUDE_SLACK_BOT_TOKEN` | No | Slack bot token for notifications |
| `CLAUDE_SLACK_CHANNEL` | No | Slack channel ID for notifications |

---

## GitHub App Setup

### Creating the App

1. Go to **Settings > Developer settings > GitHub Apps > New GitHub App**
2. Configure:
   - **Name**: Choose a descriptive name (e.g., `timescale-claude-automation`)
   - **Homepage URL**: Repository URL
   - **Webhook**: Disable (not needed)

### Required Permissions

| Permission | Access | Reason |
|------------|--------|--------|
| **Actions** | Read-only | Fetch workflow runs, jobs, and artifacts |
| **Contents** | Read & Write | Push commits to branches |
| **Pull requests** | Read & Write | Create PRs, add labels, post comments |
| **Metadata** | Read-only | Required for API access |

### Installation

1. Install the app on the target repository
2. Note the **App ID** (visible on app settings page)
3. Generate a **Private Key** (PEM file)
4. Add both as repository secrets

### Bot Username

The bot username is automatically derived from the app slug:
- App slug: `timescale-claude-automation`
- Bot username: `timescale-claude-automation[bot]`

This is used for author verification in the PR review handler.

---

## Local Testing

### Manual Workflow Triggers

Both workflows support manual triggering via GitHub UI or CLI:

```bash
# Nightly Test Fix - provide a failed workflow run ID
gh workflow run claude-nightly-fix.yaml -f run_id=12345678

# PR Review Handler - provide a PR number
gh workflow run claude-pr-review-handler.yaml -f pr_number=123
```

When manually triggered, the workflows fetch missing context from the GitHub API.

### Running Scripts Directly

For faster iteration during development, run the scripts directly without the workflow.

#### Prerequisites

- `gh` CLI authenticated (`gh auth login`)
- `claude` CLI installed and authenticated
- `jq` installed

### Nightly Test Fix Script

```bash
# Required environment variables
export SOURCE_REPOSITORY="timescale/timescaledb"
export ANALYZE_RUN_ID="12345678"  # Failed workflow run ID
export CLAUDE_BOT_USERNAME="github-actions[bot]"  # Or your app's bot name

# Optional: test without creating PRs
export DRY_RUN=true      # Show context only, don't invoke Claude
export SKIP_PR=true      # Make changes but don't create PRs
export KEEP_WORK_DIR=true  # Preserve /tmp/claude-fix-* for inspection

# Run the script
.github/scripts/claude-analyze-failures.sh
```

### PR Review Handler Script

```bash
# Required: PR number and bot username
export CLAUDE_BOT_USERNAME="github-actions[bot]"  # Must match PR author

# Optional overrides
export PR_REPOSITORY="timescale/timescaledb"
export DRY_RUN=true      # Show context only
export SKIP_PUSH=true    # Make changes but don't push
export KEEP_WORK_DIR=true

# Run with PR number as argument
.github/scripts/claude-handle-pr-review.sh 123

# Or via environment variable
PR_NUMBER=123 .github/scripts/claude-handle-pr-review.sh
```

### Environment Variables Reference

#### claude-analyze-failures.sh

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SOURCE_REPOSITORY` | Yes | - | Repository to fetch artifacts from |
| `ANALYZE_RUN_ID` | Yes | - | Workflow run ID to analyze (preferred over `GITHUB_RUN_ID`) |
| `CLAUDE_BOT_USERNAME` | Yes | - | Bot username for identification |
| `GITHUB_TOKEN` | No | `gh auth token` | GitHub token for API calls |
| `TARGET_REPOSITORY` | No | `SOURCE_REPOSITORY` | Repository to create PRs in |
| `BASE_BRANCH` | No | `main` | Branch to create PRs against |
| `CLAUDE_MODEL` | No | `claude-sonnet-4-20250514` | Claude model to use |
| `MAX_ARTIFACTS` | No | `10` | Max artifacts to download |
| `DRY_RUN` | No | `false` | Skip Claude and PR creation |
| `SKIP_PR` | No | `false` | Make changes but skip PR creation |
| `KEEP_WORK_DIR` | No | `false` | Keep temp directory after completion |
| `ANALYSIS_OUTPUT_DIR` | No | - | Copy analysis output to this directory |
| `SLACK_BOT_TOKEN` | No | - | Slack token for notifications |
| `SLACK_CHANNEL` | No | - | Slack channel ID |

#### claude-handle-pr-review.sh

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `PR_NUMBER` | Yes | - | PR number (or pass as first argument) |
| `CLAUDE_BOT_USERNAME` | Yes | - | Expected PR author (bot username) |
| `GITHUB_TOKEN` | No | `gh auth token` | GitHub token for API calls |
| `GITHUB_REPOSITORY` | No | From git remote | Repository in `owner/repo` format |
| `PR_REPOSITORY` | No | `GITHUB_REPOSITORY` | Repository where PR lives |
| `PUSH_REPOSITORY` | No | Auto-detected | Repository to push to (for forks) |
| `PR_BRANCH` | No | From PR | Branch name |
| `PR_AUTHOR` | No | From PR | PR author login |
| `REVIEWER` | No | From review | Reviewer for commit message |
| `CLAUDE_MODEL` | No | `claude-sonnet-4-20250514` | Claude model to use |
| `DRY_RUN` | No | `false` | Show context only |
| `SKIP_PUSH` | No | `false` | Make changes but don't push |
| `KEEP_WORK_DIR` | No | `false` | Keep temp directory |

---

## Security Considerations

### Workflow Permissions

Both workflows follow the principle of least privilege for GitHub Actions permissions:

```yaml
# Workflow level - deny all by default
permissions: {}

jobs:
  job-name:
    # Job level - request only what's needed
    permissions:
      contents: write       # Push commits
      pull-requests: write  # Create PRs, post comments
```

This approach:
- Defaults to no permissions at the workflow level (`permissions: {}`)
- Each job explicitly declares only the permissions it needs
- Prevents accidental token privilege escalation
- Makes permission requirements visible and auditable

### Authentication & Authorization

| Aspect | Implementation |
|--------|----------------|
| **Token type** | GitHub App installation tokens (not PATs) |
| **Token scope** | Limited to installed repositories |
| **Token lifetime** | Short-lived (1 hour max) |

### Access Controls

| Control | Description |
|---------|-------------|
| **Opt-in required** | Workflows require explicit repository variables to run |
| **Label filter** | PR review handler only runs on PRs with `claude-code` label (user can remove to disable) |
| **Author verification** | PR review handler verifies PR author matches GitHub App bot (using app-slug) |
| **Reviewer trust** | PR review handler only responds to reviews from OWNER, MEMBER, or COLLABORATOR |
| **Defense in depth** | Author verified in both workflow step and script |
| **Fork isolation** | Workflows don't run in forks without explicit configuration |

### Code Modification Restrictions

| Restriction | Reason |
|-------------|--------|
| **No workflow edits** | Scripts refuse to modify `.github/workflows/` files |
| **Scoped changes** | PR review handler only addresses feedback within PR scope |

### Audit Trail

| Event | Tracking |
|-------|----------|
| **PR creation** | PRs include link to triggering workflow run |
| **Commits** | All commits include `Co-Authored-By: Claude` |
| **Labels** | `claude-code` label identifies automated PRs |
| **Artifacts** | Analysis output saved as workflow artifacts |

### Threat Model Considerations

| Threat | Mitigation |
|--------|------------|
| **PR impersonation** | Author verified using app-slug from token generation (cannot be spoofed) |
| **Infinite loops** | Reviewer checked to not be the bot itself before processing |
| **Untrusted reviewers** | Only OWNER, MEMBER, or COLLABORATOR can trigger review handling |
| **Prompt injection via PR** | Claude operates with limited tools and cannot modify workflows |
| **Token leakage** | Tokens are short-lived and scoped to specific permissions |
| **Unauthorized access** | Requires both API key and GitHub App credentials |

### Recommendations for Security Review

1. **Review GitHub App permissions** - Ensure only necessary permissions are granted
2. **Audit repository variables** - Verify `SOURCE_REPOSITORY` points to expected repo
3. **Monitor PR activity** - Watch for unexpected PRs with `claude-code` label
4. **Review Claude output** - Artifacts contain full Claude analysis for audit
5. **Slack notifications** - Enable to get alerts when PRs are created

---

## Troubleshooting

### Workflow Not Running

1. Check that required repository variables are set
2. Verify secrets are configured (workflow will skip silently if missing)
3. For nightly fix: ensure the `trigger-claude-fix` job exists in `linux-build-and-test.yaml`

### Author Verification Failing

1. Verify `CLAUDE_BOT_USERNAME` matches the GitHub App's bot username
2. Bot username format: `<app-slug>[bot]` (e.g., `my-app[bot]`)
3. Check PR author in GitHub UI matches expected bot

### Local Testing Issues

1. Ensure `gh auth login` is completed
2. Verify `claude` CLI is authenticated (`claude --version`)
3. Check `CLAUDE_BOT_USERNAME` matches the PR author you're testing against

### PR Creation Failing

1. Check GitHub App has `contents: write` and `pull-requests: write` permissions
2. Verify app is installed on the target repository
3. Review workflow logs for specific error messages

### Source and Target Commits Differ (Warning)

When using `TARGET_REPOSITORY` different from `SOURCE_REPOSITORY`, the script warns if the repos are at different commits. This is informational only - the failed workflow may have run on a commit that differs from both repos' current HEAD. Consider syncing your fork if fixes don't apply cleanly.
