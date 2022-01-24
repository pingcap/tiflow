<!-- Lots of ideas references from https://github.com/kubernetes/community/blob/master/contributors/devel/sig-release/cherry-picks.md, Thanks!-->

# Overview

This document explains how cherry picks are managed on release branches within the `pingcap/tiflow` repository. A common
use case for this task is backporting PRs from master to release branches.

- [Prerequisites](#prerequisites)
- [What Kind of PRs are Good for Cherry Picks](#what-kind-of-prs-are-good-for-cherry-picks)
- [Initiate a Cherry Pick](#initiate-a-cherry-pick)
- [Cherry Pick Review](#cherry-pick-review)
- [Searching for Cherry Picks](#searching-for-cherry-picks)
- [Troubleshooting Cherry Picks](#troubleshooting-cherry-picks)
- [Batch Merging Cherry Picks](#batch-merging-cherry-picks)

## Prerequisites

- A pull request merged against the `master` branch.
- The release branch exists (example: [`release-5.0`](https://github.com/pingcap/tiflow/tree/release-5.0))

## What Kind of PRs are Good for Cherry Picks

The emphasis is on critical bug fixes, e.g.,

- No workaround to the issue
- Loss of data
- Memory corruption
- Panic, crash, hang
- Security

Some not so critical but important matters, e.g.,

- Tests
- Metrics And Logging
- CI Improvements

A bugfix for a functional issue (not a data loss or security issue) does not qualify as a critical bug fix.

If you are proposing a cherry pick and it is not a clear and obvious critical bug fix, please reconsider. If upon
reflection you wish to continue, bolster your case by supplementing your PR with e.g.,

- A GitHub issue detailing the problem

- Scope of the change

- Risks of adding a change

- Risks of associated regression

- Testing performed, test cases added

## Initiate a Cherry Pick

- Add `needs-cherry-pick-release-{version}` label to the PR, the robot will then automatically create a new PR for the
  cherry pick.
- Your cherry pick PR will immediately get the `do-not-merge/cherry-pick-not-approved` label.

## Cherry Pick Review

Unlike other PRs, cherry pick PR will directly copy the LGTMs of the original PR, but we still require `/merge` to
trigger the merge PR.

The same release note requirements apply as normal pull requests, except the release note stanza will auto-populate from
the master branch pull request from which the cherry pick originated.

If this is unsuccessful, the `do-not-merge/release-note-label-needed` label will be applied and the cherry pick author
must edit the pull request description to add a release note or include in a comment the `/release-note-none` command.

Cherry pick pull requests are reviewed slightly differently than normal pull requests on the `master` branch in that
they:

- Are by default expected to be `type/bug-fix`.

- The original change to the `master` branch is expected to be merged for some time and no related CI failures or test
  flakiness must be discovered.

- Milestones must be set on the PR reflecting the milestone for the target release branch (for example, milestone v5.3.2
  for a cherry pick onto branch
  `release-5.3`). This is normally done for you by manually.

- A separate cherry pick pull request should be open for every applicable target branch. This ensures that the fix will
  be present on every active branch for a given set of patch releases. If a fix is only applicable to a subset of active
  branches, it is helpful to note why that is the case on the parent pull request or on the cherry pick pull requests to
  the applicable branches.

- Have one additional level of review in that they must be approved specifically for cherry pick by branch approvers.

  Approval is signified by an approver manually applying the
  `cherry-pick-approved` label. This action removes the
  `do-not-merge/cherry-pick-not-approved` label and triggers a merge into the target branch.

  If you are concerned about the status of your cherry pick, err on the side of overcommunicating and reach out to the
  [nongfushanquan](https://github.com/nongfushanquan). We should
  contact [nongfushanquan](https://github.com/nongfushanquan) as early as possible for approval.

## Searching for Cherry Picks

Examples (based on cherry picks targeting the `release-5.3` branch):

- [`cherry-pick-approved`](https://github.com/pingcap/tiflow/pulls?q=is%3Aopen+is%3Apr+label%3Acherry-pick-approved+base%3Arelease-5.3)
- [`do-not-merge/cherry-pick-not-approved`](https://github.com/pingcap/tiflow/pulls?q=is%3Aopen+is%3Apr+label%3Ado-not-merge%2Fcherry-pick-not-approved+base%3Arelease-5.3)

## Troubleshooting Cherry Picks

Contributors may encounter some of the following difficulties when initiating a cherry pick.

- A cherry pick PR does not apply cleanly against an old release branch. In that case, you will need to manually fix
  conflicts.

- The cherry pick PR includes code that does not pass CI tests. Please re-trigger the test and submit an unstable test
  report [in this issue](https://github.com/pingcap/tiflow/issues/2246).

## Batch Merging Cherry Picks

If you have created a large number of cherry picks for the same module over time, we recommend that you merge these PRs
in batches.

- Manually merge your cherry pick PRs into a new PR.

  Please use the title of the original cherry pick PR as the commit message for the new PR. For example, if you merge 5
  PRs, use the titles of these 5 PRs as the 5 commit messages for this new PR.

  Please use `<subsystem>(ticdc|dm|both): rollup cherry picks` as the title of the PR.

  Please make it clear in the first line of your PR description what PRs this PR merges with, and what links GitHub
  recognizes. For example, `Cherrypick #8586, #8623, #8638 and #8655`.

- Add the `tide/merge-method-rebase` label, **which will let the bot keep the commits for merging instead of suqashing
  all of them together**.
