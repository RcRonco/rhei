---
name: GPG signing fallback
description: 1Password git signing can fail in worktree agents - use -c commit.gpgsign=false as fallback
type: feedback
---

When committing in worktree agent branches, 1Password SSH/GPG signing frequently fails with "failed to fill whole buffer" or "agent returned an error". Use `git -c commit.gpgsign=false` as a fallback.

**Why:** 1Password agent may not be accessible from worktree context.
**How to apply:** Try normal commit first, if it fails with 1Password error, retry with `-c commit.gpgsign=false`.
