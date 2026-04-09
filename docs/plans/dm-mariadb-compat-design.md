DM MariaDB Compatibility Design Document

Last updated: 2026-04-09
Related branch: mariadbcompat-master-rebuild
Related task board item: MySQL/MariDB compability in DM Capability Evolution Task Board

1. Executive Summary

MariaDB is listed as a supported upstream for DM, but current tasks show that support has gaps in several independent dimensions: SQL compatibility, runtime stability, checker and operator UX, and integration or CI coverage. This document defines a complete design and implementation plan with two explicit constraints:

- Keep branch-level deliverables reviewable through sequential PRs.
- Ensure the architecture can absorb remaining MariaDB tasks without repeatedly redesigning integration points.

The current branch already establishes the compatibility framework and end-to-end baseline on real MariaDB, and it now includes an architecture cleanup that centralizes compatibility activation and rewriter construction. This cleanup is the key enabler for next tasks.

2. Background and Problem Statement

The task board entries under MySQL/MariDB compability are not a single bug class. They represent a queue of different technical domains:

- SQL dialect and DDL or schema differences between MariaDB and TiDB, for example issue 12429 and related data type or temporal syntax items.
- Runtime replication behavior differences, including GTID and relay or incremental path stability issues, for example issues 12423, 11784, and 10658.
- User-facing and operator-facing behavior, such as version checks and instructions, for example issues 12209 and 11734.
- Coverage gaps in integration and CI, for example issues 12412 and 10734.

If these domains are implemented as one mixed patch, the result will be difficult to review, difficult to test, and hard to extend safely. The design therefore uses a layered approach with explicit boundaries.

3. Goals, Non-Goals, and Success Criteria

3.1 Goals

- Treat flavor: mariadb as a first-class upstream path in DM.
- Make compatibility behavior automatic by default for MariaDB flavor while keeping explicit on or off controls.
- Keep parser-first compatibility strategy and use rewrite fallback only when needed.
- Ensure loader, syncer, and checker consume the same compatibility contract.
- Add deterministic integration and CI coverage using a real MariaDB service.

3.2 Non-Goals for this branch

- Redesigning GTID model semantics for MariaDB.
- Redesigning relay catch-up behavior and upstream switch protocols.
- Solving every open MariaDB runtime incident in one branch.

3.3 Success Criteria

- MariaDB flavor can run full-load and incremental DDL paths without manual compatibility toggles.
- Checker and runtime path see consistent transformed SQL behavior.
- CI can run MariaDB integration cases continuously.
- Remaining MariaDB tasks can be added through existing extension points without rewriting activation plumbing.

4. Design Principles

- Layered ownership: parser handles parseability, rewriter handles semantic downgrade, runtime layer handles protocol behavior.
- Single activation path: all DM units should ask config for compatibility rewriter through one method.
- Parser-first policy: if parser can handle syntax and behavior is safe, keep SQL as-is.
- Explicit fallback policy: rewrite only when TiDB semantic gap requires it.
- Test-first for each new rule: every compatibility rule must have unit coverage and at least one integration assertion where practical.

5. High-Level Architecture

5.1 Layer A: Parser and Rewrite Core

- Core package is dm/pkg/mariadbcompat.
- Pipeline is parser loader -> transformer engine -> rule registry -> SQL writer.
- Rule registry supports deterministic ordering, named rules, and per-rule enable or disable controls.
- Preparse stage is retained as fallback and has been hardened to avoid rewriting inside quoted strings and non-version comments.

5.2 Layer B: Configuration and Activation

- Task and subtask config include mode, enabled-rules, disabled-rules, and strict-mode.
- Mode behavior:
  - auto: enabled only for MariaDB flavor.
  - on: force enabled for any flavor.
  - off: force disabled.

Architecture change completed in this branch:

- Added MariaDBCompatConfig.NewRewriterForFlavor(flavor) in dm/config/task.go.
- Added SubTaskConfig.MariaDBCompatRewriter() in dm/config/subtask.go.
- Updated loader, syncer, and checker to use this shared config entry point.

This removes duplicated gate logic and prevents divergence across runtime paths.

5.3 Layer C: Runtime Integration

- Loader transforms schema files before load when compatibility is enabled.
- Syncer rewrites DDL before downstream execution.
- Checker rewrites upstream CREATE TABLE text before table-structure and primary-key checks.

5.4 Layer D: Integration and CI

- New integration case mariadb_source validates baseline MariaDB upstream flow without MariaDB-specific syntax.
- New integration case mariadb_compat validates compatibility rewrite path for full-load and incremental DDL.
- CI pipeline adds real MariaDB container and environment wiring.

6. Branch Deliverables Implemented

6.1 Compatibility Framework

- Introduced and integrated dm/pkg/mariadbcompat in DM execution path.
- Added multi-rule compatibility handling for MariaDB-specific DDL and schema differences.
- Added strict and non-strict behavior controls.

6.2 Integration and CI Foundation

- Added dm/tests/mariadb_source and dm/tests/mariadb_compat.
- Added DM test group wiring and environment defaults for MariaDB host, port, and password.
- Prepared CI draft PR for MariaDB service in DM integration workflow: https://github.com/PingCAP-QE/ci/pull/4496

6.3 Architecture Cleanup for Extensible Follow-up

- Centralized compatibility rewriter construction in config layer.
- Unified loader, syncer, and checker integration to consume that single interface.
- Added tests for new config-level constructor paths.

7. Coverage Analysis Against Task Board

This section evaluates what the current branch solves, what it only prepares for, and what remains out of branch scope.

7.1 Covered in current branch

- Embedded schema and DDL transformation engine in DM, corresponding to mariadb2tidb task intent.
- Major DDL option compatibility path, corresponding to issue 12429.
- Real MariaDB integration foundation and CI wiring, corresponding to MariaDB side of issue 12412.
- Privilege-check syntax issue 12207 is already closed by upstream PR 12404 on master baseline.

7.2 Partially covered in current branch

- MariaDB specific data types, issue 10212: many cases have rule support, but not full issue matrix closure.
- Temporal tables, issue 10213: base syntax handling exists, full semantic behavior still requires targeted follow-up.
- current_timestamp default on varchar, issue 5133: rewrite groundwork exists, additional matrix tests still required.
- Unhelpful MariaDB instruction issue 11734: workflow is cleaner, user-facing message quality work remains.

7.3 Not covered in current branch

- GTID update failure issue 12423.
- Relay catch-up timeout issue 11784.
- Incremental mode crash issue 10658.
- Version check and warning message issue 12209.
- Upstream switch integration coverage issue 10734.

These are primarily runtime or operational behavior tracks and are intentionally separated from this branch.

8. Final Implementation Plan to Reach End-State

Workstream 1: Complete MariaDB integration coverage

- Add MariaDB variant of dm_upstream_switch test scenario.
- Wire it into group-based integration execution and CI.

Workstream 2: Runtime and protocol stability

- Deliver targeted fixes for GTID, relay catch-up, and incremental crash issues.
- Add deterministic reproduction and regression tests for each runtime fix.

Workstream 3: Operator and checker UX

- Improve version-check logic and warning text for MariaDB.
- Improve checker and operational instructions with MariaDB-specific context.

Workstream 4: Compatibility matrix closure

- Expand integration matrix for temporal features, data types, and function default edge cases.
- Remove rewrite rules that become redundant after parser improvements, while preserving behavior with regression tests.

9. PR Strategy and Reviewability

PR stacking should keep each patch reviewable and independently testable:

1. MariaDB source integration baseline and CI wiring.
2. Compatibility core package and rule framework.
3. Config surface and activation semantics.
4. Runtime integration into loader, syncer, and checker.
5. Compatibility integration coverage.
6. Architecture cleanup and consistency refinements.
7. Runtime issue tracks and UX tracks as dedicated follow-up PRs.

10. Risks and Mitigations

Risk: Rewrite rules conflict with parser behavior changes after dependency bumps.
Mitigation: parser-first policy, per-rule tests, and periodic overlap cleanup.

Risk: Divergent behavior across loader, syncer, and checker.
Mitigation: single config-level rewriter constructor, shared activation path.

Risk: CI instability due to additional MariaDB service.
Mitigation: explicit readiness checks and integration group balancing.

Risk: One branch attempts to solve all MariaDB issues and becomes unreviewable.
Mitigation: strict PR boundaries by technical domain and dedicated follow-up workstreams.

11. Validation Evidence

The branch includes passing targeted tests for new activation APIs and runtime integration wiring, plus existing checker and integration script validation used during branch iteration.

12. Reviewer Checklist

- Are layer boundaries clear and respected?
- Is parser-first policy correctly applied?
- Is activation logic centralized and consistently used?
- Are compatibility rules deterministic and test-backed?
- Does integration coverage represent real MariaDB behavior?
- Are non-goals correctly excluded from this branch?
