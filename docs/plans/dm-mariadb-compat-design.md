DM MariaDB Compatibility Design Document

Last updated: 2026-04-09
Related branch: `mariadbcompat-master-rebuild`

1. Compatibility-First Motivation

The starting point of this design is simple: MariaDB compatibility is a product capability, not a temporary patch set.

For DM, compatibility must satisfy three requirements at the same time:

- Correctness: schema and DDL statements from MariaDB should not fail on downstream TiDB for common migration paths.
- Consistency: loader, syncer, and checker should observe the same compatibility behavior.
- Extensibility: new compatibility gaps should be fixable by extending existing layers, not by adding ad-hoc logic in many places.

This document defines the architecture and branch plan using compatibility as the only top-level driver.

2. Scope and Non-Scope

2.1 In Scope

- Compatibility architecture for MariaDB schema and DDL handling in DM.
- Unified activation and integration path across `loader`, `syncer`, and `checker`.
- Branch-level implementation boundaries and follow-up implementation plan.
- Integration and CI baseline for real MariaDB upstream coverage.

2.2 Out of Scope for This Branch

- Redesign of MariaDB GTID semantics or protocol internals.
- Redesign of relay catch-up protocol and upstream-switch behavior.
- Closing every MariaDB-related runtime issue in one branch.

3. Compatibility Architecture

3.1 Layered Model

Compatibility is implemented as four layers with explicit responsibilities.

- Layer A: Parser compatibility
  - Goal: maximize direct parseability for MariaDB syntax.
  - Policy: if parser behavior is sufficient and safe, keep SQL as-is.

- Layer B: Rewriter compatibility
  - Goal: bridge TiDB semantic gaps that parser support alone does not solve.
  - Policy: explicit and rule-based downgrade, not implicit behavior spread across runtime paths.

- Layer C: Runtime compatibility
  - Goal: apply compatibility consistently in `full-load`, `incremental DDL`, and checker pre-validation flows.
  - Policy: all runtime units use the same activation and constructor contract.

- Layer D: Verification compatibility
  - Goal: validate behavior with deterministic integration tests and CI.
  - Policy: include both baseline MariaDB flow and compatibility-path coverage.

3.2 Activation Contract

Compatibility activation is controlled by `flavor` plus explicit mode:

- `flavor: mariadb` is the default user signal.
- `mariadb-compat.mode=auto` enables compatibility for MariaDB flavor.
- `mariadb-compat.mode=on` forces enable for debugging or controlled rollout.
- `mariadb-compat.mode=off` forces disable for comparison and fallback.

This contract must be resolved in one place and consumed everywhere.

3.3 Data Flow and Execution Path

3.3.1 Full-load path

- Loader collects schema files.
- If compatibility is enabled, loader rewrites schema SQL before applying to TiDB.
- Rewritten output is deterministic and based on rule registry ordering.

3.3.2 Incremental DDL path

- Syncer receives upstream DDL events.
- DDL SQL is normalized and rewritten when needed.
- Downstream execution always uses transformed SQL, while preserving stable error handling under strict or non-strict mode.

3.3.3 Checker path

- Checker obtains upstream `CREATE TABLE` SQL.
- Compatibility transformation is applied before AST-based structure checks.
- This avoids checker/runtime drift where checker reports false incompatibility that runtime could already rewrite.

3.4 Rule Lifecycle and Ordering

Compatibility rules must follow a managed lifecycle:

- Add rule only for real TiDB semantic gap or MariaDB syntax gap not solved by parser.
- Keep deterministic priority ordering in registry.
- Allow selective enable/disable for diagnosis and controlled rollout.
- Add unit tests for each rule and integration assertion for representative rules.
- Remove or narrow rules when parser capability catches up, to avoid overlap and double-transform risk.

3.5 Failure Semantics

`strict-mode` governs failure policy:

- Strict mode: rewrite errors are surfaced; task fails fast.
- Non-strict mode: rewrite failures fall back to original SQL where possible.

Design requirement:

- strict/non-strict behavior must be identical across loader, syncer, and checker integration points.

4. Branch Design and Implementation Details

4.1 Unified Constructor and Gate (Core Refactor)

This branch introduces a single compatibility constructor path:

- `MariaDBCompatConfig.NewRewriterForFlavor(flavor)` in `dm/config/task.go`
- `SubTaskConfig.MariaDBCompatRewriter()` in `dm/config/subtask.go`

Why this change is mandatory:

- Before unification, enablement decisions could be duplicated and drift over time.
- After unification, all runtime units consume one contract, so mode/flavor behavior remains consistent by design.
- Future compatibility features can extend this contract without touching multiple call-site gate implementations.

4.2 Loader Integration Design

- Integration point: schema-file transform before load.
- Behavior:
  - discover schema SQL files,
  - construct rewriter from `SubTaskConfig`,
  - rewrite file contents when enabled,
  - skip no-op transformations.
- Operational property: full-load compatibility is deterministic and does not depend on manual per-file handling.

4.3 Syncer Integration Design

- Integration point: DDL handling path before downstream execution.
- Behavior:
  - construct rewriter from unified config contract,
  - rewrite incoming DDL SQL,
  - preserve strict/non-strict semantics consistently.
- Operational property: incremental compatibility follows same rule set as full-load.

4.4 Checker Integration Design

- Integration point: table structure and primary key checks based on upstream `CREATE TABLE`.
- Behavior:
  - build per-source rewriters using unified constructor,
  - transform upstream DDL text before parser comparison logic.
- Operational property: checker output aligns with runtime transformed behavior.

4.5 Integration and CI Design

This branch establishes two test layers:

- Baseline MariaDB upstream flow:
  - `dm/tests/mariadb_source`
  - purpose: validate end-to-end MariaDB source path without relying on MariaDB-specific rewrite cases.

- Compatibility-path flow:
  - `dm/tests/mariadb_compat`
  - purpose: validate full-load + incremental DDL compatibility transformations.

CI foundation:

- MariaDB service container added in DM integration pipeline draft PR:
  - `https://github.com/PingCAP-QE/ci/pull/4496`

4.6 Extensibility by Construction

The branch structure is intentionally designed to scale to uncovered compatibility work:

- New grammar/support gaps:
  - parser-first attempt,
  - then add focused rewrite rule if parser coverage is insufficient.

- New runtime behavior gaps:
  - integrate in runtime layer without changing constructor contract.

- New checker mismatches:
  - inherit same transform contract and avoid one-off checker logic.

This prevents compatibility development from degenerating into duplicated per-component patches.

5. Current Coverage and Explicit Boundaries

5.1 Covered in This Branch

- Embedded MariaDB compatibility core package and runtime integration.
- Full-load and incremental DDL compatibility baseline coverage.
- Unified activation and rewriter construction path.
- Real MariaDB integration and CI foundation.

5.2 Partially Covered in This Branch

- MariaDB data type compatibility matrix closure.
- Temporal-table related compatibility closure.
- Default-expression edge-case closure (for example function defaults).

These areas now have compatible extension points but still need issue-focused completion.

5.3 Not Included in This Branch

- GTID/protocol-level redesign work.
- Relay catch-up and upstream-switch protocol redesign.
- Broad runtime incident closure outside compatibility core scope.

6. End-State Implementation Plan

6.1 Compatibility Matrix Completion

- Expand integration matrix for:
  - data types,
  - temporal features,
  - default-expression variants.

6.2 Runtime Stability Tracks

- Deliver dedicated runtime fixes for GTID/relay/incremental stability.
- Each fix must include deterministic reproduction and regression tests.

6.3 Operator and Checker Experience

- Improve MariaDB-specific version-check and user-facing guidance.
- Keep messaging aligned with actual compatibility behavior.

6.4 Parser-First Cleanup Loop

- Reevaluate rewrite rules after parser upgrades.
- Remove overlap rules when parser behavior is sufficient.

7. PR Split Strategy

Recommended sequence for reviewability:

- MariaDB integration baseline and CI wiring.
- Compatibility core and rule framework.
- Config surface and unified activation constructor.
- Loader/syncer/checker runtime integration.
- Compatibility integration coverage.
- Targeted follow-up tracks for runtime stability and operator UX.

8. Risks and Mitigations

Risk: parser improvements overlap with existing rewrite rules.
Mitigation: parser-first cleanup policy and overlap regression checks.

Risk: behavior divergence across runtime components.
Mitigation: use only `SubTaskConfig.MariaDBCompatRewriter()` as constructor path.

Risk: CI instability after introducing MariaDB service.
Mitigation: explicit readiness checks and balanced integration grouping.

Risk: compatibility scope grows into monolithic branch.
Mitigation: strict PR split by technical layer.

9. Acceptance Criteria

- For `flavor: mariadb`, mainstream full-load and incremental DDL paths run without extra manual compatibility toggles.
- Checker and runtime observe consistent transformed behavior.
- CI continuously validates real MariaDB compatibility paths.
- New compatibility gaps can be implemented by extending current architecture, without redesigning activation plumbing.
