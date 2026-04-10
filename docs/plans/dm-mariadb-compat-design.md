DM MariaDB Compatibility Design

Last updated: 2026-04-10  
Related branch: `mariadbcompat-master-rebuild`

## 1. Abstract

This design defines how DM supports MariaDB as a documented upstream in a compatibility-first way.

The target is not a one-time syntax patch. The target is a stable compatibility architecture where:

- mainstream MariaDB schema/DDL can replicate to TiDB without manual workarounds,
- `loader`, `syncer`, and `checker` follow one consistent behavior contract,
- future compatibility items can be added by extending existing layers, not by copy-pasting logic across components.

## 2. Problem Statement and Scope

### 2.1 Problem Statement

MariaDB compatibility issues in DM are currently distributed across different execution paths:

- full-load schema apply,
- incremental DDL apply,
- checker pre-validation.

Without one shared compatibility contract, these paths can drift:

- a statement may pass one path and fail another,
- parser and rewriter responsibilities can become mixed,
- each new compatibility gap tends to add local patches.

The design requirement is to turn this into a coherent compatibility system.

### 2.2 In Scope

- compatibility architecture for MariaDB schema and DDL in DM,
- shared activation/constructor contract for all runtime components,
- integration tests with MariaDB as the real upstream,
- CI wiring that continuously runs MariaDB integration coverage.

### 2.3 Out of Scope for This Branch

- MariaDB GTID/protocol redesign,
- relay catch-up / upstream-switch protocol redesign,
- full closure of all runtime incidents that are not compatibility-core issues.

## 3. Compatibility Architecture

### 3.1 Design Principles

The architecture follows four principles:

- Parser-first: if TiDB parser already supports a statement with correct semantics, keep SQL unchanged.
- Rewriter-second: when parser support is missing or semantic mapping is incompatible, apply explicit rewrite rules.
- One contract everywhere: activation and rewrite behavior must be shared by `loader`, `syncer`, and `checker`.
- Deterministic behavior: same input SQL + same config must always produce same output and same failure semantics.

### 3.2 Layered Model

The compatibility system is split into four layers with explicit boundaries.

`Layer A - Parser Compatibility`

- Responsibility: absorb MariaDB syntax support when parser can handle it.
- Output: parsed AST or normalized SQL without compatibility rewrite.
- Rule: parser support is preferred over rewriting when behavior is equivalent and stable.

`Layer B - Rewriter Compatibility`

- Responsibility: perform TiDB-target transformation for MariaDB syntax/semantics gaps.
- Output: transformed SQL that can be executed or further checked by TiDB.
- Rule: transformation must be explicit, rule-based, and independently testable.

`Layer C - Runtime Compatibility`

- Responsibility: integrate compatibility into all DM execution paths.
- Output: aligned behavior between full-load, incremental DDL, and checker.
- Rule: runtime code cannot duplicate flavor/mode gating logic locally.

`Layer D - Verification Compatibility`

- Responsibility: prove compatibility using deterministic integration and CI coverage.
- Output: stable signal that compatibility works on real MariaDB upstream.
- Rule: baseline upstream flow and rewrite-path flow are both required.

### 3.3 Activation Contract

Compatibility activation is governed by `flavor` plus compatibility mode:

- `flavor: mariadb` is the primary upstream signal.
- `mariadb-compat.mode=auto` enables compatibility when flavor is MariaDB.
- `mariadb-compat.mode=on` forces enablement for diagnosis and controlled rollout.
- `mariadb-compat.mode=off` forces disablement for A/B comparison and fallback.

This contract is resolved once in configuration and consumed by all runtime units.

### 3.4 Rewriter Contract and Rule Lifecycle

The rewriter is treated as a narrow contract:

- input: one SQL statement and compatibility context,
- output: transformed SQL (or original SQL for no-op),
- behavior: deterministic rule ordering with transparent error boundaries.

Rule lifecycle requirements:

1. Add a rule only for a confirmed compatibility gap.
2. Register rules with deterministic priority.
3. Cover rule behavior with unit tests and at least one integration assertion.
4. Support disable/narrow/remove when parser support catches up.
5. Avoid dual ownership (same behavior in parser and rewriter simultaneously).

### 3.5 Runtime Execution Flow

`Full-load flow`

- DM loader scans dumped schema files.
- If compatibility is enabled, schema SQL is transformed before apply.
- Transformed SQL is persisted to execution path; no hidden per-file exceptions.

`Incremental DDL flow`

- DM syncer receives DDL events from MariaDB binlog stream.
- DDL SQL is transformed through the same rewriter contract.
- Downstream TiDB execution uses transformed SQL only.

`Checker flow`

- DM checker fetches upstream `CREATE TABLE` text.
- Compatibility transformation runs before AST-level structure and PK checks.
- Checker output matches runtime behavior and avoids false negatives caused by raw MariaDB syntax.

### 3.6 Failure Semantics

Failure policy is aligned with existing `strict-mode`:

- strict mode: rewrite failures are surfaced immediately; task path fails fast,
- non-strict mode: when safe, fallback to original SQL and continue with warning.

The same strict/non-strict behavior is required in loader, syncer, and checker integration points.

### 3.7 Parser-First and "Parsed but Ignored" Policy

For MariaDB compatibility, parser upgrades can make some syntax parseable without requiring runtime behavior changes.

Policy:

- if a syntax is parsed and TiDB behavior is acceptable for DM migration semantics, keep it parser-owned,
- if parseable but semantically incompatible, keep parser support and add targeted rewrite in compatibility layer,
- if parser cannot parse yet, add rewrite only when there is a clear and testable mapping.

This policy prevents rewriter bloat and keeps long-term maintainability.

## 4. Branch Design and Implementation

### 4.1 Core Abstraction and Constructor Unification

This branch standardizes rewriter construction behind:

- `MariaDBCompatConfig.NewRewriterForFlavor(flavor)` in `dm/config/task.go`,
- `SubTaskConfig.MariaDBCompatRewriter()` in `dm/config/subtask.go`.

Why this is required:

- activation logic previously risked being repeated in multiple call sites,
- repeated gating creates behavior drift over time,
- one constructor contract guarantees consistent flavor/mode resolution and simplifies future extension.

### 4.2 Compatibility Package Responsibilities

The compatibility package is responsible for:

- owning rule registration and execution ordering,
- exposing rewriter API to runtime units,
- containing focused tests around transformation behavior,
- avoiding leakage of component-specific state.

Runtime units (`loader`, `syncer`, `checker`) only consume the contract and do not own rule details.

### 4.3 Loader Integration

Integration point: full-load schema apply path.

Behavior:

1. Collect schema SQL files generated by dump/load pipeline.
2. Build rewriter via `SubTaskConfig.MariaDBCompatRewriter()`.
3. Rewrite SQL when compatibility is enabled.
4. Skip no-op rewrites to reduce unnecessary file mutation.
5. Apply transformed SQL to downstream TiDB.

Key guarantees:

- deterministic file transform behavior,
- no per-file ad-hoc compatibility branch,
- strict/non-strict semantics consistent with global mode.

### 4.4 Syncer Integration

Integration point: incremental DDL handling before downstream execution.

Behavior:

1. Receive and normalize incoming upstream DDL SQL.
2. Build/obtain rewriter from the same unified constructor contract.
3. Rewrite DDL text before execution.
4. Preserve existing DDL execution control flow and error classification.
5. Apply strict/non-strict failure policy consistently.

Key guarantees:

- incremental path uses identical compatibility rules as full-load path,
- no hidden "compatibility only in full-load" behavior.

### 4.5 Checker Integration

Integration point: table structure and key checks that use upstream `CREATE TABLE`.

Behavior:

1. Build per-source rewriters from unified constructor.
2. Transform upstream DDL text before parser comparison.
3. Continue with existing checker comparison logic on transformed SQL.

Key guarantees:

- checker result reflects what runtime can actually execute,
- reduced false incompatibility reports for MariaDB upstream.

### 4.6 Test and CI Design

Compatibility validation is split into two integration layers:

`Layer 1 - Baseline MariaDB upstream flow`

- case: `dm/tests/mariadb_source`,
- objective: prove DM can run end-to-end with MariaDB upstream even without MariaDB-specific syntax stress.

`Layer 2 - Compatibility-path flow`

- case: `dm/tests/mariadb_compat`,
- objective: cover statements that require compatibility transformation in full-load and incremental paths.

CI wiring:

- MariaDB service is added in DM integration pipeline draft PR:
  - `https://github.com/PingCAP-QE/ci/pull/4496`

Execution strategy:

- merge baseline coverage with CI first,
- then expand compatibility-path cases incrementally with deterministic assertions.

### 4.7 Branch Split Strategy for Reviewability

To make review focused and bisect-friendly, the branch is split in sequence:

1. MariaDB upstream integration baseline + CI wiring.
2. Compatibility core and rewriter framework.
3. Unified config activation/constructor path.
4. Loader/syncer/checker integration.
5. Compatibility-focused integration test expansion.

This ordering keeps every PR reviewable, runnable, and independently revertible.

### 4.8 Compatibility Boundaries in This Branch

Covered in this branch:

- shared compatibility constructor and activation contract,
- runtime integration in loader/syncer/checker,
- baseline MariaDB integration + CI foundation,
- initial compatibility-path tests.

Partially covered:

- full MariaDB type matrix closure,
- temporal-table variations,
- default-expression edge cases.

Explicitly not in this branch:

- GTID/protocol redesign,
- relay/upstream-switch protocol redesign,
- broader runtime tracks not rooted in compatibility architecture.

## 5. End-State Plan (Compatibility-Driven)

### 5.1 Close the Remaining Compatibility Matrix

- extend integration coverage for data types, temporal features, and default expression variants,
- add representative rewrite/parse ownership tests for each category.

### 5.2 Keep Parser and Rewriter Ownership Clean

- evaluate each existing rewrite rule after parser upgrades,
- retire or narrow rules where parser behavior is sufficient,
- keep only semantic-gap rewrites in compatibility layer.

### 5.3 Improve Operator Experience Around Compatibility

- ensure MariaDB version checks and guidance match real behavior,
- avoid exposing internal implementation details (rewriter/parser split) as operational burden.

## 6. Risks and Mitigations

Risk: parser upgrades overlap with rewrite rules and cause double-transform behavior.  
Mitigation: parser-first ownership policy + overlap regression tests.

Risk: compatibility behavior drifts across runtime components.  
Mitigation: enforce unified constructor path and ban local compatibility gate logic.

Risk: CI instability after introducing MariaDB service.  
Mitigation: readiness checks, isolated case grouping, deterministic teardown.

Risk: branch scope grows into unrelated runtime refactor.  
Mitigation: keep PR split aligned to compatibility layers and explicit non-scope.

## 7. Acceptance Criteria

- For `flavor: mariadb`, mainstream full-load and incremental DDL replication paths run without extra manual compatibility toggles.
- `loader`, `syncer`, and `checker` consume one compatibility contract and show consistent behavior.
- CI continuously validates MariaDB upstream baseline and compatibility-path cases.
- New compatibility items can be implemented by extending existing layers without redesigning activation plumbing.
