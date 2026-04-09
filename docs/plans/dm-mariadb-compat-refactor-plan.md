# DM MariaDB Compatibility End-State Plan

Last updated: 2026-04-09

## Goal
Treat MariaDB as a first-class DM upstream. `flavor: mariadb` is the required user signal, and compatibility rewriting is enabled automatically.

## Boundaries
- Parser layer: accept MariaDB/MySQL dialect syntax and preserve AST.
- Rewriter layer: handle TiDB-target semantic downgrades (for example `CREATE OR REPLACE` multi-statement rewrite).
- Replication/checker layer: handle runtime/protocol/capability differences, not SQL grammar emulation.
- Integration/CI: run real MariaDB upstream coverage (not MySQL port substitution).

## Compatibility Policy
- For syntax-only table options already accepted upstream, keep SQL as-is (`parsed but ignored` is acceptable).
- For TiDB semantic gaps, keep explicit rewrite/downgrade in DM.
- Prefer parser-native support first; text preparse cleanup is fallback only.

## Scope In This Branch
1. MariaDB upstream integration smoke test (`mariadb_source`) and CI wiring.
2. MariaDB compatibility core rewriter package (`dm/pkg/mariadbcompat`).
3. Loader/syncer/checker integration enabled by MariaDB flavor.
4. MariaDB compatibility integration case (`mariadb_compat`) for full-load + incremental DDL.
5. Rule-set cleanup after parser bump on master:
   - remove default ENGINE rewrite overlap (`EngineOptions` no longer in default registry).

## Explicitly Out Of Scope
- GTID model redesign for MariaDB differences.
- Annotated row events / row metadata / `binlog_legacy_event_pos`.
- `CREATE TABLE ... START TRANSACTION` binlog semantic redesign.
- Full MariaDB failover/upstream-switch protocol parity.

## PR Stack Target
1. `test(dm): add MariaDB source smoke integration case`
2. `pkg(dm): add MariaDB compatibility core package`
3. `config(dm): add MariaDB compatibility task config`
4. `dm: integrate MariaDB compatibility rewriter`
5. `test(dm): add MariaDB compatibility integration coverage`
6. follow-up cleanup commit(s) on top of the stack for flavor-driven defaults and rule overlap removal.
