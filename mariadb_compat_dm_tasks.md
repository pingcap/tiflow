# MariaDBCompat DM Tasks

Last updated: 2026-04-09

## Task List

- [x] T0: Rebuild branch from latest `origin/master`.
- [x] T1: Keep MariaDB source smoke integration case (`dm/tests/mariadb_source`) and DM test grouping.
- [x] T2: Keep MariaDB compatibility core package and loader/syncer/checker integration.
- [x] T3: Keep MariaDB compatibility integration case (`dm/tests/mariadb_compat`).
- [x] T4: Ensure compatibility enablement follows MariaDB flavor automatically in runtime paths.
- [x] T5: Remove default rewrite overlap for parser-supported ENGINE syntax (`EngineOptions` removed from default rule registry).
- [x] T6: Make `mariadb_compat` integration task rely on flavor auto-enable (remove explicit `mariadb-compat` block).
- [x] T7: Persist end-state plan in `docs/plans/dm-mariadb-compat-refactor-plan.md`.
- [ ] T8: Run full DM integration and CI validation in CI environment with real MariaDB service.
- [ ] T9: Update/refresh CI repo draft PR after DM branch is ready to merge.

## Notes

- `origin/master` already includes TiDB parser bump; this branch is designed on top of that baseline.
- OpenAPI exposure for `mariadb-compat` is intentionally not in this mainline path.
