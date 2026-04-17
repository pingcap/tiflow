# Next-Gen DM CI Status Tracker

## PR: https://github.com/pingcap/tiflow/pull/12599
## Branch: mariadb-source-smoke-dm → mine/mariadb-source-smoke-dm

## Goal: All groups pass on BOTH classic and next-gen CI

### Validation Progress

| Group | Next-Gen | Classic | Notes |
|-------|----------|---------|-------|
| G00 | **PASSED** #82 | NEED CHECK | |
| G01 | **PASSED** #83 | NEED CHECK | |
| G02 | **PASSED** #84 | NEED CHECK | |
| G03 | **PASSED** #86 | NEED CHECK | |
| G04 | **PASSED** #87 | NEED CHECK | |
| G05 | **PASSED** #101 | NEED CHECK | many_tables: import-into + MinIO for Phase 2 |
| G06 | **PASSED** #88 | NEED CHECK | |
| G07 | **PASSED** #89 | NEED CHECK | |
| G08 | **PASSED** #90 | NEED CHECK | |
| G09 | **PASSED** #94 | NEED CHECK | Flaky ERROR 1008 on #93 |
| G10 | PENDING #103 | NEED CHECK | mariadb_source removed, others adapted |
| G11 | **PASSED** #95 | NEED CHECK | |
| TLS_GROUP | **PASSED** #96 | NEED CHECK | |

### Tests Skipped on Next-Gen

| Test | Group | Reason |
|------|-------|--------|
| new_collation_off | G09 | Next-gen can't disable new collation |
| s3_dumpling_lightning | G09 | Lightning version gate (physical mode) |

### Tests Adapted for Next-Gen

| Test | Group | Change |
|------|-------|--------|
| many_tables Phase 2 | G05 | import-into mode + existing MinIO instead of Lightning physical |
| sync_collation | G11 | Explicit COLLATE utf8_general_ci in SQL |
| openapi test_tls | G09 | TLS TiDB with plain mysql probe (status port stays HTTP) |
| openapi test_delete_task_with_stopped_downstream | G09 | cleanup_tidb_server (port-4000 targeted) |
| new_relay | G10 | cleanup_tidb_server instead of pkill tidb-server |
| all_mode | G10 | cleanup_tidb_server instead of pkill tidb-server |
| import_into_mode | G10 | PID-targeted MinIO kill (preserve next-gen cluster MinIO) |

### Key Fixes Applied

1. DDL fix: Don't set tidb_ddl_enable_fast_reorg=0 / tidb_enable_dist_task=0 on next-gen
2. CONFIG privilege: Added to test user GRANT
3. run_tidb_server: Unified TiDB startup (unistore/tikv via PD_ADDR, TLS detection)
4. env_variables: Centralized next-gen vars (PD_ADDR, TIKV_WORKER_ADDR, KEYSPACE_NAME, etc.)
5. Cluster scripts: Source env_variables for standalone invocation
6. cleanup_tidb_server: Port-4000 targeted, removes temp-storage _dir.lock
7. shardddl1: DML merge threshold relaxed (>2)
8. dmctl_basic: Session block normalization for tidb_txn_mode diff
9. print_debug_status: Moved to ha_cases_lib.sh
10. TLS classic cluster: Restored original (separate ports, inline TiDB startup)
11. Makefile: check_third_party_binary_for_dm checks sync_diff_inspector exists instead of rebuilding

### Remaining Work

- [ ] Build #103: Full next-gen run with all groups including G10
- [ ] Verify classic CI passes
- [ ] Final cleanup: simplify scripts, squash commits
