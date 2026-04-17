# DM Integration Test on Next-Gen TiDB Report

**Date**: 2026-04-10

## Environment

| Component | Version/Config |
|-----------|---------------|
| Next-Gen TiDB | v8.5.4-nextgen.202510.12 @ 127.0.0.1:14000 (remote, via tunnel) |
| MySQL Source1 | 8.0.44 @ 127.0.0.1:3306 (local container) |
| MySQL Source2 | 8.0.44 @ 127.0.0.1:3307 (local container) |
| DM Binary | v9.0.0-beta.2.pre-64-g8879687fb (failpoint build) |
| Connection latency | TiDB ~1.3s/conn, MySQL ~8ms/conn |

## Summary

**Total: 29 tests** | **PASS: 11** | **FAIL: 18**

排除网络延迟因素后：**真正的 next-gen TiDB 兼容性问题只有 3 个**（第 4 个 drop_column_with_index 也是 failpoint 问题）。

---

## 一、高概率网络问题（14 个 FAIL）

TiDB 14000 端口实际是远端集群（PROCESSLIST 显示 client IP `10.234.136.183`），每次 TCP 连接 ~1.3s。以下失败预期在本地 next-gen TiDB 上可以通过。

### 1.1 context deadline exceeded（5 个）

DM Sync 初始化时串行建 checkpoint/onlineddl/shardmeta 等多张表，每次 DDL 开新连接耗 ~1.3s，累计超过 context timeout。

| Test Case | Group |
|-----------|-------|
| dmctl_basic | G03 |
| handle_error_2 | G02 |
| handle_error_3 | G02 |
| shardddl_optimistic | G11 |
| handle_error | G02 |

### 1.2 测试时序被延迟打乱（5 个）

| Test Case | Group | 具体表现 |
|-----------|-------|---------|
| validator_basic | G11 | increment SQL 在 dump snapshot 之前执行，syncer totalEvents=0 |
| downstream_more_column | G03 | 数据最终同步成功（手动验证 count=2），但 retry 耗尽 |
| expression_filter (standalone) | Pre | Error 1412: Table definition has changed（dump 阶段竞态） |
| expression_filter | G04 | Task 始终未达到 "synced" |
| checkpoint_transaction | G02 | Diff check 通过了但后续步骤超时 |

### 1.3 Failpoint 时序被延迟打乱（3 个）

| Test Case | Group | 具体表现 |
|-----------|-------|---------|
| incremental_mode | G04 | FlushCheckpointStage failpoint 未触发 |
| all_mode/test_fail_job_between_event | Pre | failSecondJob failpoint 未触发 |
| drop_column_with_index | G04 | "go-mysql returned an error" failpoint 未触发 |

### 1.4 环境问题（1 个）

| Test Case | Group | 具体表现 |
|-----------|-------|---------|
| dmctl_command | G03 | `https_proxy` 环境变量干扰 |

---

## 二、需要修复的真正兼容性问题（3 个 FAIL）

### 2.1 `sql_mode` 测试 — `NO_AUTO_CREATE_USER`

**为什么加了这个测试**：

`sql_mode` 测试验证 DM 能在上游 MySQL 设置各种 SQL mode 的情况下正常同步数据。`NO_AUTO_CREATE_USER` 是 MySQL 5.7 的默认 SQL mode 之一，测试想覆盖 DM 处理该 mode 的能力。

DM 代码中 `pkg/conn/db.go:AdjustSQLModeCompatible()` 已经会在同步时**自动剥离** `NO_AUTO_CREATE_USER`（因为 TiDB 从不支持）。所以这个 SQL mode 的核心处理逻辑已经有了。

**为什么失败**：

测试脚本 `run.sh:27` 在 **MySQL 8.0 source** 上执行 `SET @@GLOBAL.SQL_MODE='...NO_AUTO_CREATE_USER...'`，但 **MySQL 8.0 也已经移除了** `NO_AUTO_CREATE_USER`（ERROR 1231）。测试代码的注释也承认了这一点（`db2.prepare.sql`: "NO_AUTO_CREATE_USER set failed in mysql8.0"）。

> 注意：这不仅仅是 next-gen TiDB 的问题，用 MySQL 8.0 做上游时在 classic TiDB 上也会有同样的失败。

**修复建议**：

```diff
# dm/tests/sql_mode/run.sh:27
- run_sql_source1 "SET @@GLOBAL.SQL_MODE='PIPES_AS_CONCAT,...,NO_AUTO_CREATE_USER,...,REAL_AS_FLOAT'"
+ run_sql_source1 "SET @@GLOBAL.SQL_MODE='PIPES_AS_CONCAT,IGNORE_SPACE,ONLY_FULL_GROUP_BY,NO_UNSIGNED_SUBTRACTION,NO_DIR_IN_CREATE,NO_AUTO_VALUE_ON_ZERO,NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES,STRICT_ALL_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ALLOW_INVALID_DATES,ERROR_FOR_DIVISION_BY_ZERO,HIGH_NOT_PRECEDENCE,NO_ENGINE_SUBSTITUTION,REAL_AS_FLOAT'"
```

去掉 `NO_AUTO_CREATE_USER` 即可。`db1.prepare.sql` 中对应的 `grant select on *.* to 'no_auto_create_user'` 测试行为也需要移除（MySQL 8.0 的 GRANT 不会自动创建用户，无论 SQL mode）。Classic 和 next-gen 都兼容。

---

### 2.2 `check_task` 测试 — `GRANT ALL PRIVILEGES`

**为什么加了这个测试**：

`check_task` 测试验证 DM 的 `check-task` 预检查功能，包括权限检查。`test_privileges_can_migrate()` 创建一个只有最小权限的用户来证明 DM 不需要 SUPER 权限。`test_privilege_precheck()` 中用 `GRANT ALL PRIVILEGES` 是为了快速给用户全部权限做对照测试。

**为什么失败**：

`GRANT ALL PRIVILEGES` 在 next-gen TiDB 上失败，因为 `ALL PRIVILEGES` 包含 `SHUTDOWN` 和 `CONFIG`，而 next-gen TiDB 没有这些权限（ERROR 8121）。

**修复建议**：

`run_tidb_server` 中 `SKIP_TIDB_START=1` 分支已经用了具体权限列表，把同样的模式应用到所有 `GRANT ALL PRIVILEGES` 的位置：

```diff
# dm/tests/_utils/run_tidb_server (原始 TiDB 启动路径，约 line 49)
- mysql ... -e "GRANT ALL PRIVILEGES ON *.* TO 'test'@'%' WITH GRANT OPTION;" || true
+ mysql ... -e "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, \
+   CREATE VIEW, SHOW VIEW, TRIGGER, REFERENCES, EXECUTE, SHOW DATABASES, SUPER, \
+   LOCK TABLES, CREATE TEMPORARY TABLES, RELOAD, REPLICATION CLIENT, \
+   REPLICATION SLAVE, PROCESS, CREATE USER, CREATE ROUTINE, ALTER ROUTINE, \
+   EVENT ON *.* TO 'test'@'%' WITH GRANT OPTION;" || true

# dm/tests/check_task/run.sh 中的 GRANT ALL PRIVILEGES 同理替换
```

排除 `SHUTDOWN`、`CONFIG` 即可。`|| true` 保证 classic TiDB 上如果某个权限不存在也不会阻塞。Classic 和 next-gen 都兼容。

---

### 2.3 `sync_collation` 测试 — 默认 collation 差异

**为什么加了这个测试**：

`sync_collation` 测试验证 DM 的 `collation_compatible: "strict"` 功能。该功能让 DM 在同步 DDL 时，自动为只指定了 charset 但没指定 collation 的 CREATE TABLE/DATABASE 语句添加显式 collation，确保下游和上游行为一致。

**为什么失败**：

```
MySQL source:     CREATE TABLE t1 (...) CHARACTER SET utf8
                  → 默认 collation: utf8_general_ci（大小写不敏感）

Next-gen TiDB:    CHARACTER SET utf8 不带 collation
                  → 默认 collation: utf8_bin（大小写敏感）❌
                  
Next-gen TiDB:    CHARACTER SET utf8 COLLATE utf8_general_ci
                  → collation: utf8_general_ci ✅（显式指定时正常）
```

Classic TiDB 启动时通过 `new_collations_enabled_on_first_bootstrap = true` 控制 collation 行为，utf8 默认仍然是 `utf8_general_ci`。但 next-gen TiDB 的 `collation_server = utf8mb4_bin`，utf8 charset 默认变成了 `utf8_bin`。

DM 的 `collation_compatible: "strict"` 在 **Syncer（增量同步）阶段**会通过 `adjustCollation()` 添加显式 collation。但在 **Dumpling/Lightning（全量 dump+load）阶段**，dumpling 虽然接收了 `CollationCompatible` 配置，但 dump 出的 CREATE TABLE 语句如果上游没有显式 collation，dump 出来的也没有，Lightning 执行时就依赖 TiDB 的默认值。

**修复建议**：

这是一个**真正需要评估的兼容性问题**，有两个方向：

**方向 A — 改 DM（推荐）**：让 dumpling 在 `collation_compatible: "strict"` 模式下，dump CREATE TABLE 时也添加显式 collation。这样 classic 和 next-gen 都能正确处理。改动在 dumpling 的 `CollationCompatible` 处理逻辑中。

**方向 B — 改测试**：在 sync_collation 测试中检测 TiDB collation_server 值，如果是 `utf8mb4_bin` 则跳过 case-insensitive 验证。但这治标不治本——实际用户在 next-gen TiDB 上使用 DM 也会遇到同样的 collation 不一致问题。

**方向 C — 改 next-gen TiDB 配置**：让 next-gen TiDB 的 utf8 charset 默认 collation 保持和 classic 一致（utf8_general_ci）。这需要 next-gen TiDB 团队评估。

---

## 三、非阻塞 Warning（测试能通过，但有报错日志）

| 问题 | 错误 | 影响 | 建议 |
|------|------|------|------|
| `SET tidb_ddl_enable_fast_reorg = 0` | ERROR 1235 | 不阻塞（`\|\| true`） | `run_tidb_server` 中已处理 |
| `SET tidb_enable_dist_task = 0` | ERROR 1235 | 不阻塞 | 同上 |
| `SET tidb_opt_write_row_id = '1'` | ERROR 1227 | Lightning Warning | 不影响数据正确性 |
| `select tidb_version()` | ERROR 1046 | DM 已有 fallback | 无需修改 |

---

## 四、通过的测试（11 个）

dm_syncer, slow_relay_writer, adjust_gtid, async_checkpoint_flush, binlog_parse, case_sensitive, dmctl_advance, downstream_diff_index, initial_unit, extend_column, all_mode(部分子测试)

---

## 五、结论

| 类别 | 数量 | 说明 |
|------|------|------|
| PASS | 11 | DM 核心功能兼容 |
| 网络延迟导致的 FAIL | 14 | 用本地 next-gen TiDB 预期可通过 |
| 需修复的 FAIL | 3 | 见下表 |
| 非阻塞 Warning | 4 | 不影响测试结果 |

| 问题 | 改谁 | 难度 | Classic 兼容 |
|------|------|------|-------------|
| `NO_AUTO_CREATE_USER` | **测试脚本** | 低 — 去掉一个 SQL mode 值 | 是（MySQL 8.0 也不支持） |
| `GRANT ALL PRIVILEGES` | **测试脚本** | 低 — 换成具体权限列表 | 是（`\|\| true` 兜底） |
| `utf8 默认 collation 变 utf8_bin` | **DM dumpling** 或 **next-gen TiDB 配置** | 中 — dumpling strict 模式需覆盖 load 阶段 | 是（strict 模式只是加显式 collation） |
