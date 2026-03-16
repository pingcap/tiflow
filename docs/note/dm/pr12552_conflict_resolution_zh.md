# PR #12552 冲突处理记录

日期：2026-03-16

## 背景

- 目标 PR：`pingcap/tiflow#12552`
- 目标分支：`release-8.5`
- 相关先合入 PR：`pingcap/tiflow#12541`
- 原始主线 PR：`pingcap/tiflow#12414`

`#12552` 是 `#12414` 到 `release-8.5` 的 CPPR。`#12541` 先进入了 `release-8.5`，它已经改动了同一批 DM safe-mode / foreign key 相关文件，因此 `#12552` 在 GitHub 上变成了 `CONFLICTING`。

本次处理目标：

1. 把 `release-8.5` 合到 `#12552` 分支，消除冲突。
2. 保证冲突解决后的结果仍然符合 `#12414` 的原始语义，不引入额外行为变化。
3. 记录处理步骤、分析结论和验证结果。

## 处理步骤

1. 在本地 `tiflow` 仓库拉取以下引用：
   - `pingcap/release-8.5`
   - `pull/12552/head`
   - `pull/12541/head`
   - `pull/12414/head`
2. 在独立 worktree 中从 `pr-12552` 检出工作分支，执行：

```bash
git merge --no-commit --no-ff pingcap/release-8.5
```

3. 复现到 8 个冲突文件：
   - `dm/syncer/dml_worker.go`
   - `dm/syncer/dml_worker_test.go`
   - `dm/tests/run_group.sh`
   - `dm/tests/safe_mode_foreign_key/conf/dm-task.yaml`
   - `dm/tests/safe_mode_foreign_key/conf/source1.yaml`
   - `dm/tests/safe_mode_foreign_key/data/db1.increment.sql`
   - `dm/tests/safe_mode_foreign_key/data/db1.prepare.sql`
   - `dm/tests/safe_mode_foreign_key/run.sh`

## 冲突分析与取舍

### 1. `dm/syncer/dml_worker.go`

`release-8.5` 已经通过 `#12541` 带入了：

- `foreignKeyChecksEnabled`
- `isForeignKeyChecksEnabled`
- `shouldDisableForeignKeyChecksForJob`
- safe-mode 下按 batch 切分 `foreign_key_checks`

`#12414` 在这个基础上新增的是：

- `validateSafeModeForeignKeyUpdate`
- 在 `executeBatchJobs` 里对 safe-mode + `foreign_key_checks=1` + PK/UK update 做 guard

最终保留策略：

- 保留 `release-8.5` 已有实现。
- 仅补上 `#12414` 新增的 guard 逻辑。

这样既不会丢掉 `#12541` 已进入 release 分支的改动，也不会改变 `#12414` 在 safe-mode FK guard 上的原始语义。

### 2. `dm/syncer/dml_worker_test.go`

`release-8.5` 已经有以下测试：

- `TestShouldDisableForeignKeyChecksForJob`
- `TestIsForeignKeyChecksEnabled`
- `TestShouldDisableForeignKeyChecks`
- `TestExecuteBatchJobsWithForeignKey`

`#12414` 额外需要的是对 FK guard 的回归测试。

最终保留策略：

- 不重复引入 `release-8.5` 已有测试。
- 只补 `TestValidateSafeModeForeignKeyUpdate`。

备注：

- 原始 `#12414` 分支里有一个 `c := c` 的 range 变量拷贝；`release-8.5` 已在 `#12541` 中去掉该冗余写法。本次沿用 `release-8.5` 的更简洁写法，不影响语义。

### 3. `dm/tests/run_group.sh`

`release-8.5` 已把 `safe_mode_foreign_key` 放进 `G06`。`#12414` 新增的只有：

- `foreign_key_multi_worker`
- `foreign_key_route_single_worker`
- `foreign_key_schema_repair`

最终保留策略：

- 在 `release-8.5` 当前 `G06` 上增补这 3 个 case。
- 不带入与本 PR 无关的主线分组变化。

### 4. `dm/tests/safe_mode_foreign_key/*`

这组文件在 `release-8.5` 和 `#12414` 中都存在，但内容不同。这里不能简单选 `release-8.5`，因为 `#12414` 在主线上对该 case 做了后续演进：

- 把验证字段从 `payload` 改为 `note`，避免 guard 校验步骤与上游 FK 约束互相干扰。
- 增加 safe-mode FK guard 的暂停校验。
- 增加 `CREATE TABLE ... FOREIGN KEY` 的拒绝路径校验。
- 调整 `checker` 配置，避免 auto-resume 和期望的 `Paused` 状态竞争。

最终保留策略：

- 这一组文件整体采用 `#12414` 的最终版本。

这部分是保证“处理后的 release-8.5 结果符合 `#12414` 原始内容”的关键。

## 额外发现与最小修正

在尝试跑定向单测时，发现 `#12414` 这批 schema tracker 代码在当前 `release-8.5` 依赖树下存在一个类型不匹配问题：

- `dm/pkg/schema/tracker.go` 使用了 `ast.CIStr`
- 当前 `go.mod` 中的 TiDB 依赖版本实际使用的是 `pmodel.CIStr`

因此新增了两个纯类型对齐修正：

1. `dm/pkg/schema/tracker.go`
   - `sameColumns(a []ast.CIStr, b []ast.CIStr)` 改为 `sameColumns(a []pmodel.CIStr, b []pmodel.CIStr)`
   - `getColumnsByNames(..., names []ast.CIStr)` 改为 `names []pmodel.CIStr`
2. `dm/pkg/schema/tracker_test.go`
   - `ast.NewCIStr("parent_id_shadow")` 改为 `pmodel.NewCIStr("parent_id_shadow")`

这两个改动不改变逻辑，只是把 `#12414` 里的 FK schema-tracker 代码对齐到当前 release 分支的依赖类型定义，以便本地和 CI 能正常编译验证。

## 结果对比结论

本地对比时重点看了两类差异：

1. 相对 `pr-12552` 的差异
   - `safe_mode_foreign_key/*` 保持为 `#12414` 最终版本。
   - `dml_worker.go` / `dml_worker_test.go` 仅保留 `#12414` 新增 guard 与回归测试，没有重复带入 `#12541` 已在 release 中存在的逻辑。
   - `run_group.sh` 只做 release 分支所需的最小化分组补充。

2. 相对 `pingcap/release-8.5` 的差异
   - 与 FK causality / safe-mode FK guard / 新增 DM case 直接相关。
   - 未引入与本次冲突处理无关的额外行为改动。

结论：冲突解决结果与 `#12414` 原始目标一致；新增的 `pmodel.CIStr` 对齐属于构建兼容性修正，不改变功能语义。

## 验证

### 已完成

1. `dm/syncer` 定向单测

```bash
make failpoint-enable && (
  go test -run 'Test(GenSQL|ShouldDisableForeignKeyChecksForJob|IsForeignKeyChecksEnabled|ShouldDisableForeignKeyChecks|ValidateSafeModeForeignKeyUpdate|ExecuteBatchJobsWithForeignKey)$' --tags=intest ./dm/syncer
  rc=$?
  make failpoint-disable
  exit $rc
)
```

结果：通过

2. `dm/pkg/schema` FK 相关定向单测

```bash
go test -run 'Test(ForeignKeyRelationBuildsRootParents|ForeignKeyRelationBuildsRootParentsWithHiddenColumns|ForeignKeyRelationSchemaAlignmentErrorGuidesRepair|ForeignKeyRelationFallsBackToDirectParentWhenUnmappable)$' --tags=intest ./dm/pkg/schema
```

结果：通过

### 未完成

3. DM 集成用例 `safe_mode_foreign_key`

尝试命令：

```bash
./tests/run.sh safe_mode_foreign_key
```

执行目录：`dm/`

结果：未能在当前环境完成，原因是本地没有启动 DM 集成测试依赖的 MySQL upstream（`127.0.0.1:3306/3307` 连接失败）。

因此，本次验证结论是：

- 单元级别和 FK schema-tracker 定向测试已覆盖并通过。
- 端到端集成 case 受环境限制未完成，需要在具备 DM 集成环境的机器或 CI 中继续确认。

## 后续动作

1. 提交 merge commit。
2. 推送到 `#12552` 对应分支。
3. 在 PR 中说明：
   - 冲突来自 `#12541` 先进入 `release-8.5`
   - 冲突已按 `#12414` 语义解决
   - 增加了 `pmodel.CIStr` 兼容性修正以恢复当前 release 分支依赖下的可编译性
