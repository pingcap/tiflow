// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// The sequence of coordinate a shard DDL lock:
//   1. DM-master GET all history `Info` & construct/sync shard DDL locks (revision-M1).
//      - also PUT the `Operation` of synced locks as step-4.
//   2. DM-worker construct `Info` based on received shard DDL & PUT the `Info` into etcd (revision-W1).
//   3. DM-master get `Info` through WATCH (after revision-M1) & try to construct/sync the `Lock`.
//   4. DM-master PUT the `Operation` into etcd (revision-M2).
//   5. DM-worker get `Operation` through WATCH (after revision-W1).
//   6. DM-worker exec the `Operation` (execute/skip the shard DDL statements).
//   7. DM-worker flush the checkpoint.
//   8. DM-worker PUT `done=true` of `Operation` & DELETE the `Info` (in one txn, revision-W2).
//   9. DM-master get `done=true` of `Operation` through WATCH (after revision-M1).
//   10. DM-master DELETE the `Operation` after all DM-worker finished the operation (revision-M3).
//
// for step-4:
//   1. DM-master PUT `exec=true` `Operation` for the shard DDL lock owner.
//   2. DM-master wait for the owner to finish the `Operation`.
//   3. DM-master PUT `exec=false` `Operation` for other DM-workers.
//
// the order of revision:
//   * revision-M1 < revision-W1 < revision-M2 < revision-W2 < revision-M3.
//
// the operation on `Info`:
//   * PUT & DELETE by DM-worker (in revision-W1 & revision-W2).
//   * GET & WATCH by DM-master (for all revision).
//
// the operation on `Operation`:
//   * PUT & DELETE by DM-master (in revision-M2 & revision-M3).
//   * PUT (update) by DM-worker (in revision-W2).
//   * GET by DM-worker (after revision-W1).
//   * WATCH by DM-master (after revision-M1).
//
// re-entrant of DM-worker:
//   * before step-6:
//     * just re-run the sequence of the flow is fine.
//   * in step-6:
//     * operation has canceled: like `before step-6`.
//     * operation has done: need to tolerate the re-execution of DDL statements,
//       such as ignore `ErrColumnExists` for `ADD COLUMN`.
//       TODO(csuzhangxc): flush checkpoint before execute/skip shard DDL statements.
//   * in step-7:
//     * operation has canceled: like `operation has done` in step-6.
//     * operation has done: need to use `unlock-ddl-lock` resolve the lock manually.
//       TODO(csuzhangxc): if we can find out this case correctly, it may be handled automatically later.
//   * in step-8:
//     * operation has canceled: like `operation has done` in step-7.
//     * operation has done: just re-run the sequence of the flow is fine.
//   * after step-8:
//     * just re-run the sequence of the flow is fine.
//
// re-entrant of DM-master:
//   * just re-run the sequence of the flow is fine.
//   * do not overwrite the result of `Operation` PUTed by DM-worker in step-4.

package pessimism
