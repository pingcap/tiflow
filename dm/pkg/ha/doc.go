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

package ha

// Data need to be persisted for the HA scheduler.
// - configuration:
//   - the upstream MySQL config (content of `SourceConfig`):
//     - PUT when adding an upstream (`operate-mysql create`) by DM-master.
//       - verify the validation before PUT it into etcd.
//     - GET when scheduling the source to a DM-worker instance by DM-worker.
//     - DELETE when removing an upstream (`operate-mysql stop`) by DM-master.
//       - DELETE with `the expectant stage of the relay` in one txn.
//       - DELETE with `the bound relationship between the DM-worker instance and the upstream MySQL source` in one txn.
//     - TODO: UPDATE support with `the expectant stage of the relay`.
//   - the data migration subtask config (content of `SubTaskConfig`):
//     - PUT when starting a task (`start-task`) by DM-master.
//       - verify the validation before PUT it into etcd.
//       - PUT with `the expectant stage of the subtask` in one txn.
//     - GET when starting a subtask by DM-worker.
//     - DELETE when stopping a task (`stop-task`) by DM-master.
//       - DELETE with `the expectant stage of the subtask` in one txn.
//     - TODO: UPDATE support with `the expectant stage of the subtask`.
//
// - node information (name, address, etc.):
//   - the DM-worker instance:
//     - PUT when adding a DM-worker instance by DM-master.
//     - GET only when restoring the in-memory information after the leader of DM-master changed by the new leader.
//     - DELETE when removing a DM-worker instance by DM-master.
//     - TODO: UPDATE support later.
//
// - the health status (or keep-alive) of component instances:
//   - the DM-worker instance:
//     - PUT (keep-alive) by DM-worker (when the node is healthy).
//     - GET (through WATCH) by DM-master to know if another schedule needed.
//     - DELETE (when the lease is timeout) by etcd (when the node is un-healthy).
//     - no need to UPDATE it manually.
//
// - the running stage:
//   - NOTE: persist the current stage of the relay and subtask if needed later.
//   - the bound relationship between the DM-worker instance and the upstream MySQL source (including relevant relay and subtasks):
//     - PUT when scheduling the source to a DM-worker instance by DM-master.
//       - PUT with `the expectant stage of the relay` in one txn for the first time.
//     - GET (through GET/WATCH) by DM-worker to know relevant relay/subtasks have to do.
//     - DELETE when the bounded DM-worker become offline.
//     - DELETE when removing an upstream by DM-master.
//       - DELETE with `the upstream MySQL config` in one txn.
//       - DELETE with `the expectant stage of the relay` in one txn.
//     - UPDATE when scheduling the source to another DM-worker instance by DM-master.
//   - the expectant stage of the relay:
//     - PUT when scheduling the source to a DM-worker instance by DM-master.
//       - PUT with `the bound relationship between the DM-worker instance and the upstream MySQL source` in one txn for the first time.
//     - GET (through GET/WATCH) by DM-worker to know how to update the current stage.
//     - UPDATE when handling the user request (pause-relay/resume-relay) by DM-master.
//     - DELETE when removing an upstream by DM-master.
//       - DELETE with `the upstream MySQL config` in one txn.
//       - DELETE with `the bound relationship between the DM-worker instance and the upstream MySQL source` in one txn.
//   - the expectant stage of the subtask:
//     - PUT/DELETE/UPDATE when handling the user request (start-task/stop-task/pause-task/resume-task) by DM-master.
//     - GET (through GET/WATCH) by DM-worker to know how to update the current stage.
//
// The summary of the above:
//   - only the DM-master WRITE schedule operations
//     - NOTE: the DM-worker WRITE (PUT) its information and health status.
//   - the DM-worker READ schedule operations and obey them.
// In other words, behaviors of the cluster are clear, that are decisions made by the DM-master.
// As long as the DM-worker can connect to the cluster, it must obey these decisions.
// If the DM-worker can't connect to the cluster, it must shutdown all operations.
//
// In this model, we use etcd as the command queue for communication between the DM-master and DM-worker instead of gRPC.
//
// One example of the workflow:
// 0. the user starts the DM-master cluster, and GET all history persisted data described above.
//   - restore the in-memory status.
// 1. the user starts a DM-worker instance.
//   - PUT DM-worker instance information into etcd.
// 2. DM-master GET the information of the DM-worker instance, and mark it as `free` status.
// 3. the user adds an upstream config.
//   - PUT the config of the upstream into etcd.
// 4. DM-master schedules the upstream relevant operations to the free DM-worker.
//   - PUT the bound relationship.
//   - PUT the expectant stage of the relay if not exists.
// 5. DM-worker GET the bound relationship, the config of the upstream and the expectant stage of the relay.
// 6. DM-worker obey the expectant stage of the relay.
//   - start relay (if error occurred, wait for the user to resolve it and do not re-schedule it to other DM-worker instances).
// 7. the user starts a data migration task.
// 8. DM-master PUT the data migration task config and the expectant stage of subtasks into etcd.
// 9. DM-worker GET the config of the subtask, the expectant stage of the subtask.
// 10. DM-worker obey the expectant stage of the subtask
//   - start the subtask (if error occurred, wait for the user to resolve it).
// 11. the task keeps running for a period.
// 12. the user pauses the task.
// 13. DM-master PUT the expectant stage of the subtask.
// 14. DM-worker obey the expectant stage of the subtask.
// 15. the user resumes the task (DM-master and DM-worker handle it similar to pause the task).
// 16. the user stops the task.
// 17. DM-master DELETE the data migration task config and the expectant stage of subtasks in etcd.
//   - DELETE the information before subtasks shutdown.
// 18. DM-worker stops the subtask.
//   - NOTE: DM-worker should always stop the subtask if the expectant stage of the subtask is missing.
// 19. the relay of the DM-worker continues to run.
// 20. the user remove the upstream config.
// 21. DM-master DELETE the upstream MySQL config, the bound relationship and the expectant stage of the relay.
// 22. DM-worker shutdown.
// 23. the user marks the DM-worker as offline.
//   - DELETE DM-worker instance information in etcd.
//
// when the DM-worker (with relay and subtasks) is down:
// 0. the status of the old DM-worker is un-health (keep-alive failed).
// 1. DM-master choose another DM-worker instance for failover.
//   - DM-master can only schedule the source to another new DM-worker only after the old DM-worker shutdown,
//     this may be achieved with some timeout/lease.
// 2. DM-master UPDATE the bound relationship to the new DM-worker.
// 3. the new DM-worker GET upstream config, the expectant stage of the relay and the expectant stage of the subtasks.
// 4. the new DM-worker obey the expectant stage.
//
// when the leader of the DM-master cluster changed:
// 0. the old DM-master shutdown its operation.
// 1. the new DM-master GET all history information to restore the in-memory status.
// 2. the new DM-master continue to handle user requests and scheduler for upstream sources.
//
// the operation for expectant stage (both for the relay and subtasks):
// - New:
//   - not a valid expectant stage.
//   - always mark the expectant stage as Running for the first create.
// - Running (schedule the source to the DM-worker, resume-relay or start-task, resume-task):
//   - create and start if the relay/subtask instance not exists.
//   - resume when in Paused currently.
//   - invalid for other current stages, do nothing.
// - Paused (pause-relay or pause-task):
//   - do nothing if the relay/subtask instance not exists.
//   - pause when in Running currently.
//   - invalid for other current stages, do nothing.
// - Stopped (stop-relay or stop-task):
//   - never exists for expectant stage in etcd but DELETE the relevant information.
//   - do nothing if the relay/subtask instance not exists.
//   - stop if the relay/subtask instance exists.
// - Finished:
//   - never exists for expectant stage in etcd.
