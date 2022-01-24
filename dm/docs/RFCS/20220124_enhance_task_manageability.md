# Proposal: Enhance task manageability

- Author(s): [ehco](https://github.com/Ehco1996)
- Last updated: 2022-01-22

## Background

The main purpose of this change is to address the root cause of problems like [#3771](https://github.com/pingcap/tiflow/issues/3771), which are caused by the fact that DM Task itself does not distinguish between dynamic configuration and static resources, making it impossible for users to intuitively manage synchronisation of tasks. For these reasons, we will attempt to redesign the DM Task lifecycle and the corresponding DMCTL interaction interface to provide a better user experience.

### Current State machine of task

![dm old task lifecycle](../media/dm-old-task-lifecycle.png)

### New State machine of task

![dm new task lifecycle](../media/dm-new-task-lifecycle.png)

This is mainly achieved by adding a new `create task` action to create a new synchronous task in a stopped state, rather than directly creating and starting a synchronous task with `start-task` .

## Goals

- Organize and optimize the state machine of synchronization tasks for better management of synchronization tasks
- Unified style of commands in DMCTL, optimized DMCTL interaction experience
- Adding/modifying OpenAPI to enhance peripheral eco-tools

## Design and Examples

New syntax of DMCTL is `dmctl [resource type] [command] [flags]`

where command, `resource type` , `command` , and `flags` are:

- `resource type`: Specifies the resource you want to control. Resource types are case-insensitive and there are limited resource types.

- `command`: Specifies the operation that you want to perform on one or more resources, for example `create` , `get` , `update` , `delete` , `etc` .

- `flags`: Specifies optional flags. For example, you can use the `--master-addr` flags to specify the address and port of the DM-Master server. note that we not allow any non-keyword arguments all arguments must be specified as `--flag value`.

### New DMCTL commands for Task

| command | Full Syntax Example                                                                                                                            | Flags                                                                           | Description                                                              |
| ------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| check   | `dmctl task check --config-file=dm.yaml --error-count=1 --warn-count=1`                                                                        | --config-file, --error-count, --warn-count                                      | check the task config yaml file.                                         |
| create  | `dmctl task create --config-file="dm.yaml"`                                                                                                    | --config-file                                                                   | create a stopped task with config file.                                  |
| update  | `dmctl task update --config-file="dm.yaml"`                                                                                                    | --config-file                                                                   | update a stopped task with config file.                                  |
| delete  | `dmctl task delete --task-name="test" --yes`                                                                                                   | --task-name --yes                                                               | delete a not running task and remove all meta data for this task.        |
| get     | `dmctl task get --task-name="test" --output="new_task.yaml"`                                                                                   | --task-name, --output                                                           | show the task config in yaml format, also support output to file.        |
| list    | `dmctl task list --status="running" --sources="source1,source2"`                                                                               | --status, --sources                                                             | list all tasks in current cluster, support filter by status and sources. |
| status  | `dmctl task status --task-name="test" --sources="source1,source2"`                                                                             | --task-name --sources                                                           | show task detail status.                                                 |
| start   | `dmctl task start --task-name="test" --sources="source1,source2" --remove-meta=false --start-time="2021-01-01 00:00:00" --safe-mode-time="1s"` | --task-name, --status, --sources, --remove-meta, --start-time, --safe-mode-time | start a stopped task with many flags.                                    |
| stop    | `dmctl task stop --task-name="test" --sources="source1,source2" --timeout="60s"`                                                               | --task-name, --sources, --timeout                                               | stop a running task with many flags.                                     |

### New DMCTL commands for Source

| command  | Full Syntax Example                                                     | Flags                        | Description                                                         |
| -------- | ----------------------------------------------------------------------- | ---------------------------- | ------------------------------------------------------------------- |
| create   | `dmctl source create --config-file="source1.yaml"`                      | --config-file                | create source with config file.                                     |
| update   | `dmctl source update --config-file="source2.yaml"`                      | --config-file                | update a source that has no running tasks.                          |
| delete   | `dmctl source delete --source-name="source1"`                           | --source-name                | delete a source that has no running tasks.                          |
| get      | `dmctl source get --source-name="source1" --output="source1.yaml"`      | --source-name, --output      | show the source config in yaml format, also support output to file. |
| list     | `dmctl source list`                                                     |                              | list all source in current cluster.                                 |
| status   | `dmctl source status --source-name="source1"`                           | --source-name                | show source detail status.                                          |
| enable   | `dmctl source enable --source-name="source1"`                           | --source-name                | enable a disabled source.                                           |
| disable  | `dmctl source disable --source-name="source1"`                          | --source-name                | disable a source and also stop the running subtasks of this source. |
| transfer | `dmctl source transfer --source-name="source1" --worker-name="worker1"` | --source-name, --worker-name | transfers a source to a free worker.                                |

### New DMCTL commands for Relay

| command | Full Syntax Example                                                                                                                     | Flags                                 | Description                                                                  |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- | ---------------------------------------------------------------------------- |
| start   | `dmctl relay start --source-name="source1.yaml" --worker-name="worker1"`                                                                | --source-name, --worker-name          | start relay for a source on a worker.                                        |
| stop    | `dmctl relay stop --source-name="source1.yaml" --worker-name="worker1"`                                                                 | --source-name, --worker-name          | stop relay for a source on a worker.                                         |
| pause   | `dmctl relay pause --source-name="source1.yaml" --worker-name="worker1"`                                                                | --source-name, --worker-name          | pause relay for a source on a worker.                                        |
| resume  | `dmctl relay resume --source-name="source1.yaml" --worker-name="worker1"`                                                               | --source-name, --worker-name          | resume relay for a source on a worker.                                       |
| purge   | `dmctl relay purge --source-name="source1.yaml" --file-name="mysql-bin.000006" --sub-dir="2ae76434-f79f-11e8-bde2-0242ac130008.000001"` | --source-name, --file-name, --sub-dir | purges relay log files of the DM-worker according to the specified filename. |

### New DMCTL commands for DDL-LOCK

| command | Full Syntax Example                                          | Flags                    | Description                                  |
| ------- | ------------------------------------------------------------ | ------------------------ | -------------------------------------------- |
| list    | `dmctl ddl-lock list --task-name="test"`                     | --task-name              | show shard-ddl locks information for a task. |
| unlock  | `dmctl ddl-lock unlock --task-name="test" --lock-id="lock1"` | --task-name, --lock-name | force unlock un-resolved DDL locks.          |

### New DMCTL commands for member

command | Full Syntax Example | Flags | Description
member list | `dmctl member list --name="master-1" --role="master/worker/leader"` | --name, --role | show members of current cluster by name and role.
member offline | `dmctl member offline --name="master-1"` | --name | offline members of current cluster by name.
member evict-leader | `dmctl member evict-leader --name="master-1"` | --name | evict leader for master node.
member cancel-evict-leader | `dmctl member cancel-evict-leader --name="master-1"` | --name | cancel evict leader for master node.

### 优化交互模式的 DMCTL（可选的）

TBD

## Breaking Changes for OpenAPI

- `POST /api/v1/tasks` will be changed from creating and starting tasks to creating only tasks.
- `DELETE /api/v1/tasks/{task-name}` will be changed from stopping task to a stopping task and delete the meta data.
- `POST /api/v1/tasks/{task-name}/pause` will be updated to `POST /api/v1/tasks/{task-name}/stop`
- `POST /api/v1/tasks/{task-name}/resume` will be updated to `POST /api/v1/tasks/{task-name}/start`

## Milestones

### Milestone 1 - Implementation of the modified synchronous task state machine according to the design documentation

For Task, the DM-Master's internal scheduling module needs to support the creation of a SubTask in the Stopped state, and for Source, the DM-Master will **synchronously** notify the DM-Worker when it receives a Disable request from the user and tell the DM-Worker to stop processing the SubTask. Note that all changes to the internal logic at this stage will not have any impact on the existing DMCTL commands.

### Milestone 2 - Defining the new OpenAPI Spec and implementing specific features

This phase is used to identify and implement the new OpenAPI and to test the new OpenAPI under certain defined scenarios when doing the corresponding unit tests and integration tests to see if it meets current expectations. It is important to note that changes to the code in this phase will result in the above incompatible changes to the existing OpenAPI, so the logic of the OpenAPI at this point will be somewhat different to that of the DMCTL.

### Milestone 3 - Implementing commands in DMCTL with OpenAPI and optimising the interaction experience

This phase will focus most of the effort on implementing the commands in the DMCTL and optimising the interaction experience, and completing the corresponding unit and integration tests. It is to be expected that a lot of time will be spent in this phase on modifying and testing the CI.

### Milestone 4 - Perform corresponding testing and documentation completions

This is the final stage of testing, including integration and compatibility testing, as well as completing the documentation.
