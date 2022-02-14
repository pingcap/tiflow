# Proposal: Enhance task manageability

- Author(s): [ehco](https://github.com/Ehco1996)
- Last updated: 2022-01-22

## Background

The main purpose of this change is to address the root cause of problems like [#3771](https://github.com/pingcap/tiflow/issues/3771), which are caused by the fact that commands about DM task do not distinguish between dynamic configuration and static resources, making it impossible for users to intuitively manage their tasks. For this reasons, we will attempt to redesign the state machine of DM task and optimize the dmctl interaction interface to provide a better user experience.

### Current State machine of task

![dm old task lifecycle](../media/dm-old-task-lifecycle.png)

### New State machine of task

![dm new task lifecycle](../media/dm-new-task-lifecycle.png)

This is mainly achieved by adding a new command `dmctl task create`, which creates a stopped task instead of creating and starting a new task with single command `dmctl start-task`.

## Goals

- Organize and optimize the state machine of tasks for better management
- Unified style of commands in dmctl, optimized dmctl interaction experience
- Adding/modifying OpenAPI, enhance eco-tools

## Design and Examples

New syntax of dmctl is `dmctl [resource type] [command] [flags]`

where `resource type`, `command` and `flags` are:

- `resource type`: Specifies the resource you want to control. resource types are case-insensitive and there are limited resource types. currently there are only these types of resource types: `task`, `source`, `relay`, `ddl-lock` and `member`.

- `command`: Specifies the operation that you want to perform on one or more resources, for example `create`, `get`, `update`, `delete`, `etc`.

- `flags`: Specifies optional flags. For example, you can use the `--master-addr` flags to specify the address and port of the DM-Master server. Note that we **not allow** any non-keyword arguments, All arguments must be specified as this: `--flag value`.

### dmctl commands for Task

| command | Full Syntax Example                                                                                                                           | Flags                                                                          | Description                                                              |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| check   | `dmctl task check --config-file=dm.yaml --error-count=1 --warn-count=1`                                                                       | --config-file, --error-count, --warn-count                                     | check the task config yaml file.                                         |
| create  | `dmctl task create --config-file="dm.yaml"`                                                                                                   | --config-file                                                                  | create a stopped task with config file.                                  |
| update  | `dmctl task update --config-file="dm.yaml"`                                                                                                   | --config-file                                                                  | update a stopped task with config file.                                  |
| delete  | `dmctl task delete --task-name="test" --yes`                                                                                                  | --task-name, --yes                                                             | delete a not running task and remove all meta data for this task.        |
| get     | `dmctl task get --task-name="test" --output="new_task.yaml"`                                                                                  | --task-name, --output                                                          | show the task config in yaml format, also support output to file.        |
| list    | `dmctl task list --stage="running" --source="source1,source2"`                                                                                | --stage, --source                                                              | list all tasks in current cluster, support filter by status and sources. |
| status  | `dmctl task status --task-name="test" --source="source1,source2"`                                                                             | --task-name --source                                                           | show task detail status.                                                 |
| start   | `dmctl task start --task-name="test" --source="source1,source2" --remove-meta=false --start-time="2021-01-01 00:00:00" --safe-mode-time="1s"` | --task-name, --status, --source, --remove-meta, --start-time, --safe-mode-time | start a stopped task with many flags.                                    |
| stop    | `dmctl task stop --task-name="test" --source="source1,source2" --timeout="60s"`                                                               | --task-name, --source, --timeout                                               | stop a running task with many flags.                                     |

### dmctl commands for Source

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

### dmctl commands for Relay

| command | Full Syntax Example                                                                                                                     | Flags                                 | Description                                                                  |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------- | ---------------------------------------------------------------------------- |
| start   | `dmctl relay start --source-name="source1.yaml" --worker-name="worker1"`                                                                | --source-name, --worker-name          | start relay for a source on a worker.                                        |
| stop    | `dmctl relay stop --source-name="source1.yaml" --worker-name="worker1"`                                                                 | --source-name, --worker-name          | stop relay for a source on a worker.                                         |
| purge   | `dmctl relay purge --source-name="source1.yaml" --file-name="mysql-bin.000006" --sub-dir="2ae76434-f79f-11e8-bde2-0242ac130008.000001"` | --source-name, --file-name, --sub-dir | purges relay log files of the DM-worker according to the specified filename. |

### dmctl commands for DDL-LOCK

| command | Full Syntax Example                                          | Flags                    | Description                                  |
| ------- | ------------------------------------------------------------ | ------------------------ | -------------------------------------------- |
| list    | `dmctl ddl-lock list --task-name="test"`                     | --task-name              | show shard-ddl locks information for a task. |
| unlock  | `dmctl ddl-lock unlock --task-name="test" --lock-id="lock1"` | --task-name, --lock-name | force unlock un-resolved DDL locks.          |

### dmctl commands for member

| command                    | Full Syntax Example                                                 | Flags          | Description                                       |
| -------------------------- | ------------------------------------------------------------------- | -------------- | ------------------------------------------------- |
| member list                | `dmctl member list --name="master-1" --role="master/worker/leader"` | --name, --role | show members of current cluster by name and role. |
| member offline             | `dmctl member offline --name="master-1"`                            | --name         | offline members of current cluster by name.       |
| member evict-leader        | `dmctl member evict-leader --name="master-1"`                       | --name         | evict leader for master node.                     |
| member cancel-evict-leader | `dmctl member cancel-evict-leader --name="master-1"`                | --name         | cancel evict leader for master node.              |

### Optimised dmctl for interaction mode (optional)

The current interaction of dmctl still has room for optimization, and I hope to take this opportunity to do some optimization of the interaction experience, focusing on the user can quickly select the command they want to enter through the keyboard, instead of entering commands by their memory.

Here is a simple prototype demo:

![dmctl](../media/new-dmctl.svg)

## Breaking Changes for OpenAPI

- `POST /api/v1/tasks` will be changed from creating and starting tasks to only creating tasks.
- `DELETE /api/v1/tasks/{task-name}` will be changed from stopping task to stopping task and delete the meta data.
- `POST /api/v1/tasks/{task-name}/pause` will be updated to `POST /api/v1/tasks/{task-name}/stop`
- `POST /api/v1/tasks/{task-name}/resume` will be updated to `POST /api/v1/tasks/{task-name}/start`
- `POST /api/v1/sources/{source-name}/pause-relay` will be deleted
- `POST /api/v1/sources/{source-name}/resume-relay` will be deleted

## Milestones

### Milestone 1 - Implementation of the modified task state machine according to the design documentation

This phase is mainly about implementing the DM-Master/DM-Worker internal logic.

For tasks, the DM-Master's internal scheduling module needs to support the creation of a subtask in a stopped state, in addition to adapting additional parameters like `-start-time`,`--time-out` and so on. the DM-Worker need to watch the task stage from etcd and operate the subtask according the stage.

And for Sources, when the DM-Master receives a `disable source` request from a user, it will **synchronously** notify the DM-Worker and tell the DM-Worker to stop processing the subtask.

Note that all changes to the internal logic at this stage do not have any effect on existing dmctl.

### Milestone 2 - Defining the new OpenAPI Spec and implementing specific features

This phase is used to identify and implement the new OpenAPI and to use unit and integration tests to determine whether the OpenAPI meets expectations in certain scenarios, It is important to note that changes to the code in this phase will result in the above incompatible changes to the existing OpenAPI.

### Milestone 3 - Implementing commands in dmctl with OpenAPI and optimize the interaction experience

This phase will focus most of the effort on implementing the commands in the dmctl and optimize the interaction experience, and completing the corresponding unit and integration tests. It is to be expected that a lot of time will be spent in this phase on modifying and testing the CI.

### Milestone 4 - Perform corresponding testing and documentation completions

This is the final stage of testing, including system and compatibility testing, as well as completing the documentation.
