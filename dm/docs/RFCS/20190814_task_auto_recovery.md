# Proposal: Add task auto recovery framework

- Author(s):    [yangfei](https://github.com/amyangfei)
- Last updated: 2019-08-14

## Abstract

This proposal introduces a framework for **task recovery** automation, which enables recovering paused task from expected replication interruption.

Table of contents:

- [Background](#Background)
- [Implementation](#Implementation)
    - [Known recoverable interruption](#Known-recoverable-interruption)
    - [Task status checker](#Task-status-checker)
    - [Task recovery executor](#Task-recovery-executor)

## Background

DM can meet replication interruption in many scenarios, currently most of these interruptions are not handled automatically, we want to make task recovery more automated to reduce replication interruption time, besides we also need to refine our system to reduce the frequency of interruption. The following tasks are needed to be done

1. Classify interruption scenarios
2. Add automated recovery mechanism for some expected interruption scenarios
3. Add some measures and actions for known scenarios to reduce preventable interruption.

This proposal mainly focus on the first two parts.

## Implementation

### Known recoverable interruption

- database driver error
    - invalid connection
    - bad connection
    - retry timeout because of downstream busy
- network related interruption
    - no route to host
    - connection refused
- relay log data race
    - parse relay log file xxx from offset 4 error EOF

### Task status checker

In current DM architecture, task status is stored in the memory of DM-worker, so we can easily check task status in DM-worker itself. Task status checker is responsible for finding out paused task and distinguishing whether it is paused with a recoverable error. If task status checker finds a paused task can be resumed, it will dispatch an `auto-resume` `operation log` via DM-worker task log mechanism.

Currently when a subtask unit meets an error it will send the `pb.ProcessResult` via a channel to DM-worker main thread, because it is impossible to pass `*Error` object directly through protobuf object, we will wrap more `*Error` fields in `pb.ProcessError` to reconstruct an `*Error` object when we need.

Task status checker is running in the background of DM-worker, in the future DM may have a more complicated architecture for high availability, including multiple copies of DM-workers and more task control power in DM-master. In order to be compatible with DM-master leaded task controlling, we can have the control flows as `DM-master -> DM-worker task controller -> DM-worker`.

### Task recovery executor

Task recovery executor will be implement in the `handleTask` function of DM-worker as a standalone `TaskOp` processing case, it will reuse the subtask `Resume` interface to resume a task. We should refine the status management in DM-worker in order to meet the following guarantees:

- It is always safe and no harm to resume a paused task, although in some scenarios a task will always meet error after the resume, for example when a replication task meets a TiDB incompatible DDL.

- Resume a non-paused task will do nothing and return an error.
