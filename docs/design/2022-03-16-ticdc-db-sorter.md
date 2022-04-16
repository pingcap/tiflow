# DB Sorter

- Author(s): [overvenus](https://github.com/overvenus)
- Tracking Issue: https://github.com/pingcap/tiflow/issues/3227

## Table of Contents

- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Detailed Design](#detailed-design)
  - [Encoding](#encoding)
    - [Key](#key)
    - [Value](#value)
  - [GC](#gc)
  - [Snapshot and Iterator](#snapshot-and-iterator)
  - [Unexpected conditions](#unexpected-conditions)
  - [Latency](#latency)
- [Test Design](#test-design)
  - [Functional Tests](#functional-tests)
  - [Scenario Tests](#scenario-tests)
  - [Compatibility Tests](#compatibility-tests)
- [Impacts & Risks](#impacts--risks)
- [Investigation & Alternatives](#investigation--alternatives)
- [Unresolved Questions](#unresolved-questions)

## Introduction

This document provides a complete design on implementing db sorter,
a resource-friendly sorter with predictable and controllable usage of CPU,
memory, on-disk files, open file descriptors, and goroutines.

## Motivation or Background

We have met issues <sup>[1]</sup> about resource exhaustion in TiCDC.
One of the main source of consumption is TiCDC sorter.

In the current architecture, the resources consumed by sorter is proportional to
the number of tables, in terms of goroutines and CPU.

To support replicating many tables scenario, like 100,000 tables, we need
a sorter that only consumes O(1) or O(logN) resources.

## Detailed Design

LevelDB is a fast on-disk key-value storage that provides ordered key-value
iteration. Also, it has matured resource management for CPU, memory, on-disk
files and open file descriptors. It matches TiCDC sorter requirements.

To further limit consumption, TiCDC creates a fixed set of leveldb instances
that are shared by multiple tables.

The db sorter is driven by actors that run on a fixed-size of goroutine
pool. This addresses goroutine management issues.

The db sorter is composed of five structs:

1. `DBActor` is a struct that reads (by taking iterators) and writes to leveldb
   directly. It is shared by multiple tables. It is driven by an actor.
2. `TableSorter` is a struct that implements `Sorter` interface and manages
   table-level states. It forwards `Sorter.AddEntry` to `Writer` and forwards
   `Sorter.Output` to `Reader`.
3. `Writer` is a struct that collects unordered key-value change data and
   forwards to `DBActor`. It is driven by an actor.
4. `Reader` is a struct that reads ordered key-value change data from iterators.
5. `Compactor` is a garbage collector for leveldb. It is shared by multiple
   tables. It is driven by an actor.

_Quantitative relationship between above structs_

| Table | DBActor | TableSorter | Writer | Reader | Compactor |
| ----- | ------- | ----------- | ------ | ------ | --------- |
| N     | 1       | N           | N      | N      | 1         |

| Read Write Sequence                                                                             | Table Sorter Structs                                                                      |
| ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| <img src="../media/db-sorter-sequence.svg?sanitize=true" alt="db-sorter-sequence" width="600"/> | <img src="../media/db-sorter-class.svg?sanitize=true" alt="db-sorter-class" width="600"/> |

### Encoding

#### Key

The following table shows key encoding. Events are sorted by
randomly generated ID, table ID, CRTs, start ts, OpType, and key.

```
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   unique ID   |    table ID   |      CRTs     |   start ts    | |  key (variable-length)  |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                                                                 ^OpType(Put/Delete)
```

LevelDB sorts keys in ascending order, so as table sorter reads events.

Let’s say “A has higher priority than B” means A is sorted before B.
Unique ID has the highest priority. It is assigned when the table pipeline
starts, and it serves two purposes:

1. Prevent data conflicting after table rescheduling, e.g., move out/in.
2. Table data can be deleted by the unique ID prefix after tables are
   scheduled out.

`CRTs` has higher priority than start ts, because TiCDC needs to output events
in commit order.
`start ts` has higher priority than key because TiCDC needs to group events
in the same transaction, and `start ts` is the transaction ID.
`OpType` has higher priority than key and `Delete` has higher priority
than `Put`, because REPLACE SQL might change the key by deleting the original
key and putting a new key. TiCDC must execute `Delete` first,
otherwise, data is lost.

#### Value

Value is a binary representation of events, encoded by MessagePack<sup>[2]</sup>.

```
0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| event value (variable-length) |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

### GC

Because all events are written to leveldb, TiCDC needs a GC mechanism to
free disk in time.

Leveldb sorter adopts `DeleteRange` approach, which bulk deletes key-values
that has been outputted. It minimizes the GC impact to `Writer` and `Reader`
for both read/write throughput and latency.

For table movement, we bulk delete data in the background after the table is
stopped.

### Snapshot and Iterator

Leveldb sorter limits the total number and the max alive time of snapshots and
iterators, because they pin memtable and keep obsolete sst files on disk.
Too many concurrent snapshots and iterators can easily cause performance and
stability issues.

### Unexpected conditions

Leveldb has two kinds of unexpected conditions,

1. Disk full: disk is full, TiCDC can no longer write more data.
2. I/O error: it means hardware reports an error, and data may be corrupted.

TiCDC should stop changefeeds immediately.

### Latency

Data can only be read after they were written to leveldb, it adds extra latency
to changefeed replication lag. It ranges from sub milliseconds to minutes
(write stall) depending on upstream write QPS.

As an optimization, we can implement storage that stores data in memory or
on-disk depending on data size as future work.

## Test Design

Leveldb sorter is an internal optimization. For tests, we focus on the scenario
tests and benchmark.

### Functional Tests

Regular unit tests and integration tests, no additional tests required.
(The proposal does not add or remove any functionality).

#### Unit test

Coverage should be more than 75% in newly added code.

### Scenario Tests

- Regular unit tests and integration tests.
- 1 TiCDC node and 12 TiKV nodes with 100K tables and 270K regions.
  - No OOM.
  - Changefeed lag should be less than 1 minute.

We will test the scenario of replicating 100,000 tables in one TiCDC node.

### Compatibility Tests

#### Compatibility with other features/components

Should be compatible with other features.

#### Upgrade Downgrade Compatibility

Sorter cleans up on-disk files when TiCDC starts, so there should be no upgrade
or downgrade compatibility issues.

## Impacts & Risks

N/A

## Investigation & Alternatives

N/A

## Unresolved Questions

N/A

[1]: https://github.com/pingcap/tiflow/issues/2698
[2]: https://msgpack.org
