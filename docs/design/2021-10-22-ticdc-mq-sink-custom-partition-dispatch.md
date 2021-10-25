# TiCDC Design Documents

- Author(s): [3AceShowHand](https://github.com/3AceShowHand)
- Tracking Issue:

## Table of Contents

- [TiCDC Design Documents](#ticdc-design-documents)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Motivation or Background](#motivation-or-background)
  - [Detailed Design](#detailed-design)
    - [Partition dispatch configuration format](#partition-dispatch-configuration-format)
    - [Partition dispatch expression details](#partition-dispatch-expression-details)
      - [Matcher rule](#matcher-rule)
      - [How to handle DDL](#how-to-handle-ddl)
      - [How to handle partition count](#how-to-handle-partition-count)
  - [Test Design](#test-design)
    - [Functional Tests](#functional-tests)
      - [Unit test](#unit-test)
      - [Integration test](#integration-test)
    - [Scenario Tests](#scenario-tests)
    - [Compatibility Tests](#compatibility-tests)
      - [Upgrade compatibility](#upgrade-compatibility)
      - [Downgrade compatibility](#downgrade-compatibility)
    - [Benchmark Tests](#benchmark-tests)
  - [Impacts & Risks](#impacts--risks)
  - [Investigation & Alternatives](#investigation--alternatives)
  - [Reference](#reference)

## Introduction

This document provides a complete design about implementing partition dispatch in TiCDC MQ Sink.

## Motivation or Background

TiCDC is a change data capture for TiDB, it supports replicating change data from upstream TiDB to various kinds of downstream, including MySQL compatible database, messaging queue system such as Kafka. When synchronizing data to downstream message queue system, some users that data row can be sent to specific partition by rule.Based on such requirements, we plan to add the custom partition dispatch feature in TiCDC. The key requirement of this feature are:

- Support for users can specify rules which can be used to dispatch each row data to kafka topic’s partitions.
- Keep Backward compatibility with existing partition dispatch strategy.

## Detailed Design

### Partition dispatch configuration format

Partition dispatch is already a supported feature in the TiCDC, we introduce new configuration format as follow:

```toml
[sink]

// old format
dispatchers = [
    {matcher = ['test1.*', 'test2.*'], dispatcher = "ts"},
    {matcher = ['test3.*', 'test4.*'], dispatcher = "rowid"},
]

// new format
dispatchers = [
    {matcher = ['test1.*', 'test2.*'], partition = "Partition dispatch expression 1"},
    {matcher = ['test3.*', 'test4.*'], partition = "Partition dispatch expression 2"},
    {matcher = ['test1.*', 'test5.*'], partition = "Partition dispatch expression 3"},
]
```

The "dispatchers" is a supported field in the configuration file. In this feature we will introduce a new keyword "partition" to form the new format. We have to keep an eyes on the following things:
- Treat "dispatcher" as an alias for “partition” to support backward compatibility, which means TiCDC Cluster upgraded from any old version, can still work.
- First wins all: when a table matches multiple "partition dispatch expression", take the first one as the dispatch rule.
- Default: if any of the specified rule not match existing rules, use the "default" instead.

### Partition dispatch expression details

#### Matcher rule
The matcher stands for a list of table names. The rule of the matcher is identical to the TiDB table filter rules.

The main and the only usage of “partition dispatch expression” is to indicate the rule, which is used to direct the way of constructing row data keys for hashing. The detail of rule as follows:

| Partition Expression | Usage |
|----------------------|-------|
| partition="default" | since old value enabled by default, it's identical to partition="table" |
| partition="rowid" | HashKey = HandleKey’s name + value |
| partition="ts" | HashKey = commitTs |
| partition="table"  | HashKey = SchemaName + TableName |
| partition="pk" | identical to "rowid" |
| partition="[column-1, column-2, ..., column-n]" | HashKey = Concatenation of column values |
| partition = ${Index} | No hash, partition = Index |

When implement the new dispatch rules, we follow the rules:
* By pk: pk is an alias of “rowid”, treated with the same strategy.
* By Partition Index: user input number should be a valid positive integer n, 0 <= n < #partition.
  * If n is not a valid positive integer, create changefeed failed.
* By Columns: use '[' and ']' to detect the rule, and separate each column’s name by ','.
  * if rule = [a, b, c], we would use values corresponding to columns a, b, c to get the final partition index. But we cannot know whether the specified * columns exist or not until row data comes. We only use the existing column to do the calculation. eg:
    * if none of the columns exists, always send to partition 0.
    * if only part of columns exist, use those to get the partition index.

#### How to handle DDL
The dispatch rule for DDL is only related to the mq message protocol, and is not affected by the configuration.

| Message Encoding Protocol | DDL Event Dispatch |
|---------------------------|--------------------|
| Open Protocol | Broadcast to all partitions |
| Canal-JSON / Canal-PB | Only send to partition 0 |

#### How to handle partition count

User will set the partition number when creating the changefeed, but that does not always match the real topic partition number in the broker. There is the following definition:

A: the user set partition number
B: the broker’s topic partition number, real one.

* By case:
  * If A > B, fail to create changefeed. since this would make producer send data to invalid partition, then make data loss happen.
  * If A = B, create changefeed success.
  * If A < B, create changefeed success, but exceed partitions will not have data to be written.

## Test Design

### Functional Tests
#### Unit test

* TiCDC should correctly parse partition dispatch rules in configuration.

#### Integration test

* TiCDC should sink data to the specified partition according to the rule.

### Scenario Tests

### Compatibility Tests

Before this, if the user specified partition rule is not matched, use the default one. After this one, if any of the user specified partition expressions is not valid, create changefeed will be failed.

#### Upgrade compatibility

When upgrading from the TiCDC version which does not support this feature yet, the old dispatch rule should be correctly handled, which means changefeed sync data correctly.
  * The keyword “dispatcher” can be correctly parsed.

#### Downgrade compatibility

In this feature, we use the keyword partition to indicate the rule, but for the old version, it cannot be parsed. so the changefeed will not work as expected.
  * If we would like to support downgrading, we have to handle this situation, such as cherry-picking the feature to the old version.
  * At least, cherry-pick configuration parsing to the old version TiCDC.

### Benchmark Tests

N/A

## Impacts & Risks

N/A

## Investigation & Alternatives

N/A

## Reference
[tidb table filter rules]: https://docs.pingcap.com/tidb/stable/table-filter#syntax
