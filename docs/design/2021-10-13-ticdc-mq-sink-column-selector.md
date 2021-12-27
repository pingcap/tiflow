# TiCDC Design Documents

- Author(s): [hi-rustin](https://github.com/hi-rustin)
- Tracking Issue: https://github.com/pingcap/tiflow/issues/3082

## Table of Contents

- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Detailed Design](#detailed-design)
- [Test Design](#test-design)
  - [Functional Tests](#functional-tests)
  - [Scenario Tests](#scenario-tests)
  - [Compatibility Tests](#compatibility-tests)
  - [Benchmark Tests](#benchmark-tests)
- [Impacts & Risks](#impacts--risks)
- [Investigation & Alternatives](#investigation--alternatives)
- [Unresolved Questions](#unresolved-questions)

## Introduction

This document provides a complete design about implementing column selector in TiCDC MQ Sink. This feature is currently only available for the `canal-json` protocol.

## Motivation or Background

TiCDC is a change data capture for TiDB, it supports replicating change data from upstream TiDB to various kinds of downstream, including MySQL compatible database, messaging queue system such as Kafka. When synchronizing data to Kafka, we currently support row data synchronization, but there are some user scenarios where users only need a few columns of data that have changed.

In this scenario, we need to support synchronization of specified columns. The key requirements of this feature are as follows:

- Support for synchronizing multiple columns
- Flexible column configuration support

## Detailed Design

This solution will introduce a new configuration that will specify the columns in the tables that sink needs to synchronize.

### Column selector configuration format

This configuration will be added to the TiCDC changefeed configuration file. This configuration only takes effect when the protocol is `canal-json`. Adding this configuration under other protocols will report an error.

```toml
[sink]
dispatchers = [
    {matcher = ['test1.*', 'test2.*'], dispatcher = "ts"},
    {matcher = ['test3.*', 'test4.*'], dispatcher = "rowid"},
]

protocol = "open-protocol"

column-selectors = [
    {matcher = ['test1.*', 'test2.*'], columns = ["Column selector expression"]},
    {matcher = ['test1.*', 'test2.*'], columns = ["Column selector expression"]},
]
```

Add a new selector array configuration item named `column-selectors`. Each item consists of a matcher array and a
columns array. This allows us to support multiple table and column selections.

The matcher match rules for tables are the same as the [TiDB table filter rules]. The column selector rules are
explained in detail below.

### Column selector expression details

The syntactic parsing of column selector rules is similar to that of matcher, but for columns.

- Use column names directly
  - Only columns whose names match the rules exactly will be accepted.
- Using wildcards
  - `*` — matches zero or more characters
  - `?` — matches one character
  - `[a-z]` — matches one character between "a" and "z" inclusively
  - `[!a-z]` — matches one character except "a" to "z"
  - `Character` here means a Unicode code point
- Exclusion
  - An `!` at the beginning of the rule means the pattern after it is used to exclude columns from being processed

Column is not case-sensitive on any platform, nor are column aliases. The matching order is the same as the filter rule, the first match from the back to the front.

Some examples:

- matcher = ['test1.student_*'], columns = ["id", "name"]
  - For the schema named test1, all tables prefixed with student\_, only the id, name columns are synchronized
- matcher = ['test1.t1'], columns = ["*", "!name"]
  - For the test1.t1 table, synchronize the columns except for the name column
- matcher = ['test1.t2'], columns = ["src*", "!src1"]
  - For the test1.t2 table, synchronize all columns prefixed with src, except for column src1
- matcher = ['test1.t3'], columns = ["sdb?c"]
  - For the test1.t3 table, synchronize all columns of the form sdb?c, "?" can only represent one character, such as sdb1c, sdboc, sbd-c

## Test Design

This functionality will be mainly covered by unit and integration tests, and we also need to design a testing framework for it.

### Functional Tests

#### Unit test

Coverage should be more than 75% in new added code.

#### Integration test

Can pass all existing integration tests when changefeed without column selector configuration.
Build a new mock integration test framework to validate column selector.

### Scenario Tests

N/A

### Compatibility Tests

#### Compatibility with other features/components

Because there is a possibility of filtering out all columns, the open protocol delete event may have empty data. If this happens we will delete the message.

#### Upgrade compatibility

The columns will not be filtered without this configuration, so just add the configuration and create a new changefeed after the upgrade.

#### Downgrade compatibility

The new configuration is not recognized by the old TiCDC, so you need to downgrade after modifying the configuration and remove the changefeed.

### Benchmark Tests

N/A

## Impacts & Risks

N/A

## Investigation & Alternatives

N/A

## Unresolved Questions

How to build a mock integration test framework to validate filtering rules, and that integration test framework should also be able to be used to validate other encodings and message formats.

[tidb table filter rules]: https://docs.pingcap.com/tidb/stable/table-filter#syntax
