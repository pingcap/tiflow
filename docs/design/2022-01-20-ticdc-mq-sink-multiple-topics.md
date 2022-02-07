# TiCDC supports multi-topic dispatch

- Author(s): [hi-rustin](https://github.com/hi-rustin)
- Tracking Issue: https://github.com/pingcap/tiflow/issues/4423

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

This document provides a complete design on implementing multi-topic support in TiCDC MQ Sink.

## Motivation or Background

TiCDC MQ Sink only supports sending messages to a single topic, but in the MQ Sink usage scenario, we send data to
systems like [Flink], which requires us to support multiple topics, each topic as a data source.

## Detailed Design

This solution will introduce a new configuration to the configuration file that specifies which topic the sink will send
the table data to.

We will continue to keep the original topic configuration in the sinkURI, which serves two purposes.

1. when there is no new configuration or the configuration does not match, the data will be sent to that default topic.
2. DDLs of the schema level will be sent to this topic by default.

### Topic dispatch configuration format

This configuration will be added to the TiCDC changefeed configuration file.

```toml
[sink]
dispatchers = [
  { matcher = ['test1.*', 'test2.*'], dispatcher = "ts", topic = "Topic dispatch expression" },
  { matcher = ['test3.*', 'test4.*'], dispatcher = "rowid", topic = "Topic dispatch expression" },
  { matcher = ['test1.*', 'test5.*'], dispatcher = "ts", topic = "Topic dispatch expression" },
]
```

A new topic field has been added to dispatchers that will specify the topic dispatching rules for these tables.

### Topic dispatch expression details

The expression format looks like `flink_{schema}{table}`. This expression consists of two keywords and the `flink_`
prefix.

Two keywords(case-insensitive):

| Keyword  | Description            | Required |
| -------- | ---------------------- | -------- |
| {schema} | the name of the schema | no       |
| {table}  | the name of the table  | no       |

> When neither keyword is filled in, it is equivalent to sending the data to a fixed topic.

`flink_` is the user-definable part, where the user can fill in the expression with a custom string.

Some examples:

```toml
[sink]
dispatchers = [
  { matcher = ['test1.table1', 'test2.table1'], topic = "{schema}_{table}" },
  { matcher = ['test3.*', 'test4.*'], topic = "flink{schema}" },
  { matcher = ['test1.*', 'test5.*'], topic = "test-cdc" },
]
```

- matcher = ['test1.*', 'test2.*'], topic = "{schema}\_{table}"
  - Send the data from `test1.table1` and `test2.table1` to the `test1_table1` and `test2_table1` topics, respectively
- matcher = ['test3.*', 'test4.*'], topic = "flink\_{schema}"
  - Send the data from all tables in `test3` and `test4` to `flinktest3` and `flinktest4` topics, respectively
- matcher = ['test1.*', 'test5.*'], topic = "test-cdc"
  - Send the data of all the tables in `test1` (except `test1.table1`) and `test5` to the `test-cdc` topic
  - The `table1` in `test1` is sent to the `test1_table1` topic, because for tables matching multiple matcher rules, the
    topic expression corresponding to the top matcher prevails

### DDL dispatch rules

- schema-level DDLs that are sent to the default topic
- table-level DDLs, will be sent to the matching topic, if there is no topic match, it will be sent to the default topic

## Test Design

This functionality will be mainly covered by unit and integration tests.

### Functional Tests

#### Unit test

Coverage should be more than 75% in new added code.

#### Integration test

Can pass all existing integration tests when changefeed without topic dispatch configuration. In addition, we will
integrate [Flink] into our integration tests to verify multi-topic functionality.

### Scenario Tests

We will test the scenario of using `canal-json` format to connect data to [Flink].

### Compatibility Tests

#### Compatibility with other features/components

For TiCDC's original support of only a single topic, we're not going to break it this time. When you pass only the
default topic in the sinkURI and there is no topic expression configuration, it will work as is.

#### Upgrade compatibility

When not configured, it works as a single topic, so just add the configuration and create a new changefeed after the
upgrade.

#### Downgrade compatibility

The new configuration is not recognized by the old TiCDC, so you need to remove the changefeed before downgrading.

### Benchmark Tests

N/A

## Impacts & Risks

N/A

## Investigation & Alternatives

N/A

## Unresolved Questions

N/A

[flink]: https://flink.apache.org/
