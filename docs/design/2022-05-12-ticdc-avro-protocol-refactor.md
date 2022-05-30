# TiCDC Design Documents

- Author(s): [Zhao Yilin](http://github.com/leoppro), [Zhang Xiang](http://github.com/zhangyangyu)
- Tracking Issue: https://github.com/pingcap/tiflow/issues/5338

## Table of Contents

- [TiCDC Design Documents](#ticdc-design-documents)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Motivation or Background](#motivation-or-background)
  - [Detailed Design](#detailed-design)
    - [New Config Items](#new-config-items)
    - [flat-avro Schema Definition](#flat-avro-schema-definition)
      - [Key Schema](#key-schema)
      - [Value Schema](#value-schema)
    - [DML Events](#dml-events)
    - [Schema Change](#schema-change)
    - [Subject Name Strategy](#subject-name-strategy)
    - [ColumnValueBlock and Data Mapping](#columnvalueblock-and-data-mapping)
  - [Test Design](#test-design)
    - [Functional Tests](#functional-tests)
      - [CLI Tests](#cli-tests)
      - [Data Mapping Tests](#data-mapping-tests)
      - [DML Tests](#dml-tests)
      - [Schema Tests](#schema-tests)
      - [SubjectNameStrategy Tests](#subjectnamestrategy-tests)
    - [Compatibility Tests](#compatibility-tests)
  - [Impacts & Risks](#impacts--risks)
  - [Investigation & Alternatives](#investigation--alternatives)
  - [Unresolved Questions](#unresolved-questions)

## Introduction

This document provides a complete design on refactoring the existing Avro protocol implementation. A common Avro data format is defined in order to building data pathways to various streaming systems.

## Motivation or Background

Apache Avro™ is a data serialization system with rich data structures and a compact binary data format. Avro relies on schemas, schemas is managed by schema-registry. Avro is a common data format in streaming systems, supported by Confluent, Flink, Debezium, etc.

## Detailed Design

### New Config Items

| Config item                        | Option values          | Default | Explain                                                                                                                                                                                                                                                                                                         |
| ---------------------------------- | ---------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| protocol                           | canal-json / flat-avro | -       | Specify the format of messages written to Kafka.<br>The `flat-avro` option means that using the avro format design by this document.                                                                                                                                                                            |
| enable-tidb-extension              | true / false           | false   | Append tidb extension fields into avro message or not.                                                                                                                                                                                                                                                          |
| schema-registry                    | -                      | -       | Specifies the schema registry endpoint.                                                                                                                                                                                                                                                                         |
| avro-decimal-handling-mode         | precise / string       | precise | Specifies how the TiCDC should handle values for DECIMAL columns:<br>`precise` option represents encoding decimals as precise bytes.<br>`string` option encodes values as formatted strings, which is easy to consume but semantic information about the real type is lost.                                     |
| avro-bigint-unsigned-handling-mode | long / string          | long    | Specifies how the TiCDC should handle values for UNSIGNED BIGINT columns:<br>`long` represents values by using Avro long(64-bit signed integer) which might overflow but which is easy to use in consumers.<br>`string` represents values by string which is precise but which needs to be parsed by consumers. |

### flat-avro Schema Definition

`flat-avro` is an alias of `avro` protocol. It means all column values are placed directly inside the message with no nesting. This structure is compatible with most confluent sink connectors, but it cannot handle `old-value`. `rich-avro` is opposite and reserved for future needs.

#### Key Schema

```
{
    "name":"{{RecordName}}",
    "namespace":"{{Namespace}}",
    "type":"record",
    "fields":[
        {{ColumnValueBlock}},
        {{ColumnValueBlock}},
    ]
}
```

- `{{RecordName}}` represents full qualified table name.
- `{{ColumnValueBlock}}` represents a JSON block, which defines a column value of a key.
- Key only includes the valid index fields.

#### Value Schema

```
{
    "name":"{{RecordName}}",
    "namespace":"{{Namespace}}",
    "type":"record",
    "fields":[
        {{ColumnValueBlock}},
        {{ColumnValueBlock}},
        {
            "name":"_tidb_op",
            "type":"string"
        },
        {
            "name":"_tidb_commit_ts",
            "type":"long"
        },
        {
            "name":"_tidb_commit_physical_time",
            "type":"long"
        }
    ]
}
```

- `{{RecordName}}` represents full qualified table name.
- `{{ColumnValueBlock}}` represents a JSON block, which defines a column value of a key.
- `_tidb_op` is used to distinguish between INSERT or UPDATE events, optional values are "c" / "u".
- `_tidb_commit_ts` represents a CommitTS of a transaction.
- `_tidb_commit_physical_time` represents a physical timestamp of a transaction.

When `enable-tidb-extension` is `true`, `_tidb_op`, `_tidb_commit_ts`, `_tidb_commit_physical_time` will be appended to every Kafka value. When `enable-tidb-extension` is `false`, no extension fields will be appended to Kafka values.

### DML Events

If `enable-tidb-extension` is `true`, the `_tidb_op` field for the INSERT event is "c" and the field for UPDATE event is "u".

If `enable-tidb-extension` is `false`, the `_tidb_op` field will not be appended in Kafka value, so there is no difference between INSERT and UPDATE event.

For the DELETE event, TiCDC will send the primary key value as the Kafka key, and the Kafka value will be `null`.

### Schema Change

Avro detects schema change at every DML events instead of DDL events. Whenever there is a schema change, avro codec tries to register a new version schema under corresponding subject in the schema registry. Whether it succeeds or not depends on the schema evolution compatibility. Avro codec will not address any compatibility issues and simply propagates errors.

### Subject Name Strategy

Avro codec only supports the default `TopicNameStrategy`. This means a kafka topic could only accepts a unique schema. With the multi-topic ability in TiCDC, events from multiple tables could be all dispatched to one topic, which is not allowed under `TopicNameStrategy`. So we require in the dispatcher rules, for avro protocol, the topic rule must contain both `{schema}` and `{table}` placeholders, which means one table would occupy one kafka topic.

### ColumnValueBlock and Data Mapping

A `ColumnValueBlock` has the following schema:

```
{
    "name":"{{ColumnName}}",
    "type":{
        "connect.parameters":{
            "tidb_type":"{{TIDB_TYPE}}"
        },
        "type":"{{AVRO_TYPE}}"
    }
}
```

| SQL TYPE                                           | TIDB_TYPE                    | AVRO_TYPE | Description                                                                                                                    |
| -------------------------------------------------- | ---------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------ |
| TINYINT/BOOL/SMALLINT/MEDIUMINT/INT                | INT                          | int       | When it's unsigned, TIDB_TYPE is INT UNSIGNED. For SQL TYPE INT UNSIGNED, its AVRO_TYPE is long.                               |
| BIGINT                                             | BIGINT                       | long      | When it's unsigned, TIDB_TYPE is BIGINT UNSIGNED. If `avro-bigint-unsigned-handling-mode` is string, AVRO_TYPE is string.      |
| TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB/BINARY/VARBINARY | BLOB                         | bytes     |                                                                                                                                |
| TINYTEXT/TEXT/MEDIUMTEXT/LONGTEXT/CHAR/VARCHAR     | TEXT                         | string    |                                                                                                                                |
| FLOAT/DOUBLE                                       | FLOAT/DOUBLE                 | double    |                                                                                                                                |
| DATE/DATETIME/TIMESTAMP/TIME                       | DATE/DATETIME/TIMESTAMP/TIME | string    |                                                                                                                                |
| YEAR                                               | YEAR                         | int       |                                                                                                                                |
| BIT                                                | BIT                          | bytes     | BIT has another `connector.parameters` entry `"length":"64"`.                                                                  |
| JSON                                               | JSON                         | string    |                                                                                                                                |
| ENUM/SET                                           | ENUM/SET                     | string    | BIT has another `connector.parameters` entry `"allowed":"a,b,c"`.                                                              |
| DECIMAL                                            | DECIMAL                      | bytes     | This is an avro logical type having `scale` and `precision`. When `avro-decimal-handling-mode` is string, AVRO_TYPE is string. |

## Test Design

### Functional Tests

#### CLI Tests

- avro/flat-avro protocol
- avro/flat-avro protocol & true/false/invalid enable-tidb-extension
- avro/flat-avro protocol & precise/string/invalid avro-decimal-handling-mode
- avro/flat-avro protocol & long/string/invalid avro-bigint-unsigned-handling-mode
- avro/flat-avro protocol & valid/invalid schema-registry

#### Data Mapping Tests

- With protocol=avro&enable-tidb-extension=false&avro-decimal-handling-mode=precise&avro-bigint-unsigned-handling-mode=long, all generated schema and data are correct.
- With enable-tidb-extension=true, schema and value will have \_tidb_op, \_tidb_commit_ts, \_tidb_commit_physical_time fields.
- With avro-decimal-handling-mode=string，decimal field generates string schema and data.
- With avro-bigint-unsigned-handling-mode=string, bigint unsigned generates string schema and data.

#### DML Tests

- Insert row and check the row in downstream database.
- Update row and check the row in downstream database.
- Delete row and check the row in downstream database.

#### Schema Tests

- When the schema is not in schema registry, a fresh new schema is created.
- When the schema is in schema registry and pass compatibility check, a new version is created.
- When the schema is in schema registry and cannot pass compatibility check, reports error.

#### SubjectNameStrategy Tests

- When there is only default topic, a changefeed could only replicate one table.
- When there is invalid topic rule, report error.

### Compatibility Tests

N/A

## Impacts & Risks

N/A

## Investigation & Alternatives

N/A

## Unresolved Questions

N/A
