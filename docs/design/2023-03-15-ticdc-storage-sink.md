# TiCDC Design Documents

- Author(s): [CharlesCheung](https://github.com/CharlesCheung96), [zhaoxinyu](https://github.com/zhaoxinyu)
- Tracking Issue: https://github.com/pingcap/tiflow/issues/6797

## Table of Contents

- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Detailed Design](#detailed-design)
  - [Storage path structure](#storage-path-structure)
    - [Data change records](#data-change-records)
    - [Index files](#index-files)
    - [Metadata](#metadata)
    - [DDL events](#ddl-events)
  - [Data type in schema](#data-type-in-schema)
    - [Integer types](#integer-types)
    - [Decimal types](#decimal-types)
    - [Date and time types](#date-and-time-types)
    - [String types](#string-types)
    - [Enum and Set types](#enum-and-set-types)
  - [Protocols](#protocols)
    - [CSV](#csv)
    - [Canal json](#canal-json)
- [Test Design](#test-design)
  - [Functional Tests](#functional-tests)
  - [Scenario Tests](#scenario-tests)
  - [Compatibility Tests](#compatibility-tests)
  - [Benchmark Tests](#benchmark-tests)
- [Impacts & Risks](#impacts--risks)
- [Investigation & Alternatives](#investigation--alternatives)
- [Unresolved Questions](#unresolved-questions)

## Introduction

This document provides a complete design on implementing storage sink, which provides
the ability to output changelogs to NFS, Amazon S3, GCP and Azure Blob Storage.

## Motivation or Background

External storage services, such as Amazon S3, GCP and Azure Blob Storage, are designed
to handle large volumes of data and provide high availability and durability. By
leveraging such services, TiCDC can provide a scalable and cost-effective way to
store and manage TiDB's incremental changelogs, and enable users to build flexible
end-to-end data integration pipelines that can support a wide range of use cases
and scenarios.

## Detailed Design

### Storage path structure

This section describes the storage path structure of data change records, metadata, and DDL events.
Using the csv protocol as an example, files containing row change events should be organized as follows:

```
s3://bucket/prefix1/prefix2                                   <prefix>
├── metadata
└── schema1                                                   <schema>
    ├── meta
    │   └── schema_441349361156227000_3233644819.json         [database schema file]
    └── table1                                                <table>
        │── meta
        │   └── schema_441349361156227074_3131721815.json     [table schema file]
        └── 441349361156227074                                <table-version-separator>
            └── 13                                            <partition-separator, optional>
                ├── 2023-05-09                                <date-separator, optional>
                │   ├── CDC00000000000000000001.csv
                │   └── meta
                │       └── CDC.index
                └── 2023-05-10                                <date-separator, optional>
                    ├── CDC00000000000000000001.csv
                    └── meta
                       └── CDC.index
```

#### Data change records

Data change records are saved to the following path:

```shell
{scheme}://{prefix}/{schema}/{table}/{table-version-separator}/{partition-separator}/{date-separator}/CDC{num}.{extension}
```

- `scheme`: specifies the data transmission protocol, or the storage type, for example, <code>**s3**://xxxxx</code>.
- `prefix`: specifies the user-defined parent directory, for example, <code>s3://**bucket/prefix1/prefix2**</code>.
- `schema`: specifies the schema name, for example, <code>s3://bucket/prefix1/prefix2/**schema1**</code>.
- `table`: specifies the table name, for example, <code>s3://bucket/prefix1/prefix2/schema1/**table1**</code>.
- `table-version-separator`: specifies the separator that separates the path by the table version, for example, <code>s3://bucket/prefix1/prefix2/schema1/table1/**441349361156227074**</code>.
- `partition-separator`: specifies the separator that separates the path by the table partition, for example, <code>s3://bucket/prefix1/prefix2/schema1/table1/441349361156227074/**13**</code>.
- `date-separator`: classifies the files by the transaction commit date. Value options are:
  - `none`: no `date-separator`. For example, all files with `test.table1` version being `441349361156227074` are saved to `s3://bucket/prefix1/prefix2/schema1/table1/441349361156227074`.
  - `year`: the separator is the year of the transaction commit date, for example, <code>s3://bucket/prefix1/prefix2/schema1/table1/441349361156227074/**2022**</code>.
  - `month`: the separator is the year and month of the transaction commit date, for example, <code>s3://bucket/prefix1/prefix2/schema1/table1/441349361156227074/**2022-05**</code>.
  - `day`: the separator is the year, month, and day of the transaction commit date, for example, <code>s3://bucket/prefix1/prefix2/schema1/table1/441349361156227074/**2022-05-09**</code>.
- `num`: saves the serial number of the file that records the data change, for example, <code>s3://bucket/prefix1/prefix2/schema1/table1/441349361156227074/2022-01-02/CDC**000001**.csv</code>.
- `extension`: specifies the extension of the file. TiDB v6.5.0 supports the CSV and Canal-JSON formats.

> **Note:**
>
> The table version changes only after a DDL operation is performed, the table version is the TSO when the DDL is executed in the upstream TiDB. However, the change of the table version does not mean the change of the table schema. For example, adding a comment to a column does not cause the schema file content to change.

#### Index files

An index file is used to prevent written data from being overwritten by mistake. It is stored in the same path as the data change record.

```shell
{scheme}://{prefix}/{schema}/{table}/{table-version-separator}/{partition-separator}/{date-separator}/meta/CDC.index
```

An index file records the largest file name used in the current directory. For example:

```
CDC000005.csv
```

In this example, the files CDC000001.csv through CDC000004.csv in this directory are occupied. When a table scheduling or node restart occurs in the TiCDC cluster, the new node reads the index file and determines if CDC000005.csv is occupied. If it is not occupied, the new node writes the file starting from CDC000005.csv. If it is occupied, it starts writing from CDC000006.csv, which prevents overwriting data written by other nodes.

#### Metadata

Metadata is saved in the following path:

```shell
{protocol}://{prefix}/metadata
```

Metadata is a JSON-formatted file, for example:

```json
{
  "checkpoint-ts": 433305438660591626
}
```

- `checkpoint-ts`: Transactions with `commit-ts` smaller than `checkpoint-ts` are written to the target storage in the downstream.

#### DDL events

##### Table DDL events

When DDL events cause the table version to change, TiCDC switches to a new path to write data change records. For example, when the version of `test.table1` changes to `441349361156227074`, data will be written to the path `s3://bucket/prefix1/prefix2/schema1/table1/441349361156227074/2022-01-02/CDC000001.csv`. In addition, when DDL events occur, TiCDC generates a file to save the table schema information.

Table schema information is saved in the following path:

```shell
{scheme}://{prefix}/{schema}/{table}/meta/schema_{table-version}_{hash}.json
```

The following is a table schema file named `schema_441349361156227074_3131721815.json`:

```json
{
  "Table": "table1",
  "Schema": "schema1",
  "Version": 1,
  "TableVersion": 441349361156227074,
  "Query": "ALTER TABLE schema1.table1 ADD OfficeLocation blob(20)",
  "Type": 5,
  "TableColumns": [
    {
      "ColumnName": "Id",
      "ColumnType": "INT",
      "ColumnNullable": "false",
      "ColumnIsPk": "true"
    },
    {
      "ColumnName": "LastName",
      "ColumnType": "CHAR",
      "ColumnLength": "20"
    },
    {
      "ColumnName": "FirstName",
      "ColumnType": "VARCHAR",
      "ColumnLength": "30"
    },
    {
      "ColumnName": "HireDate",
      "ColumnType": "DATETIME"
    },
    {
      "ColumnName": "OfficeLocation",
      "ColumnType": "BLOB",
      "ColumnLength": "20"
    }
  ],
  "TableColumnsTotal": "5"
}
```

- `Table`: Table name.
- `Schema`: Schema name.
- `Version`: Protocol version of the storage sink.
- `TableVersion`: Table version.
- `Query`：DDL statement.
- `Type`: Type of DDL event.
- `TableColumns`: An array of one or more maps, each of which describes a column in the source table.
  - `ColumnName`: Column name.
  - `ColumnType`: Column type. For details, see [Data type](#data-type).
  - `ColumnLength`: Column length. For details, see [Data type](#data-type).
  - `ColumnPrecision`: Column precision. For details, see [Data type](#data-type).
  - `ColumnScale`: The number of digits following the decimal point (the scale). For details, see [Data type](#data-type).
  - `ColumnNullable`: The column can be NULL when the value of this option is `true`.
  - `ColumnIsPk`: The column is part of the primary key when the value of this option is `true`.
- `TableColumnsTotal`: The size of the `TableColumns` array.

##### Schema DDL events

Database schema information is saved in the following path:

```shell
{scheme}://{prefix}/{schema}/meta/schema_{table-version}_{hash}.json
```

The following is a database schema file named `schema_441349361156227000_3131721815.json`:

```json
{
  "Table": "",
  "Schema": "schema1",
  "Version": 1,
  "TableVersion": 441349361156227000,
  "Query": "CREATE DATABASE `schema1`",
  "Type": 1,
  "TableColumns": null,
  "TableColumnsTotal": 0
}
```

### Data type in schema

This section describes the data types used in the schema file. The data types are defined as `T(M[, D])`.

#### Integer types

Integer types in TiDB are defined as `IT[(M)] [UNSIGNED]`, where

- `IT` is the integer type, which can be `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`, or `BIT`.
- `M` is the display width of the type.

Integer types are defined as follows in schema:

```json
{
  "ColumnName": "COL1",
  "ColumnType": "{IT} [UNSIGNED]",
  "ColumnPrecision": "{M}"
}
```

#### Decimal types

Decimal types in TiDB are defined as `DT[(M,D)][UNSIGNED]`, where

- `DT` is the floating-point type, which can be `FLOAT`, `DOUBLE`, `DECIMAL`, or `NUMERIC`.
- `M` is the precision of the data type, or the total number of digits.
- `D` is the number of digits following the decimal point.

Decimal types are defined as follows in schema file:

```json
{
  "ColumnName": "COL1",
  "ColumnType": "{DT} [UNSIGNED]",
  "ColumnPrecision": "{M}",
  "ColumnScale": "{D}"
}
```

#### Date and time types

Date types in TiDB are defined as `DT`, where

- `DT` is the date type, which can be `DATE` or `YEAR`.

The date types are defined as follows in schema file:

```json
{
  "ColumnName": "COL1",
  "ColumnType": "{DT}"
}
```

The time types in TiDB are defined as `TT[(M)]`, where

- `TT` is the time type, which can be `TIME`, `DATETIME`, or `TIMESTAMP`.
- `M` is the precision of seconds in the range from 0 to 6.

The time types are defined as follows in schema file:

```json
{
  "ColumnName": "COL1",
  "ColumnType": "{TT}",
  "ColumnScale": "{M}"
}
```

#### String types

The string types in TiDB are defined as `ST[(M)]`, where

- `ST` is the string type, which can be `CHAR`, `VARCHAR`, `TEXT`, `BINARY`, `BLOB`, or `JSON`.
- `M` is the maximum length of the string.

The string types are defined as follows in schema file:

```json
{
  "ColumnName": "COL1",
  "ColumnType": "{ST}",
  "ColumnLength": "{M}"
}
```

#### Enum and Set types

The Enum and Set types are defined as follows in schema file:

```json
{
  "ColumnName": "COL1",
  "ColumnType": "{ENUM/SET}"
}
```

### Protocols

#### CSV

##### Transactional constraints

- In a single CSV file, the commit-ts of a row is equal to or smaller than that of the subsequent row.
- The same transactions of a single table are stored in the same CSV file when `transaction-atomicity` is set to table level.
- Multiple tables of the same transaction can be stored in different CSV files.

##### Definition of the data format

In the CSV file, each column is defined as follows:

- Column 1: The operation-type indicator, including `I`, `U`, and `D`. `I` means `INSERT`, `U` means `UPDATE`, and `D` means `DELETE`.
- Column 2: Table name.
- Column 3: Schema name.
- Column 4: The `commit-ts` of the source transaction. This column is optional.
- Column 5 to the last column: One or more columns that represent data to be changed.

Assume that table `hr.employee` is defined as follows:

```sql
CREATE TABLE `employee` (
  `Id` int NOT NULL,
  `LastName` varchar(20) DEFAULT NULL,
  `FirstName` varchar(30) DEFAULT NULL,
  `HireDate` date DEFAULT NULL,
  `OfficeLocation` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

The DML events of this table are stored in the CSV format as follows:

```shell
"I","employee","hr",433305438660591626,101,"Smith","Bob","2014-06-04","New York"
"U","employee","hr",433305438660591627,101,"Smith","Bob","2015-10-08","Los Angeles"
"D","employee","hr",433305438660591629,101,"Smith","Bob","2017-03-13","Dallas"
"I","employee","hr",433305438660591630,102,"Alex","Alice","2017-03-14","Shanghai"
"U","employee","hr",433305438660591630,102,"Alex","Alice","2018-06-15","Beijing"
```

##### Data type mapping

| MySQL type                                                        | CSV type | Example                        | Description                        |
| ----------------------------------------------------------------- | -------- | ------------------------------ | ---------------------------------- |
| `BOOLEAN`/`TINYINT`/`SMALLINT`/`INT`/`MEDIUMINT`/`BIGINT`         | Integer  | `123`                          | -                                  |
| `FLOAT`/`DOUBLE`                                                  | Float    | `153.123`                      | -                                  |
| `NULL`                                                            | Null     | `\N`                           | -                                  |
| `TIMESTAMP`/`DATETIME`                                            | String   | `"1973-12-30 15:30:00.123456"` | Format: `yyyy-MM-dd HH:mm:ss.%06d` |
| `DATE`                                                            | String   | `"2000-01-01"`                 | Format: `yyyy-MM-dd`               |
| `TIME`                                                            | String   | `"23:59:59"`                   | Format: `yyyy-MM-dd`               |
| `YEAR`                                                            | Integer  | `1970`                         | -                                  |
| `VARCHAR`/`JSON`/`TINYTEXT`/`MEDIUMTEXT`/`LONGTEXT`/`TEXT`/`CHAR` | String   | `"test"`                       | UTF-8 encoded                      |
| `VARBINARY`/`TINYBLOB`/`MEDIUMBLOB`/`LONGBLOB`/`BLOB`/`BINARY`    | String   | `"6Zi/5pav"`                   | base64 encoded                     |
| `BIT`                                                             | Integer  | `81`                           | -                                  |
| `DECIMAL`                                                         | String   | `"129012.1230000"`             | -                                  |
| `ENUM`                                                            | String   | `"a"`                          | -                                  |
| `SET`                                                             | String   | `"a,b"`                        | -                                  |

#### Canal json

Storage sink uses the same canal-json protocol as the mq sink. The [official documentation](https://docs.pingcap.com/tidb/dev/ticdc-canal-json/) shows how the Canal-JSON data format is implemented in TiCDC, including the TiDB extended fields, the definition of the Canal-JSON data format, and the comparison with the official Canal.

## Test Design

Storage sink is a new feature, For tests, we focus on the functional tests, scenario tests and benchmark.

### Functional Tests

- Regular unit testing and integration testing cover the correctness of data replication using csv and canal-json protocol.
- Manually test the availability and correctness of data synchronization using different external storage.

### Scenario Tests

Run stability and chaos tests under different workloads.

- The upstream and downstream data are consistent.
- Throughput and latency are stable for most scenarios.

### Compatibility Tests

#### Compatibility with other features/components

Should be compatible with other features.

#### Upgrade Downgrade Compatibility

Storage sink is a new feature, so there should be no upgrade
or downgrade compatibility issues.

### Benchmark Tests

Perform benchmark tests under common scenarios, big data scenarios, multi-table scenarios, and wide table scenarios with different parameters.

## Impacts & Risks

N/A

## Investigation & Alternatives

N/A

## Unresolved Questions

N/A
