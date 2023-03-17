// Copyright 2022 PingCAP, Inc.
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

package codec

var expectedSchemaWithoutExtension = `{
  "type": "record",
  "name": "rowtoavroschema",
  "namespace": "default.testdb",
  "fields": [
    {
      "name": "tiny",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT"
        }
      }
    },
    {
      "default": null,
      "name": "tinynullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT"
          }
        }
      ]
    },
    {
      "name": "short",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT"
        }
      }
    },
    {
      "default": null,
      "name": "shortnullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT"
          }
        }
      ]
    },
    {
      "name": "int24",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT"
        }
      }
    },
    {
      "default": null,
      "name": "int24nullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT"
          }
        }
      ]
    },
    {
      "name": "long",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT"
        }
      }
    },
    {
      "default": null,
      "name": "longnullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT"
          }
        }
      ]
    },
    {
      "name": "longlong",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidb_type": "BIGINT"
        }
      }
    },
    {
      "default": null,
      "name": "longlongnullable",
      "type": [
        "null",
        {
          "type": "long",
          "connect.parameters": {
            "tidb_type": "BIGINT"
          }
        }
      ]
    },
    {
      "name": "tinyunsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT UNSIGNED"
        }
      }
    },
    {
      "default": null,
      "name": "tinyunsignednullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "shortunsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT UNSIGNED"
        }
      }
    },
    {
      "default": null,
      "name": "shortunsignednullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "int24unsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT UNSIGNED"
        }
      }
    },
    {
      "default": null,
      "name": "int24unsignednullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "longunsigned",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidb_type": "INT UNSIGNED"
        }
      }
    },
    {
      "default": null,
      "name": "longunsignednullable",
      "type": [
        "null",
        {
          "type": "long",
          "connect.parameters": {
            "tidb_type": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "longlongunsigned",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidb_type": "BIGINT UNSIGNED"
        }
      }
    },
    {
      "default": null,
      "name": "longlongunsignednullable",
      "type": [
        "null",
        {
          "type": "long",
          "connect.parameters": {
            "tidb_type": "BIGINT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "float",
      "type": {
        "type": "float",
        "connect.parameters": {
          "tidb_type": "FLOAT"
        }
      }
    },
    {
      "default": null,
      "name": "floatnullable",
      "type": [
        "null",
        {
          "type": "float",
          "connect.parameters": {
            "tidb_type": "FLOAT"
          }
        }
      ]
    },
    {
      "name": "double",
      "type": {
        "type": "double",
        "connect.parameters": {
          "tidb_type": "DOUBLE"
        }
      }
    },
    {
      "default": null,
      "name": "doublenullable",
      "type": [
        "null",
        {
          "type": "double",
          "connect.parameters": {
            "tidb_type": "DOUBLE"
          }
        }
      ]
    },
    {
      "name": "bit",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "length": "1",
          "tidb_type": "BIT"
        }
      }
    },
    {
      "default": null,
      "name": "bitnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "length": "1",
            "tidb_type": "BIT"
          }
        }
      ]
    },
    {
      "name": "decimal",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "DECIMAL"
        },
        "logicalType": "decimal",
        "precision": 10,
        "scale": 0
      }
    },
    {
      "default": null,
      "name": "decimalnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "DECIMAL"
          },
          "logicalType": "decimal",
          "precision": 10,
          "scale": 0
        }
      ]
    },
    {
      "name": "tinytext",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "tinytextnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "mediumtext",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "mediumtextnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "text",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "textnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "longtext",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "longtextnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "varchar",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "varcharnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "varstring",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "varstringnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "string",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "stringnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "tinyblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "tinyblobnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "mediumblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "mediumblobnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "blob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "blobnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "longblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "longblobnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "varbinary",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "varbinarynullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "varbinary1",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "varbinary1nullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "binary",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "binarynullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "enum",
      "type": {
        "type": "string",
        "connect.parameters": {
          "allowed": "a\\,,b",
          "tidb_type": "ENUM"
        }
      }
    },
    {
      "default": null,
      "name": "enumnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "allowed": "a\\,,b",
            "tidb_type": "ENUM"
          }
        }
      ]
    },
    {
      "name": "set",
      "type": {
        "type": "string",
        "connect.parameters": {
          "allowed": "a\\,,b",
          "tidb_type": "SET"
        }
      }
    },
    {
      "default": null,
      "name": "setnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "allowed": "a\\,,b",
            "tidb_type": "SET"
          }
        }
      ]
    },
    {
      "name": "json",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "JSON"
        }
      }
    },
    {
      "default": null,
      "name": "jsonnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "JSON"
          }
        }
      ]
    },
    {
      "name": "date",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "DATE"
        }
      }
    },
    {
      "default": null,
      "name": "datenullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "DATE"
          }
        }
      ]
    },
    {
      "name": "datetime",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "DATETIME"
        }
      }
    },
    {
      "default": null,
      "name": "datetimenullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "DATETIME"
          }
        }
      ]
    },
    {
      "name": "timestamp",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TIMESTAMP"
        }
      }
    },
    {
      "default": null,
      "name": "timestampnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TIMESTAMP"
          }
        }
      ]
    },
    {
      "name": "time",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TIME"
        }
      }
    },
    {
      "default": null,
      "name": "timenullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TIME"
          }
        }
      ]
    },
    {
      "name": "year",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "YEAR"
        }
      }
    },
    {
      "default": null,
      "name": "yearnullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "YEAR"
          }
        }
      ]
    }
  ]
}`

var expectedSchemaWithExtension = `{
  "type": "record",
  "name": "rowtoavroschema",
  "namespace": "default.testdb",
  "fields": [
    {
      "name": "tiny",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT"
        }
      }
    },
    {
      "default": null,
      "name": "tinynullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT"
          }
        }
      ]
    },
    {
      "name": "short",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT"
        }
      }
    },
    {
      "default": null,
      "name": "shortnullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT"
          }
        }
      ]
    },
    {
      "name": "int24",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT"
        }
      }
    },
    {
      "default": null,
      "name": "int24nullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT"
          }
        }
      ]
    },
    {
      "name": "long",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT"
        }
      }
    },
    {
      "default": null,
      "name": "longnullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT"
          }
        }
      ]
    },
    {
      "name": "longlong",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidb_type": "BIGINT"
        }
      }
    },
    {
      "default": null,
      "name": "longlongnullable",
      "type": [
        "null",
        {
          "type": "long",
          "connect.parameters": {
            "tidb_type": "BIGINT"
          }
        }
      ]
    },
    {
      "name": "tinyunsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT UNSIGNED"
        }
      }
    },
    {
      "default": null,
      "name": "tinyunsignednullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "shortunsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT UNSIGNED"
        }
      }
    },
    {
      "default": null,
      "name": "shortunsignednullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "int24unsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "INT UNSIGNED"
        }
      }
    },
    {
      "default": null,
      "name": "int24unsignednullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "longunsigned",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidb_type": "INT UNSIGNED"
        }
      }
    },
    {
      "default": null,
      "name": "longunsignednullable",
      "type": [
        "null",
        {
          "type": "long",
          "connect.parameters": {
            "tidb_type": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "longlongunsigned",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidb_type": "BIGINT UNSIGNED"
        }
      }
    },
    {
      "default": null,
      "name": "longlongunsignednullable",
      "type": [
        "null",
        {
          "type": "long",
          "connect.parameters": {
            "tidb_type": "BIGINT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "float",
      "type": {
        "type": "float",
        "connect.parameters": {
          "tidb_type": "FLOAT"
        }
      }
    },
    {
      "default": null,
      "name": "floatnullable",
      "type": [
        "null",
        {
          "type": "float",
          "connect.parameters": {
            "tidb_type": "FLOAT"
          }
        }
      ]
    },
    {
      "name": "double",
      "type": {
        "type": "double",
        "connect.parameters": {
          "tidb_type": "DOUBLE"
        }
      }
    },
    {
      "default": null,
      "name": "doublenullable",
      "type": [
        "null",
        {
          "type": "double",
          "connect.parameters": {
            "tidb_type": "DOUBLE"
          }
        }
      ]
    },
    {
      "name": "bit",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "length": "1",
          "tidb_type": "BIT"
        }
      }
    },
    {
      "default": null,
      "name": "bitnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "length": "1",
            "tidb_type": "BIT"
          }
        }
      ]
    },
    {
      "name": "decimal",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "DECIMAL"
        },
        "logicalType": "decimal",
        "precision": 10,
        "scale": 0
      }
    },
    {
      "default": null,
      "name": "decimalnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "DECIMAL"
          },
          "logicalType": "decimal",
          "precision": 10,
          "scale": 0
        }
      ]
    },
    {
      "name": "tinytext",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "tinytextnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "mediumtext",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "mediumtextnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "text",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "textnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "longtext",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "longtextnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "varchar",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "varcharnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "varstring",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "varstringnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "string",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TEXT"
        }
      }
    },
    {
      "default": null,
      "name": "stringnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TEXT"
          }
        }
      ]
    },
    {
      "name": "tinyblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "tinyblobnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "mediumblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "mediumblobnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "blob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "blobnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "longblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "longblobnullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "varbinary",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "varbinarynullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "varbinary1",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "varbinary1nullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "binary",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidb_type": "BLOB"
        }
      }
    },
    {
      "default": null,
      "name": "binarynullable",
      "type": [
        "null",
        {
          "type": "bytes",
          "connect.parameters": {
            "tidb_type": "BLOB"
          }
        }
      ]
    },
    {
      "name": "enum",
      "type": {
        "type": "string",
        "connect.parameters": {
          "allowed": "a\\,,b",
          "tidb_type": "ENUM"
        }
      }
    },
    {
      "default": null,
      "name": "enumnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "allowed": "a\\,,b",
            "tidb_type": "ENUM"
          }
        }
      ]
    },
    {
      "name": "set",
      "type": {
        "type": "string",
        "connect.parameters": {
          "allowed": "a\\,,b",
          "tidb_type": "SET"
        }
      }
    },
    {
      "default": null,
      "name": "setnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "allowed": "a\\,,b",
            "tidb_type": "SET"
          }
        }
      ]
    },
    {
      "name": "json",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "JSON"
        }
      }
    },
    {
      "default": null,
      "name": "jsonnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "JSON"
          }
        }
      ]
    },
    {
      "name": "date",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "DATE"
        }
      }
    },
    {
      "default": null,
      "name": "datenullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "DATE"
          }
        }
      ]
    },
    {
      "name": "datetime",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "DATETIME"
        }
      }
    },
    {
      "default": null,
      "name": "datetimenullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "DATETIME"
          }
        }
      ]
    },
    {
      "name": "timestamp",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TIMESTAMP"
        }
      }
    },
    {
      "default": null,
      "name": "timestampnullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TIMESTAMP"
          }
        }
      ]
    },
    {
      "name": "time",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidb_type": "TIME"
        }
      }
    },
    {
      "default": null,
      "name": "timenullable",
      "type": [
        "null",
        {
          "type": "string",
          "connect.parameters": {
            "tidb_type": "TIME"
          }
        }
      ]
    },
    {
      "name": "year",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidb_type": "YEAR"
        }
      }
    },
    {
      "default": null,
      "name": "yearnullable",
      "type": [
        "null",
        {
          "type": "int",
          "connect.parameters": {
            "tidb_type": "YEAR"
          }
        }
      ]
    },
    {
      "name": "_tidb_op",
      "type": "string"
    },
    {
      "name": "_tidb_commit_ts",
      "type": "long"
    },
    {
      "name": "_tidb_commit_physical_time",
      "type": "long"
    }
  ]
}`
