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
  "name": "testdb.rowtoavroschema",
  "fields": [
    {
      "name": "tiny",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT"
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
            "tidbType": "INT"
          }
        }
      ]
    },
    {
      "name": "short",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT"
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
            "tidbType": "INT"
          }
        }
      ]
    },
    {
      "name": "int24",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT"
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
            "tidbType": "INT"
          }
        }
      ]
    },
    {
      "name": "long",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT"
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
            "tidbType": "INT"
          }
        }
      ]
    },
    {
      "name": "longlong",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidbType": "BIGINT"
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
            "tidbType": "BIGINT"
          }
        }
      ]
    },
    {
      "name": "tinyunsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT UNSIGNED"
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
            "tidbType": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "shortunsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT UNSIGNED"
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
            "tidbType": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "int24unsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT UNSIGNED"
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
            "tidbType": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "longunsigned",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidbType": "INT UNSIGNED"
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
            "tidbType": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "longlongunsigned",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidbType": "BIGINT UNSIGNED"
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
            "tidbType": "BIGINT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "float",
      "type": {
        "type": "double",
        "connect.parameters": {
          "tidbType": "FLOAT"
        }
      }
    },
    {
      "default": null,
      "name": "floatnullable",
      "type": [
        "null",
        {
          "type": "double",
          "connect.parameters": {
            "tidbType": "FLOAT"
          }
        }
      ]
    },
    {
      "name": "double",
      "type": {
        "type": "double",
        "connect.parameters": {
          "tidbType": "DOUBLE"
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
            "tidbType": "DOUBLE"
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
          "tidbType": "BIT"
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
            "tidbType": "BIT"
          }
        }
      ]
    },
    {
      "name": "decimal",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "DECIMAL"
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
            "tidbType": "DECIMAL"
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
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "mediumtext",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "text",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "longtext",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "varchar",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "varstring",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "string",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "tinyblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "mediumblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "blob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "longblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "varbinary",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "varbinary1",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "binary",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
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
          "tidbType": "ENUM"
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
            "tidbType": "ENUM"
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
          "tidbType": "SET"
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
            "tidbType": "SET"
          }
        }
      ]
    },
    {
      "name": "json",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "JSON"
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
            "tidbType": "JSON"
          }
        }
      ]
    },
    {
      "name": "date",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "DATE"
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
            "tidbType": "DATE"
          }
        }
      ]
    },
    {
      "name": "datetime",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "DATETIME"
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
            "tidbType": "DATETIME"
          }
        }
      ]
    },
    {
      "name": "timestamp",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TIMESTAMP"
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
            "tidbType": "TIMESTAMP"
          }
        }
      ]
    },
    {
      "name": "time",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TIME"
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
            "tidbType": "TIME"
          }
        }
      ]
    },
    {
      "name": "year",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "YEAR"
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
            "tidbType": "YEAR"
          }
        }
      ]
    }
  ]
}`

var expectedSchemaWithExtension = `{
  "type": "record",
  "name": "testdb.rowtoavroschema",
  "fields": [
    {
      "name": "tiny",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT"
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
            "tidbType": "INT"
          }
        }
      ]
    },
    {
      "name": "short",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT"
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
            "tidbType": "INT"
          }
        }
      ]
    },
    {
      "name": "int24",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT"
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
            "tidbType": "INT"
          }
        }
      ]
    },
    {
      "name": "long",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT"
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
            "tidbType": "INT"
          }
        }
      ]
    },
    {
      "name": "longlong",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidbType": "BIGINT"
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
            "tidbType": "BIGINT"
          }
        }
      ]
    },
    {
      "name": "tinyunsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT UNSIGNED"
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
            "tidbType": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "shortunsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT UNSIGNED"
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
            "tidbType": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "int24unsigned",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "INT UNSIGNED"
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
            "tidbType": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "longunsigned",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidbType": "INT UNSIGNED"
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
            "tidbType": "INT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "longlongunsigned",
      "type": {
        "type": "long",
        "connect.parameters": {
          "tidbType": "BIGINT UNSIGNED"
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
            "tidbType": "BIGINT UNSIGNED"
          }
        }
      ]
    },
    {
      "name": "float",
      "type": {
        "type": "double",
        "connect.parameters": {
          "tidbType": "FLOAT"
        }
      }
    },
    {
      "default": null,
      "name": "floatnullable",
      "type": [
        "null",
        {
          "type": "double",
          "connect.parameters": {
            "tidbType": "FLOAT"
          }
        }
      ]
    },
    {
      "name": "double",
      "type": {
        "type": "double",
        "connect.parameters": {
          "tidbType": "DOUBLE"
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
            "tidbType": "DOUBLE"
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
          "tidbType": "BIT"
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
            "tidbType": "BIT"
          }
        }
      ]
    },
    {
      "name": "decimal",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "DECIMAL"
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
            "tidbType": "DECIMAL"
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
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "mediumtext",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "text",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "longtext",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "varchar",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "varstring",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "string",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TEXT"
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
            "tidbType": "TEXT"
          }
        }
      ]
    },
    {
      "name": "tinyblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "mediumblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "blob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "longblob",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "varbinary",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "varbinary1",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
          }
        }
      ]
    },
    {
      "name": "binary",
      "type": {
        "type": "bytes",
        "connect.parameters": {
          "tidbType": "BLOB"
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
            "tidbType": "BLOB"
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
          "tidbType": "ENUM"
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
            "tidbType": "ENUM"
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
          "tidbType": "SET"
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
            "tidbType": "SET"
          }
        }
      ]
    },
    {
      "name": "json",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "JSON"
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
            "tidbType": "JSON"
          }
        }
      ]
    },
    {
      "name": "date",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "DATE"
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
            "tidbType": "DATE"
          }
        }
      ]
    },
    {
      "name": "datetime",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "DATETIME"
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
            "tidbType": "DATETIME"
          }
        }
      ]
    },
    {
      "name": "timestamp",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TIMESTAMP"
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
            "tidbType": "TIMESTAMP"
          }
        }
      ]
    },
    {
      "name": "time",
      "type": {
        "type": "string",
        "connect.parameters": {
          "tidbType": "TIME"
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
            "tidbType": "TIME"
          }
        }
      ]
    },
    {
      "name": "year",
      "type": {
        "type": "int",
        "connect.parameters": {
          "tidbType": "YEAR"
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
            "tidbType": "YEAR"
          }
        }
      ]
    },
    {
      "name": "tidbOp",
      "type": "string"
    },
    {
      "name": "tidbCommitTs",
      "type": "long"
    }
  ]
}`
