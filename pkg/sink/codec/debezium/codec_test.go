// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"bytes"
	"testing"
	"time"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
)

func TestDDLEvent(t *testing.T) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test_cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}

	query := "RENAME TABLE test.table1 to test.table2"
	tableInfo := model.BuildTableInfo("test", "table1", []*model.Column{{
		Name: "id",
		Type: mysql.TypeLong,
		Flag: model.PrimaryKeyFlag | model.HandleKeyFlag,
	}}, [][]int{{0}})
	preTableInfo := model.BuildTableInfo("test", "table2", []*model.Column{{
		Name: "id",
		Type: mysql.TypeLong,
		Flag: model.PrimaryKeyFlag | model.HandleKeyFlag,
	}}, [][]int{{0}})
	e := &model.DDLEvent{
		CommitTs:     1,
		TableInfo:    tableInfo,
		PreTableInfo: preTableInfo,
		Type:         timodel.ActionNone,
	}
	keyBuf := bytes.NewBuffer(nil)
	buf := bytes.NewBuffer(nil)
	err := codec.EncodeDDLEvent(e, keyBuf, buf)
	require.ErrorIs(t, err, cerror.ErrDDLUnsupportType)

	e = &model.DDLEvent{
		CommitTs:     1,
		TableInfo:    tableInfo,
		PreTableInfo: preTableInfo,
		Query:        query,
		Type:         timodel.ActionRenameTable,
	}
	keyBuf.Reset()
	buf.Reset()
	codec.config.DebeziumDisableSchema = false
	err = codec.EncodeDDLEvent(e, keyBuf, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"databaseName": "test"
		},
		"schema": {
			"type": "struct",
			"name": "io.debezium.connector.mysql.SchemaChangeKey",
			"optional": false,
			"version": 1,
			"fields": [
				{
					"field": "databaseName",
					"optional": false,
					"type": "string"
				}
			]
		}
	}`, keyBuf.String())
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test_cluster",
				"ts_ms": 0,
				"snapshot": "false",
				"db": "test",
				"table": "table1",
				"server_id": 0,
				"gtid": null,
				"file": "",
				"pos": 0,
				"row": 0,
				"thread": 0,
				"query": null,
				"commit_ts": 1,
				"cluster_id": "test_cluster"
			},
			"ts_ms": 1701326309000,
			"databaseName": "test", 
      		"schemaName": null,
    		"ddl": "RENAME TABLE test.table1 to test.table2", 
      		"tableChanges": [
				{
					"type": "ALTER", 
					"id": "\"test\".\"table2\",\"test\".\"table1\"", 
					"table": {    
						"defaultCharsetName": "",
						"primaryKeyColumnNames": ["id"],
						"columns": [
						    {
								"name": "id",
								"jdbcType": 4,
								"nativeType": null,
                           		"comment": null,
                            	"defaultValueExpression": null,
                            	"enumValues": null,
								"typeName": "INT",
								"typeExpression": "INT",
								"charsetName": null,
								"length": 0,
								"scale": null,
								"position": 1,
								"optional": false,
								"autoIncremented": false,
								"generated": false
							}
						],
                    	"comment": null
					}
				}
			]
		},
		"schema": {
			"optional": false,
			"type": "struct",
			"version": 1,
			"name": "io.debezium.connector.mysql.SchemaChangeValue",
			"fields": [
				{
					"field": "source",
					"name": "io.debezium.connector.mysql.Source",
					"optional": false,
					"type": "struct",
					"fields": [
						{
							"field": "version",
							"optional": false,
							"type": "string"
						},
						{
							"field": "connector",
							"optional": false,
							"type": "string"
						},
						{
							"field": "name",
							"optional": false,
							"type": "string"
						},
						{
							"field": "ts_ms",
							"optional": false,
							"type": "int64"
						},
						{
							"field": "snapshot",
							"optional": true,
							"type": "string",
							"parameters": {
								"allowed": "true,last,false,incremental"
							},
							"default": "false",
							"name": "io.debezium.data.Enum",
							"version": 1
						},
						{
							"field": "db",
							"optional": false,
							"type": "string"
						},
						{
							"field": "sequence",
							"optional": true,
							"type": "string"
						},
						{
							"field": "table",
							"optional": true,
							"type": "string"
						},
						{
							"field": "server_id",
							"optional": false,
							"type": "int64"
						},
						{
							"field": "gtid",
							"optional": true,
							"type": "string"
						},
						{
							"field": "file",
							"optional": false,
							"type": "string"
						},
						{
							"field": "pos",
							"optional": false,
							"type": "int64"
						},
						{
							"field": "row",
							"optional": false,
							"type": "int32"
						},
						{
							"field": "thread",
							"optional": true,
							"type": "int64"
						},
						{
							"field": "query",
							"optional": true,
							"type": "string"
						}
					]
				},
				{
					"field": "ts_ms",
					"optional": false,
					"type": "int64"
				},
				{
					"field": "databaseName",
					"optional": true,
					"type": "string"
				},
				{
					"field": "schemaName",
					"optional": true,
					"type": "string"
				},
				{
					"field": "ddl",
					"optional": true,
					"type": "string"
				},
				{
					"field": "tableChanges",
					"optional": false,
					"type": "array",
					"items": {
						"name": "io.debezium.connector.schema.Change",
						"optional": false,
						"type": "struct",
						"version": 1,
						"fields": [
							{
								"field": "type",
								"optional": false,
								"type": "string"
							},
							{
								"field": "id",
								"optional": false,
								"type": "string"
							},
							{
								"field": "table",
								"optional": true,
								"type": "struct",
								"name": "io.debezium.connector.schema.Table",
								"version": 1,
								"fields": [
									{
										"field": "defaultCharsetName",
										"optional": true,
										"type": "string"
									},
									{
										"field": "primaryKeyColumnNames",
										"optional": true,
										"type": "array",
										"items": {
											"type": "string",
											"optional": false
										}
									},
									{
										"field": "columns",
										"optional": false,
										"type": "array",
										"items": {
											"name": "io.debezium.connector.schema.Column",
											"optional": false,
											"type": "struct",
											"version": 1,
											"fields": [
												{
													"field": "name",
													"optional": false,
													"type": "string"
												},
												{
													"field": "jdbcType",
													"optional": false,
													"type": "int32"
												},
												{
													"field": "nativeType",
													"optional": true,
													"type": "int32"
												},
												{
													"field": "typeName",
													"optional": false,
													"type": "string"
												},
												{
													"field": "typeExpression",
													"optional": true,
													"type": "string"
												},
												{
													"field": "charsetName",
													"optional": true,
													"type": "string"
												},
												{
													"field": "length",
													"optional": true,
													"type": "int32"
												},
												{
													"field": "scale",
													"optional": true,
													"type": "int32"
												},
												{
													"field": "position",
													"optional": false,
													"type": "int32"
												},
												{
													"field": "optional",
													"optional": true,
													"type": "boolean"
												},
												{
													"field": "autoIncremented",
													"optional": true,
													"type": "boolean"
												},
												{
													"field": "generated",
													"optional": true,
													"type": "boolean"
												},
												{
													"field": "comment",
													"optional": true,
													"type": "string"
												},
												{
													"field": "defaultValueExpression",
													"optional": true,
													"type": "string"
												},
												{
													"field": "enumValues",
													"optional": true,
													"type": "array",
													"items": {
														"type": "string",
														"optional": false
													}
												}
											]
										}
									},
									{
										"field": "comment",
										"optional": true,
										"type": "string"
									}
								]
							}
						]
					}
				}
			]
		}
	}`, buf.String())

	codec.config.DebeziumDisableSchema = true

	query = "CREATE TABLE test.table1"
	e = &model.DDLEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		Query:     query,
		Type:      timodel.ActionCreateTable,
	}
	keyBuf.Reset()
	buf.Reset()
	err = codec.EncodeDDLEvent(e, keyBuf, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"databaseName": "test"
		}
	}`, keyBuf.String())
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test_cluster",
				"ts_ms": 0,
				"snapshot": "false",
				"db": "test",
				"table": "table1",
				"server_id": 0,
				"gtid": null,
				"file": "",
				"pos": 0,
				"row": 0,
				"thread": 0,
				"query": null,
				"commit_ts": 1,
				"cluster_id": "test_cluster"
			},
			"ts_ms": 1701326309000,
			"databaseName": "test", 
      		"schemaName": null,
    		"ddl": "CREATE TABLE test.table1", 
      		"tableChanges": [
				{
					"type": "CREATE", 
					"id": "\"test\".\"table1\"", 
					"table": {    
						"defaultCharsetName": "",
						"primaryKeyColumnNames": ["id"],
						"columns": [
						    {
								"name": "id",
								"jdbcType": 4,
								"nativeType": null,
								"comment": null,
								"defaultValueExpression": null,
								"enumValues": null,
								"typeName": "INT",
								"typeExpression": "INT",
								"charsetName": null,
								"length": 0,
								"scale": null,
								"position": 1,
								"optional": false,
								"autoIncremented": false,
								"generated": false
							}
						],
						"comment": null
					}
				}
			]
		}
	}`, buf.String())

	query = "DROP TABLE test.table2"
	e = &model.DDLEvent{
		CommitTs:     1,
		PreTableInfo: preTableInfo,
		Query:        query,
		Type:         timodel.ActionDropTable,
	}
	keyBuf.Reset()
	buf.Reset()
	err = codec.EncodeDDLEvent(e, keyBuf, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"databaseName": "test"
		}
	}`, keyBuf.String())
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test_cluster",
				"ts_ms": 0,
				"snapshot": "false",
				"db": "",
				"table": "",
				"server_id": 0,
				"gtid": null,
				"file": "",
				"pos": 0,
				"row": 0,
				"thread": 0,
				"query": null,
				"commit_ts": 1,
				"cluster_id": "test_cluster"
			},
			"ts_ms": 1701326309000,
			"databaseName": "test", 
      		"schemaName": null,
    		"ddl": "DROP TABLE test.table2",
			"tableChanges": []
		}
	}`, buf.String())
}

func TestCheckPointEvent(t *testing.T) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test_cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.DebeziumDisableSchema = false

	var ts uint64 = 3
	keyBuf := bytes.NewBuffer(nil)
	buf := bytes.NewBuffer(nil)
	err := codec.EncodeCheckpointEvent(ts, keyBuf, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {},
		"schema": {
			"fields": [],
			"optional": false,
			"name": "test_cluster.watermark.Key",
			"type": "struct"
		}
	}`, keyBuf.String())
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test_cluster",
				"ts_ms": 0,
				"snapshot": "false",
				"db": "",
				"table": "",
				"server_id": 0,
				"gtid": null,
				"file": "",
				"pos": 0,
				"row": 0,
				"thread": 0,
				"query": null,
				"commit_ts": 3,
				"cluster_id": "test_cluster"
			},
			"op":"m",
			"ts_ms": 1701326309000,
			"transaction": null
		},
		"schema": {
			"type": "struct",
			"optional": false,
			"name": "test_cluster.watermark.Envelope",
			"version": 1,
			"fields": [
				{
					"type": "struct",
					"fields": [
						{
							"type": "string",
							"optional": false,
							"field": "version"
						},
						{
							"type": "string",
							"optional": false,
							"field": "connector"
						},
						{
							"type": "string",
							"optional": false,
							"field": "name"
						},
						{
							"type": "int64",
							"optional": false,
							"field": "ts_ms"
						},
						{
							"type": "string",
							"optional": true,
							"name": "io.debezium.data.Enum",
							"version": 1,
							"parameters": {
								"allowed": "true,last,false,incremental"
							},
							"default": "false",
							"field": "snapshot"
						},
						{
							"type": "string",
							"optional": false,
							"field": "db"
						},
						{
							"type": "string",
							"optional": true,
							"field": "sequence"
						},
						{
							"type": "string",
							"optional": true,
							"field": "table"
						},
						{
							"type": "int64",
							"optional": false,
							"field": "server_id"
						},
						{
							"type": "string",
							"optional": true,
							"field": "gtid"
						},
						{
							"type": "string",
							"optional": false,
							"field": "file"
						},
						{
							"type": "int64",
							"optional": false,
							"field": "pos"
						},
						{
							"type": "int32",
							"optional": false,
							"field": "row"
						},
						{
							"type": "int64",
							"optional": true,
							"field": "thread"
						},
						{
							"type": "string",
							"optional": true,
							"field": "query"
						}
					],
					"optional": false,
					"name": "io.debezium.connector.mysql.Source",
					"field": "source"
				},
				{
					"type": "string",
					"optional": false,
					"field": "op"
				},
				{
					"type": "int64",
					"optional": true,
					"field": "ts_ms"
				},
				{
					"type": "struct",
					"fields": [
						{
							"type": "string",
							"optional": false,
							"field": "id"
						},
						{
							"type": "int64",
							"optional": false,
							"field": "total_order"
						},
						{
							"type": "int64",
							"optional": false,
							"field": "data_collection_order"
						}
					],
					"optional": true,
					"name": "event.block",
					"version": 1,
					"field": "transaction"
				}
			]
		}
	}`, buf.String())
}

func TestEncodeInsert(t *testing.T) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test_cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.DebeziumDisableSchema = true
	codec.config.DebeziumOutputOldValue = false

	tableInfo := model.BuildTableInfo("test", "table1", []*model.Column{{
		Name: "tiny",
		Type: mysql.TypeTiny,
		Flag: model.NullableFlag | model.HandleKeyFlag | model.PrimaryKeyFlag,
	}}, [][]int{{0}})
	e := &model.RowChangedEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		Columns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "tiny",
			Value: int64(1),
		}}, tableInfo),
	}

	buf := bytes.NewBuffer(nil)
	keyBuf := bytes.NewBuffer(nil)
	err := codec.EncodeKey(e, keyBuf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"tiny": 1
		}
	}
	`, keyBuf.String())
	err = codec.EncodeValue(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"before": null,
			"after": {
				"tiny": 1
			},
			"op": "c",
			"source": {
				"cluster_id": "test_cluster",
				"name": "test_cluster",
				"commit_ts": 1,
				"connector": "TiCDC",
				"db": "test",
				"table": "table1",
				"ts_ms": 0,
				"file": "",
				"gtid": null,
				"pos": 0,
				"query": null,
				"row": 0,
				"server_id": 0,
				"snapshot": "false",
				"thread": 0,
				"version": "2.4.0.Final"
			},
			"ts_ms": 1701326309000,
			"transaction": null
		}
	}
	`, buf.String())

	codec.config.DebeziumDisableSchema = false
	keyBuf.Reset()
	err = codec.EncodeKey(e, keyBuf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"tiny": 1
		},
		"schema": {
			"fields": [
			{
				"field":"tiny",
				"optional":true,
				"type":"int16"
			}
			],
			"name": "test_cluster.test.table1.Key",
			"optional": false,
			"type":"struct"
		}
	}
	`, keyBuf.String())
	buf.Reset()
	err = codec.EncodeValue(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test_cluster",
				"ts_ms": 0,
				"snapshot": "false",
				"db": "test",
				"table": "table1",
				"server_id": 0,
				"gtid": null,
				"file": "",
				"pos": 0,
				"row": 0,
				"thread": 0,
				"query": null,
				"commit_ts": 1,
				"cluster_id": "test_cluster"
			},
			"ts_ms": 1701326309000,
			"transaction": null,
			"op": "c",
			"before": null,
			"after": { "tiny": 1 }
		},
		"schema": {
			"type": "struct",
			"optional": false,
			"name": "test_cluster.test.table1.Envelope",
			"version": 1,
			"fields": [
				{
					"type": "struct",
					"optional": true,
					"name": "test_cluster.test.table1.Value",
					"field": "before",
					"fields": [{ "type": "int16", "optional": true, "field": "tiny" }]
				},
				{
					"type": "struct",
					"optional": true,
					"name": "test_cluster.test.table1.Value",
					"field": "after",
					"fields": [{ "type": "int16", "optional": true, "field": "tiny" }]
				},
				{
					"type": "struct",
					"fields": [
						{ "type": "string", "optional": false, "field": "version" },
						{ "type": "string", "optional": false, "field": "connector" },
						{ "type": "string", "optional": false, "field": "name" },
						{ "type": "int64", "optional": false, "field": "ts_ms" },
						{
							"type": "string",
							"optional": true,
							"name": "io.debezium.data.Enum",
							"version": 1,
							"parameters": { "allowed": "true,last,false,incremental" },
							"default": "false",
							"field": "snapshot"
						},
						{ "type": "string", "optional": false, "field": "db" },
						{ "type": "string", "optional": true, "field": "sequence" },
						{ "type": "string", "optional": true, "field": "table" },
						{ "type": "int64", "optional": false, "field": "server_id" },
						{ "type": "string", "optional": true, "field": "gtid" },
						{ "type": "string", "optional": false, "field": "file" },
						{ "type": "int64", "optional": false, "field": "pos" },
						{ "type": "int32", "optional": false, "field": "row" },
						{ "type": "int64", "optional": true, "field": "thread" },
						{ "type": "string", "optional": true, "field": "query" }
					],
					"optional": false,
					"name": "io.debezium.connector.mysql.Source",
					"field": "source"
				},
				{ "type": "string", "optional": false, "field": "op" },
				{ "type": "int64", "optional": true, "field": "ts_ms" },
				{
					"type": "struct",
					"fields": [
						{ "type": "string", "optional": false, "field": "id" },
						{ "type": "int64", "optional": false, "field": "total_order" },
						{
							"type": "int64",
							"optional": false,
							"field": "data_collection_order"
						}
					],
					"optional": true,
					"name": "event.block",
					"version": 1,
					"field": "transaction"
				}
			]
		}
	}
	`, buf.String())
}

func TestEncodeUpdate(t *testing.T) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test_cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.DebeziumDisableSchema = true

	tableInfo := model.BuildTableInfo("test", "table1", []*model.Column{{
		Name: "tiny",
		Type: mysql.TypeTiny,
		Flag: model.NullableFlag | model.HandleKeyFlag | model.PrimaryKeyFlag,
	}}, [][]int{{0}})
	e := &model.RowChangedEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		Columns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "tiny",
			Value: int64(1),
		}}, tableInfo),
		PreColumns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "tiny",
			Value: int64(2),
		}}, tableInfo),
	}

	buf := bytes.NewBuffer(nil)
	keyBuf := bytes.NewBuffer(nil)
	err := codec.EncodeKey(e, keyBuf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"tiny": 1
		}
	}
	`, keyBuf.String())

	err = codec.EncodeValue(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"before": {
				"tiny": 2
			},
			"after": {
				"tiny": 1
			},
			"op": "u",
			"source": {
				"cluster_id": "test_cluster",
				"name": "test_cluster",
				"commit_ts": 1,
				"connector": "TiCDC",
				"db": "test",
				"table": "table1",
				"ts_ms": 0,
				"file": "",
				"gtid": null,
				"pos": 0,
				"query": null,
				"row": 0,
				"server_id": 0,
				"snapshot": "false",
				"thread": 0,
				"version": "2.4.0.Final"
			},
			"ts_ms": 1701326309000,
			"transaction": null
		}
	}
	`, buf.String())

	codec.config.DebeziumDisableSchema = false
	keyBuf.Reset()
	err = codec.EncodeKey(e, keyBuf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"tiny": 1
		},
		"schema": {
			"fields": [
			{
				"field":"tiny",
				"optional":true,
				"type":"int16"
			}
			],
			"name": "test_cluster.test.table1.Key",
			"optional": false,
			"type":"struct"
		}
	}
	`, keyBuf.String())

	buf.Reset()
	err = codec.EncodeValue(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test_cluster",
				"ts_ms": 0,
				"snapshot": "false",
				"db": "test",
				"table": "table1",
				"server_id": 0,
				"gtid": null,
				"file": "",
				"pos": 0,
				"row": 0,
				"thread": 0,
				"query": null,
				"commit_ts": 1,
				"cluster_id": "test_cluster"
			},
			"ts_ms": 1701326309000,
			"transaction": null,
			"op": "u",
			"before": { "tiny": 2 },
			"after": { "tiny": 1 }
		},
		"schema": {
			"type": "struct",
			"optional": false,
			"name": "test_cluster.test.table1.Envelope",
			"version": 1,
			"fields": [
				{
					"type": "struct",
					"optional": true,
					"name": "test_cluster.test.table1.Value",
					"field": "before",
					"fields": [{ "type": "int16", "optional": true, "field": "tiny" }]
				},
				{
					"type": "struct",
					"optional": true,
					"name": "test_cluster.test.table1.Value",
					"field": "after",
					"fields": [{ "type": "int16", "optional": true, "field": "tiny" }]
				},
				{
					"type": "struct",
					"fields": [
						{ "type": "string", "optional": false, "field": "version" },
						{ "type": "string", "optional": false, "field": "connector" },
						{ "type": "string", "optional": false, "field": "name" },
						{ "type": "int64", "optional": false, "field": "ts_ms" },
						{
							"type": "string",
							"optional": true,
							"name": "io.debezium.data.Enum",
							"version": 1,
							"parameters": { "allowed": "true,last,false,incremental" },
							"default": "false",
							"field": "snapshot"
						},
						{ "type": "string", "optional": false, "field": "db" },
						{ "type": "string", "optional": true, "field": "sequence" },
						{ "type": "string", "optional": true, "field": "table" },
						{ "type": "int64", "optional": false, "field": "server_id" },
						{ "type": "string", "optional": true, "field": "gtid" },
						{ "type": "string", "optional": false, "field": "file" },
						{ "type": "int64", "optional": false, "field": "pos" },
						{ "type": "int32", "optional": false, "field": "row" },
						{ "type": "int64", "optional": true, "field": "thread" },
						{ "type": "string", "optional": true, "field": "query" }
					],
					"optional": false,
					"name": "io.debezium.connector.mysql.Source",
					"field": "source"
				},
				{ "type": "string", "optional": false, "field": "op" },
				{ "type": "int64", "optional": true, "field": "ts_ms" },
				{
					"type": "struct",
					"fields": [
						{ "type": "string", "optional": false, "field": "id" },
						{ "type": "int64", "optional": false, "field": "total_order" },
						{
							"type": "int64",
							"optional": false,
							"field": "data_collection_order"
						}
					],
					"optional": true,
					"name": "event.block",
					"version": 1,
					"field": "transaction"
				}
			]
		}
	}
	`, buf.String())

	codec.config.DebeziumOutputOldValue = false
	codec.config.DebeziumDisableSchema = true

	keyBuf.Reset()
	err = codec.EncodeKey(e, keyBuf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"tiny": 1
		}
	}
	`, keyBuf.String())

	buf.Reset()
	err = codec.EncodeValue(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test_cluster",
				"ts_ms": 0,
				"snapshot": "false",
				"db": "test",
				"table": "table1",
				"server_id": 0,
				"gtid": null,
				"file": "",
				"pos": 0,
				"row": 0,
				"thread": 0,
				"query": null,
				"commit_ts": 1,
				"cluster_id": "test_cluster"
			},
			"ts_ms": 1701326309000,
			"transaction": null,
			"op": "u",
			"after": { "tiny": 1 }
		}
	}
	`, buf.String())
}

func TestEncodeDelete(t *testing.T) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test_cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.DebeziumOutputOldValue = false
	codec.config.DebeziumDisableSchema = true

	tableInfo := model.BuildTableInfo("test", "table1", []*model.Column{{
		Name: "tiny",
		Type: mysql.TypeTiny,
		Flag: model.NullableFlag | model.HandleKeyFlag | model.PrimaryKeyFlag,
	}}, [][]int{{0}})
	e := &model.RowChangedEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		PreColumns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "tiny",
			Value: int64(2),
		}}, tableInfo),
	}

	buf := bytes.NewBuffer(nil)
	keyBuf := bytes.NewBuffer(nil)
	err := codec.EncodeKey(e, keyBuf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"tiny": 2
		}
	}
	`, keyBuf.String())

	err = codec.EncodeValue(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"before": {
				"tiny": 2
			},
			"after": null,
			"op": "d",
			"source": {
				"cluster_id": "test_cluster",
				"name": "test_cluster",
				"commit_ts": 1,
				"connector": "TiCDC",
				"db": "test",
				"table": "table1",
				"ts_ms": 0,
				"file": "",
				"gtid": null,
				"pos": 0,
				"query": null,
				"row": 0,
				"server_id": 0,
				"snapshot": "false",
				"thread": 0,
				"version": "2.4.0.Final"
			},
			"ts_ms": 1701326309000,
			"transaction": null
		}
	}
	`, buf.String())

	codec.config.DebeziumDisableSchema = false

	keyBuf.Reset()
	err = codec.EncodeKey(e, keyBuf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"tiny": 2
		},
		"schema": {
			"fields": [
			{
				"field":"tiny",
				"optional":true,
				"type":"int16"
			}
			],
			"name": "test_cluster.test.table1.Key",
			"optional": false,
			"type":"struct"
		}
	}
	`, keyBuf.String())

	buf.Reset()
	err = codec.EncodeValue(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test_cluster",
				"ts_ms": 0,
				"snapshot": "false",
				"db": "test",
				"table": "table1",
				"server_id": 0,
				"gtid": null,
				"file": "",
				"pos": 0,
				"row": 0,
				"thread": 0,
				"query": null,
				"commit_ts": 1,
				"cluster_id": "test_cluster"
			},
			"ts_ms": 1701326309000,
			"transaction": null,
			"op": "d",
			"after": null,
			"before": { "tiny": 2 }
		},
		"schema": {
			"type": "struct",
			"optional": false,
			"name": "test_cluster.test.table1.Envelope",
			"version": 1,
			"fields": [
				{
					"type": "struct",
					"optional": true,
					"name": "test_cluster.test.table1.Value",
					"field": "before",
					"fields": [{ "type": "int16", "optional": true, "field": "tiny" }]
				},
				{
					"type": "struct",
					"optional": true,
					"name": "test_cluster.test.table1.Value",
					"field": "after",
					"fields": [{ "type": "int16", "optional": true, "field": "tiny" }]
				},
				{
					"type": "struct",
					"fields": [
						{ "type": "string", "optional": false, "field": "version" },
						{ "type": "string", "optional": false, "field": "connector" },
						{ "type": "string", "optional": false, "field": "name" },
						{ "type": "int64", "optional": false, "field": "ts_ms" },
						{
							"type": "string",
							"optional": true,
							"name": "io.debezium.data.Enum",
							"version": 1,
							"parameters": { "allowed": "true,last,false,incremental" },
							"default": "false",
							"field": "snapshot"
						},
						{ "type": "string", "optional": false, "field": "db" },
						{ "type": "string", "optional": true, "field": "sequence" },
						{ "type": "string", "optional": true, "field": "table" },
						{ "type": "int64", "optional": false, "field": "server_id" },
						{ "type": "string", "optional": true, "field": "gtid" },
						{ "type": "string", "optional": false, "field": "file" },
						{ "type": "int64", "optional": false, "field": "pos" },
						{ "type": "int32", "optional": false, "field": "row" },
						{ "type": "int64", "optional": true, "field": "thread" },
						{ "type": "string", "optional": true, "field": "query" }
					],
					"optional": false,
					"name": "io.debezium.connector.mysql.Source",
					"field": "source"
				},
				{ "type": "string", "optional": false, "field": "op" },
				{ "type": "int64", "optional": true, "field": "ts_ms" },
				{
					"type": "struct",
					"fields": [
						{ "type": "string", "optional": false, "field": "id" },
						{ "type": "int64", "optional": false, "field": "total_order" },
						{
							"type": "int64",
							"optional": false,
							"field": "data_collection_order"
						}
					],
					"optional": true,
					"name": "event.block",
					"version": 1,
					"field": "transaction"
				}
			]
		}
	}
	`, buf.String())
}

func BenchmarkEncodeOneTinyColumn(b *testing.B) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test_cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.DebeziumDisableSchema = true

	tableInfo := model.BuildTableInfo("test", "table1", []*model.Column{{
		Name: "tiny",
		Type: mysql.TypeTiny,
	}}, nil)
	e := &model.RowChangedEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		Columns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "tiny",
			Value: int64(10),
		}}, tableInfo),
	}

	keyBuf := bytes.NewBuffer(nil)
	buf := bytes.NewBuffer(nil)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		keyBuf.Reset()
		buf.Reset()
		codec.EncodeKey(e, keyBuf)
		codec.EncodeValue(e, buf)
	}
}

func BenchmarkEncodeLargeText(b *testing.B) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test_cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.DebeziumDisableSchema = true

	tableInfo := model.BuildTableInfo("test", "table1", []*model.Column{{
		Name: "str",
		Type: mysql.TypeVarchar,
	}}, nil)
	e := &model.RowChangedEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		Columns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "str",
			Value: []byte(randstr.String(1024)),
		}}, tableInfo),
	}

	keyBuf := bytes.NewBuffer(nil)
	buf := bytes.NewBuffer(nil)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		keyBuf.Reset()
		buf.Reset()
		codec.EncodeKey(e, keyBuf)
		codec.EncodeValue(e, buf)
	}
}

func BenchmarkEncodeLargeBinary(b *testing.B) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test_cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.DebeziumDisableSchema = true

	tableInfo := model.BuildTableInfo("test", "table1", []*model.Column{{
		Name: "bin",
		Type: mysql.TypeVarchar,
		Flag: model.BinaryFlag,
	}}, nil)
	e := &model.RowChangedEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		Columns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "bin",
			Value: []byte(randstr.String(1024)),
		}}, tableInfo),
	}

	keyBuf := bytes.NewBuffer(nil)
	buf := bytes.NewBuffer(nil)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		keyBuf.Reset()
		buf.Reset()
		codec.EncodeKey(e, keyBuf)
		codec.EncodeValue(e, buf)
	}
}
