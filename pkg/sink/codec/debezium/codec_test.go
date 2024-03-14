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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"github.com/thanhpk/randstr"
)

func TestEncodeInsert(t *testing.T) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test-cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.DebeziumDisableSchema = true

	tableInfo := model.BuildTableInfo("test", "table1", []*model.Column{{
		Name: "tiny",
		Type: mysql.TypeTiny,
		Flag: model.NullableFlag,
	}}, nil)
	e := &model.RowChangedEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		Columns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "tiny",
			Value: int64(1),
		}}, tableInfo),
	}

	buf := bytes.NewBuffer(nil)
	err := codec.EncodeRowChangedEvent(e, buf)
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
				"cluster_id": "test-cluster",
				"name": "test-cluster",
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
	buf.Reset()
	err = codec.EncodeRowChangedEvent(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test-cluster",
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
				"cluster_id": "test-cluster"
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
			"name": "test-cluster.test.table1.Envelope",
			"version": 1,
			"fields": [
				{
					"type": "struct",
					"optional": true,
					"name": "test-cluster.test.table1.Value",
					"field": "before",
					"fields": [{ "type": "int16", "optional": true, "field": "tiny" }]
				},
				{
					"type": "struct",
					"optional": true,
					"name": "test-cluster.test.table1.Value",
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
		clusterID: "test-cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.DebeziumDisableSchema = true

	tableInfo := model.BuildTableInfo("test", "table1", []*model.Column{{
		Name: "tiny",
		Type: mysql.TypeTiny,
		Flag: model.NullableFlag,
	}}, nil)
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
	err := codec.EncodeRowChangedEvent(e, buf)
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
				"cluster_id": "test-cluster",
				"name": "test-cluster",
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
	buf.Reset()
	err = codec.EncodeRowChangedEvent(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test-cluster",
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
				"cluster_id": "test-cluster"
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
			"name": "test-cluster.test.table1.Envelope",
			"version": 1,
			"fields": [
				{
					"type": "struct",
					"optional": true,
					"name": "test-cluster.test.table1.Value",
					"field": "before",
					"fields": [{ "type": "int16", "optional": true, "field": "tiny" }]
				},
				{
					"type": "struct",
					"optional": true,
					"name": "test-cluster.test.table1.Value",
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

func TestEncodeDelete(t *testing.T) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test-cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.DebeziumDisableSchema = true

	tableInfo := model.BuildTableInfo("test", "table1", []*model.Column{{
		Name: "tiny",
		Type: mysql.TypeTiny,
		Flag: model.NullableFlag,
	}}, nil)
	e := &model.RowChangedEvent{
		CommitTs:  1,
		TableInfo: tableInfo,
		PreColumns: model.Columns2ColumnDatas([]*model.Column{{
			Name:  "tiny",
			Value: int64(2),
		}}, tableInfo),
	}

	buf := bytes.NewBuffer(nil)
	err := codec.EncodeRowChangedEvent(e, buf)
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
				"cluster_id": "test-cluster",
				"name": "test-cluster",
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
	buf.Reset()
	err = codec.EncodeRowChangedEvent(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `
	{
		"payload": {
			"source": {
				"version": "2.4.0.Final",
				"connector": "TiCDC",
				"name": "test-cluster",
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
				"cluster_id": "test-cluster"
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
			"name": "test-cluster.test.table1.Envelope",
			"version": 1,
			"fields": [
				{
					"type": "struct",
					"optional": true,
					"name": "test-cluster.test.table1.Value",
					"field": "before",
					"fields": [{ "type": "int16", "optional": true, "field": "tiny" }]
				},
				{
					"type": "struct",
					"optional": true,
					"name": "test-cluster.test.table1.Value",
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
		clusterID: "test-cluster",
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

	buf := bytes.NewBuffer(nil)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf.Reset()
		codec.EncodeRowChangedEvent(e, buf)
	}
}

func BenchmarkEncodeLargeText(b *testing.B) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test-cluster",
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

	buf := bytes.NewBuffer(nil)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf.Reset()
		codec.EncodeRowChangedEvent(e, buf)
	}
}

func BenchmarkEncodeLargeBinary(b *testing.B) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test-cluster",
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

	buf := bytes.NewBuffer(nil)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf.Reset()
		codec.EncodeRowChangedEvent(e, buf)
	}
}
