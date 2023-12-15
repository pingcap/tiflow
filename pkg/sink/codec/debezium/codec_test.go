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
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
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

	e := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "test", Table: "table1"},
		Columns: []*model.Column{{
			Name:  "tiny",
			Value: int64(1), Type: mysql.TypeTiny,
		}},
		ColInfos: []rowcodec.ColInfo{{
			ID:            1,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeTiny),
		}},
	}

	buf := bytes.NewBuffer(nil)
	err := codec.EncodeRowChangedEvent(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `{
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
				"snapshot": false,
				"thread": 0,
				"version": "2.4.0.Final"
			},
			"ts_ms": 1701326309000,
			"transaction": null
		}
	}`, buf.String())
}

func TestEncodeUpdate(t *testing.T) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test-cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}

	e := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "test", Table: "table1"},
		Columns: []*model.Column{{
			Name:  "tiny",
			Value: int64(1), Type: mysql.TypeTiny,
		}},
		PreColumns: []*model.Column{{
			Name:  "tiny",
			Value: int64(2), Type: mysql.TypeTiny,
		}},
		ColInfos: []rowcodec.ColInfo{{
			ID:            1,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeTiny),
		}},
	}

	buf := bytes.NewBuffer(nil)
	err := codec.EncodeRowChangedEvent(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `{
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
				"snapshot": false,
				"thread": 0,
				"version": "2.4.0.Final"
			},
			"ts_ms": 1701326309000,
			"transaction": null
		}
	}`, buf.String())
}

func TestEncodeDelete(t *testing.T) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test-cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}

	e := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "test", Table: "table1"},
		PreColumns: []*model.Column{{
			Name:  "tiny",
			Value: int64(2), Type: mysql.TypeTiny,
		}},
		ColInfos: []rowcodec.ColInfo{{
			ID:            1,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeTiny),
		}},
	}

	buf := bytes.NewBuffer(nil)
	err := codec.EncodeRowChangedEvent(e, buf)
	require.Nil(t, err)
	require.JSONEq(t, `{
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
				"snapshot": false,
				"thread": 0,
				"version": "2.4.0.Final"
			},
			"ts_ms": 1701326309000,
			"transaction": null
		}
	}`, buf.String())
}

func BenchmarkEncodeOneTinyColumn(b *testing.B) {
	codec := &dbzCodec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test-cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}

	e := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "test", Table: "table1"},
		Columns: []*model.Column{{
			Name:  "tiny",
			Value: int64(10), Type: mysql.TypeTiny,
		}},
		ColInfos: []rowcodec.ColInfo{{
			ID:            1,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeTiny),
		}},
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

	e := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "test", Table: "table1"},
		Columns: []*model.Column{{
			Name:  "str",
			Value: []byte(randstr.String(1024)), Type: mysql.TypeVarchar,
		}},
		ColInfos: []rowcodec.ColInfo{{
			ID:            1,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeVarchar),
		}},
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

	e := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "test", Table: "table1"},
		Columns: []*model.Column{{
			Name:  "bin",
			Value: []byte(randstr.String(1024)), Type: mysql.TypeVarchar, Flag: model.BinaryFlag,
		}},
		ColInfos: []rowcodec.ColInfo{{
			ID:            1,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeVarchar),
		}},
	}

	buf := bytes.NewBuffer(nil)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf.Reset()
		codec.EncodeRowChangedEvent(e, buf)
	}
}
