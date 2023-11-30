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
	"math"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestEncodeInsert(t *testing.T) {
	codec := &Codec{
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

	data, err := codec.EncodeRowChangedEvent(e)
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
	}`, string(data))
}

func TestEncodeUpdate(t *testing.T) {
	codec := &Codec{
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

	data, err := codec.EncodeRowChangedEvent(e)
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
	}`, string(data))
}

func TestEncodeDelete(t *testing.T) {
	codec := &Codec{
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

	data, err := codec.EncodeRowChangedEvent(e)
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
	}`, string(data))
}

func TestEncodeDataTypes(t *testing.T) {
	codec := &Codec{
		config:    common.NewConfig(config.ProtocolDebezium),
		clusterID: "test-cluster",
		nowFunc:   func() time.Time { return time.Unix(1701326309, 0) },
	}
	codec.config.TimeZone, _ = time.LoadLocation("Asia/Shanghai")

	e := &model.RowChangedEvent{
		CommitTs: 1,
		Table:    &model.TableName{Schema: "test", Table: "table1"},
		Columns: []*model.Column{{
			Name:  "tiny",
			Value: int64(1), Type: mysql.TypeTiny,
		}, {
			Name:  "varchar",
			Value: []byte("foo"), Type: mysql.TypeVarchar,
		}, {
			Name:  "varchar_bin",
			Value: []byte("foo"), Type: mysql.TypeVarchar, Flag: model.BinaryFlag,
		}, {
			Name:  "bit_1_1",
			Value: uint64(1), Type: mysql.TypeBit,
		}, {
			Name:  "bit_1_0",
			Value: uint64(0), Type: mysql.TypeBit,
		}, {
			Name:  "bit_4",
			Value: uint64(13), Type: mysql.TypeBit,
		}, {
			Name:  "decimal",
			Value: "129012.1230000", Type: mysql.TypeNewDecimal,
		}, {
			Name:  "bigint_unsigned",
			Value: uint64(math.MaxUint64), Type: mysql.TypeLonglong, Flag: model.UnsignedFlag,
		}, {
			Name:  "time",
			Value: "12:34:56.1234", Type: mysql.TypeDuration,
		}, {
			Name: "timestamp",
			// Note: This is the human readable time in the Asia/Shanghai timezone.
			Value: "2023-11-30 07:10:08.245", Type: mysql.TypeTimestamp,
		}, {
			Name:  "date",
			Value: "2023-11-30", Type: mysql.TypeDate,
		}},
		ColInfos: []rowcodec.ColInfo{{
			ID:            1,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeTiny),
		}, {
			ID:            2,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeVarchar),
		}, {
			ID:            3,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeVarchar),
		}, {
			ID:            4,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldTypeBuilder().SetType(mysql.TypeBit).SetFlen(1).BuildP(),
		}, {
			ID:            5,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldTypeBuilder().SetType(mysql.TypeBit).SetFlen(1).BuildP(),
		}, {
			ID:            6,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldTypeBuilder().SetType(mysql.TypeBit).SetFlen(4).BuildP(),
		}, {
			ID:            7,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeNewDecimal),
		}, {
			ID:            8,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldTypeBuilder().SetType(mysql.TypeLonglong).SetFlag(mysql.UnsignedFlag).BuildP(),
		}, {
			ID:            9,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldTypeBuilder().SetType(mysql.TypeDuration).SetDecimal(4).BuildP(),
		}, {
			ID:            10,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldTypeBuilder().SetType(mysql.TypeTimestamp).SetDecimal(3).BuildP(),
		}, {
			ID:            11,
			IsPKHandle:    false,
			VirtualGenCol: false,
			Ft:            types.NewFieldType(mysql.TypeDate),
		}},
	}

	data, err := codec.EncodeRowChangedEvent(e)
	require.Nil(t, err)
	require.JSONEq(t, `{
		"payload": {
			"before": null,
			"after": {
				"tiny": 1,
				"varchar": "foo",
				"varchar_bin": "Zm9v",
				"bit_1_0": false,
				"bit_1_1": true,
				"bit_4": "DQ==",
				"decimal": 129012.123,
				"bigint_unsigned": -1,
				"time": 45296123400,
				"timestamp": "2023-11-29T23:10:08.245Z",
				"date": 19691
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
	}`, string(data))
}
