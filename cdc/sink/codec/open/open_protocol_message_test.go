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

package open

import (
	"testing"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
	"github.com/stretchr/testify/require"
)

func TestFormatCol(t *testing.T) {
	t.Parallel()
	row := &messageRow{Update: map[string]internal.Column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.encode()
	require.Nil(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)

	row = &messageRow{Update: map[string]internal.Column{"test": {
		Type:  mysql.TypeBlob,
		Value: []byte("测"),
	}}}
	rowEncode, err = row.encode()
	require.Nil(t, err)
	row2 = new(messageRow)
	err = row2.decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
}

func TestNonBinaryStringCol(t *testing.T) {
	t.Parallel()
	col := &model.BoundedColumn{
		Name:  "test",
		Type:  mysql.TypeString,
		Value: "value",
	}
	mqCol := internal.Column{}
    mqCol.FromRowChangeColumn(col.Unbound(), model.ColumnValue { Value: col.Value })
	row := &messageRow{Update: map[string]internal.Column{"test": mqCol}}
	rowEncode, err := row.encode()
	require.Nil(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)

	mqCol2 := row2.Update["test"]
	col2, colv2 := mqCol2.ToRowChangeColumn("test")
	colv2.Value = string(colv2.Value.([]byte))
    require.Equal(t, col, &model.BoundedColumn{
        Name: col2.Name,
        Type: col2.Type,
        Value: colv2.Value,
    })
}

func TestVarBinaryCol(t *testing.T) {
	t.Parallel()
	col := &model.BoundedColumn{
		Name:  "test",
		Type:  mysql.TypeString,
		Flag:  model.BinaryFlag,
		Value: []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
	}
	mqCol := internal.Column{}
    mqCol.FromRowChangeColumn(col.Unbound(), model.ColumnValue { Value: col.Value })
	row := &messageRow{Update: map[string]internal.Column{"test": mqCol}}
	rowEncode, err := row.encode()
	require.Nil(t, err)

	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.Nil(t, err)
	require.Equal(t, row, row2)
	mqCol2 := row2.Update["test"]
	col2, colv2 := mqCol2.ToRowChangeColumn("test")
    require.Equal(t, col, &model.BoundedColumn{
        Name: col2.Name,
		Type:  col2.Type,
		Flag:  col2.Flag,
		Value: colv2.Value,
    })
}
