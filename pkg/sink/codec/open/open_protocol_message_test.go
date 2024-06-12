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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	"github.com/stretchr/testify/require"
)

func TestFormatCol(t *testing.T) {
	t.Parallel()
	row := &messageRow{Update: map[string]internal.Column{"test": {
		Type:  mysql.TypeString,
		Value: "测",
	}}}
	rowEncode, err := row.encode()
	require.NoError(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.NoError(t, err)
	require.Equal(t, row, row2)
	//
	row = &messageRow{Update: map[string]internal.Column{"test": {
		Type:  mysql.TypeBlob,
		Value: []byte("测"),
	}}}
	rowEncode, err = row.encode()
	require.NoError(t, err)
	row2 = new(messageRow)
	err = row2.decode(rowEncode)
	require.NoError(t, err)
	require.Equal(t, row, row2)
}

func TestNonBinaryStringCol(t *testing.T) {
	t.Parallel()
	col := &model.Column{
		Name:  "test",
		Type:  mysql.TypeString,
		Value: "value",
	}
	mqCol := internal.Column{}
	mqCol.FromRowChangeColumn(col)
	row := &messageRow{Update: map[string]internal.Column{"test": mqCol}}
	rowEncode, err := row.encode()
	require.NoError(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.NoError(t, err)
	require.Equal(t, row, row2)
	mqCol2 := row2.Update["test"]
	col2 := mqCol2.ToRowChangeColumn("test")
	col2.Value = string(col2.Value.([]byte))
	require.Equal(t, col, col2)
}

func TestVarBinaryCol(t *testing.T) {
	t.Parallel()
	col := &model.Column{
		Name:  "test",
		Type:  mysql.TypeString,
		Flag:  model.BinaryFlag,
		Value: []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A},
	}
	mqCol := internal.Column{}
	mqCol.FromRowChangeColumn(col)
	row := &messageRow{Update: map[string]internal.Column{"test": mqCol}}
	rowEncode, err := row.encode()
	require.NoError(t, err)
	row2 := new(messageRow)
	err = row2.decode(rowEncode)
	require.NoError(t, err)
	require.Equal(t, row, row2)
	mqCol2 := row2.Update["test"]
	col2 := mqCol2.ToRowChangeColumn("test")
	require.Equal(t, col, col2)
}

func TestOnlyOutputUpdatedColumn(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	_ = helper.DDL2Event(`create table test.t (a int primary key, b int, c int)`)
	event := helper.DML2Event(`insert into test.t values (1, 1, 1)`, "test", "t")
	event.PreColumns = make([]*model.ColumnData, len(event.Columns))
	for idx, col := range event.Columns {
		event.PreColumns[idx] = &model.ColumnData{
			ColumnID:         col.ColumnID,
			Value:            col.Value,
			ApproximateBytes: 0,
		}
	}
	codecConfig := common.NewConfig(config.ProtocolOpen)
	codecConfig.OnlyOutputUpdatedColumns = true

	// column not updated, so ignore it.
	_, row, err := rowChangeToMsg(event, codecConfig, false)
	require.NoError(t, err)
	require.Len(t, row.PreColumns, 0)

	event.TableInfo.Columns[1].FieldType = *types.NewFieldType(mysql.TypeFloat)
	event.Columns[1].Value = float32(event.Columns[1].Value.(int64))
	event.PreColumns[1].Value = float64(event.PreColumns[1].Value.(int64))
	_, row, err = rowChangeToMsg(event, codecConfig, false)
	require.NoError(t, err)
	_, ok := row.PreColumns["b"]
	require.True(t, ok)

	event.TableInfo.Columns[1].FieldType = *types.NewFieldType(mysql.TypeLong)
	event.Columns[1].Value = int64(event.Columns[1].Value.(float32))
	event.PreColumns[1].Value = uint64(event.PreColumns[1].Value.(float64))
	_, row, err = rowChangeToMsg(event, codecConfig, false)
	require.NoError(t, err)
	_, ok = row.PreColumns["b"]
	require.True(t, ok)
}

func TestRowChanged2MsgOnlyHandleKeyColumns(t *testing.T) {
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.ForceReplicate = true

	helper := entry.NewSchemaTestHelperWithReplicaConfig(t, replicaConfig)
	defer helper.Close()

	_ = helper.DDL2Event(`create table test.t(id int primary key, a int)`)
	insertEvent := helper.DML2Event(`insert into test.t values (1, 1)`, "test", "t")

	config := common.NewConfig(config.ProtocolOpen)
	config.DeleteOnlyHandleKeyColumns = true

	_, value, err := rowChangeToMsg(insertEvent, config, false)
	require.NoError(t, err)
	require.Contains(t, value.Update, "id")
	require.Contains(t, value.Update, "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value, err := rowChangeToMsg(insertEvent, config, true)
	require.NoError(t, err)
	require.True(t, key.OnlyHandleKey)
	require.Contains(t, value.Update, "id")
	require.NotContains(t, value.Update, "a")

	_ = helper.DDL2Event(`create table test.t1(id varchar(10), a varchar(10))`)
	insertEventNoHandleKey := helper.DML2Event(`insert into test.t1 values ("1", "1")`, "test", "t1")
	_, _, err = rowChangeToMsg(insertEventNoHandleKey, config, true)
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)

	updateEvent := *insertEvent
	updateEvent.PreColumns = updateEvent.Columns

	config.DeleteOnlyHandleKeyColumns = true
	_, value, err = rowChangeToMsg(&updateEvent, config, false)
	require.NoError(t, err)
	require.Contains(t, value.PreColumns, "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value, err = rowChangeToMsg(&updateEvent, config, true)
	require.NoError(t, err)
	require.True(t, key.OnlyHandleKey)
	require.NotContains(t, value.PreColumns, "a")
	require.NotContains(t, value.Update, "a")

	updateEventNoHandleKey := *insertEventNoHandleKey
	updateEventNoHandleKey.PreColumns = updateEventNoHandleKey.Columns
	_, _, err = rowChangeToMsg(&updateEventNoHandleKey, config, true)
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)

	deleteEvent := *insertEvent
	deleteEvent.PreColumns = deleteEvent.Columns
	deleteEvent.Columns = nil
	config.DeleteOnlyHandleKeyColumns = true
	_, value, err = rowChangeToMsg(&deleteEvent, config, false)
	require.NoError(t, err)
	require.Contains(t, value.Delete, "id")
	require.NotContains(t, value.Delete, "a")

	config.DeleteOnlyHandleKeyColumns = false
	_, value, err = rowChangeToMsg(&deleteEvent, config, false)
	require.NoError(t, err)
	require.Contains(t, value.Delete, "id")
	require.Contains(t, value.Delete, "a")

	config.DeleteOnlyHandleKeyColumns = false
	key, value, err = rowChangeToMsg(&deleteEvent, config, true)
	require.NoError(t, err)
	require.True(t, key.OnlyHandleKey)
	require.NotContains(t, value.Delete, "a")

	deleteEventNoHandleKey := *insertEventNoHandleKey
	deleteEventNoHandleKey.PreColumns = deleteEvent.Columns
	deleteEventNoHandleKey.Columns = nil
	_, _, err = rowChangeToMsg(&deleteEventNoHandleKey, config, true)
	require.Error(t, err, cerror.ErrOpenProtocolCodecInvalidData)
}
