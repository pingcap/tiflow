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
	"bytes"
	"encoding/json"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type messageRow struct {
	Update     map[string]internal.Column `json:"u,omitempty"`
	PreColumns map[string]internal.Column `json:"p,omitempty"`
	Delete     map[string]internal.Column `json:"d,omitempty"`
}

func (m *messageRow) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *messageRow) decode(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	err := decoder.Decode(m)
	if err != nil {
		return cerror.WrapError(cerror.ErrUnmarshalFailed, err)
	}
	for colName, column := range m.Update {
		m.Update[colName] = internal.FormatColumn(column)
	}
	for colName, column := range m.Delete {
		m.Delete[colName] = internal.FormatColumn(column)
	}
	for colName, column := range m.PreColumns {
		m.PreColumns[colName] = internal.FormatColumn(column)
	}
	return nil
}

type messageDDL struct {
	Query string             `json:"q"`
	Type  timodel.ActionType `json:"t"`
}

func (m *messageDDL) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *messageDDL) decode(data []byte) error {
	return cerror.WrapError(cerror.ErrUnmarshalFailed, json.Unmarshal(data, m))
}

func newResolvedMessage(ts uint64) *internal.MessageKey {
	return &internal.MessageKey{
		Ts:   ts,
		Type: model.MessageTypeResolved,
	}
}

func rowChangeToMsg(e *model.RowChangedEvent) (*internal.MessageKey, *messageRow) {
	var partition *int64
	if e.Table.IsPartition {
		partition = &e.Table.TableID
	}
	key := &internal.MessageKey{
		Ts:        e.CommitTs,
		Schema:    e.Table.Schema,
		Table:     e.Table.Table,
		RowID:     e.RowID,
		Partition: partition,
		Type:      model.MessageTypeRow,
	}
	value := &messageRow{}
	if e.IsDelete() {
		value.Delete = rowChangeColumns2CodecColumns(e.PreColumns, e.PreColumnValues)
	} else {
		value.Update = rowChangeColumns2CodecColumns(e.Columns, e.ColumnValues)
		value.PreColumns = rowChangeColumns2CodecColumns(e.PreColumns, e.PreColumnValues)
	}
	return key, value
}

func msgToRowChange(key *internal.MessageKey, value *messageRow) *model.RowChangedEvent {
	e := new(model.RowChangedEvent)
	// TODO: we lost the startTs from kafka message
	// startTs-based txn filter is out of work
	e.CommitTs = key.Ts
	e.Table = &model.TableName{
		Schema: key.Schema,
		Table:  key.Table,
	}
	// TODO: we lost the tableID from kafka message
	if key.Partition != nil {
		e.Table.TableID = *key.Partition
		e.Table.IsPartition = true
	}

	if len(value.Delete) != 0 {
		e.PreColumns, e.PreColumnValues = codecColumns2RowChangeColumns(value.Delete)
	} else {
		e.Columns, e.ColumnValues = codecColumns2RowChangeColumns(value.Update)
		e.PreColumns, e.PreColumnValues = codecColumns2RowChangeColumns(value.PreColumns)
	}
	return e
}

func rowChangeColumns2CodecColumns(cols []*model.Column, colvs []model.ColumnValue) map[string]internal.Column {
	jsonCols := make(map[string]internal.Column, len(cols))
	for i, col := range cols {
		if col == nil {
			continue
		}
		c := internal.Column{}
		c.FromRowChangeColumn(col, colvs[i])
		jsonCols[col.Name] = c
	}
	if len(jsonCols) == 0 {
		return nil
	}
	return jsonCols
}

func codecColumns2RowChangeColumns(cols map[string]internal.Column) ([]*model.Column, []model.ColumnValue) {
    if len(cols) == 0 {
        return nil, nil
    }
    coldefs:= make([]*model.Column, 0, len(cols))
    colvals:= make([]model.ColumnValue, 0, len(cols))
	for name, col := range cols {
		c, cv := col.ToRowChangeColumn(name)
        coldefs = append(coldefs, c)
        colvals = append(colvals, cv)
	}
    model.SortColumnsByName(coldefs, colvals, true)
	return coldefs, colvals
}

func ddlEventToMsg(e *model.DDLEvent) (*internal.MessageKey, *messageDDL) {
	key := &internal.MessageKey{
		Ts:     e.CommitTs,
		Schema: e.TableInfo.TableName.Schema,
		Table:  e.TableInfo.TableName.Table,
		Type:   model.MessageTypeDDL,
	}
	value := &messageDDL{
		Query: e.Query,
		Type:  e.Type,
	}
	return key, value
}

func msgToDDLEvent(key *internal.MessageKey, value *messageDDL) *model.DDLEvent {
	e := new(model.DDLEvent)
	e.TableInfo = new(model.TableInfo)
	// TODO: we lost the startTs from kafka message
	// startTs-based txn filter is out of work
	e.CommitTs = key.Ts
	e.TableInfo.TableName = model.TableName{
		Schema: key.Schema,
		Table:  key.Table,
	}
	e.Type = value.Type
	e.Query = value.Query
	return e
}
