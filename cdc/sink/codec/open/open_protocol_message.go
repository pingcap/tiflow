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
	"sort"
	"strings"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/internal"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

type mqMessageRow struct {
	Update     map[string]internal.Column `json:"u,omitempty"`
	PreColumns map[string]internal.Column `json:"p,omitempty"`
	Delete     map[string]internal.Column `json:"d,omitempty"`
}

func (m *mqMessageRow) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *mqMessageRow) decode(data []byte) error {
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

type mqMessageDDL struct {
	Query string             `json:"q"`
	Type  timodel.ActionType `json:"t"`
}

func (m *mqMessageDDL) encode() ([]byte, error) {
	data, err := json.Marshal(m)
	return data, cerror.WrapError(cerror.ErrMarshalFailed, err)
}

func (m *mqMessageDDL) decode(data []byte) error {
	return cerror.WrapError(cerror.ErrUnmarshalFailed, json.Unmarshal(data, m))
}

func newResolvedMessage(ts uint64) *internal.MQMessageKey {
	return &internal.MQMessageKey{
		Ts:   ts,
		Type: model.MessageTypeResolved,
	}
}

func rowChangeToMsg(e *model.RowChangedEvent) (*internal.MQMessageKey, *mqMessageRow) {
	var partition *int64
	if e.Table.IsPartition {
		partition = &e.Table.TableID
	}
	key := &internal.MQMessageKey{
		Ts:        e.CommitTs,
		Schema:    e.Table.Schema,
		Table:     e.Table.Table,
		RowID:     e.RowID,
		Partition: partition,
		Type:      model.MessageTypeRow,
	}
	value := &mqMessageRow{}
	if e.IsDelete() {
		value.Delete = rowChangeColumns2MQColumns(e.PreColumns)
	} else {
		value.Update = rowChangeColumns2MQColumns(e.Columns)
		value.PreColumns = rowChangeColumns2MQColumns(e.PreColumns)
	}
	return key, value
}

func msgToRowChange(key *internal.MQMessageKey, value *mqMessageRow) *model.RowChangedEvent {
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
		e.PreColumns = mqColumns2RowChangeColumns(value.Delete)
	} else {
		e.Columns = mqColumns2RowChangeColumns(value.Update)
		e.PreColumns = mqColumns2RowChangeColumns(value.PreColumns)
	}
	return e
}

func rowChangeColumns2MQColumns(cols []*model.Column) map[string]internal.Column {
	jsonCols := make(map[string]internal.Column, len(cols))
	for _, col := range cols {
		if col == nil {
			continue
		}
		c := internal.Column{}
		c.FromRowChangeColumn(col)
		jsonCols[col.Name] = c
	}
	if len(jsonCols) == 0 {
		return nil
	}
	return jsonCols
}

func mqColumns2RowChangeColumns(cols map[string]internal.Column) []*model.Column {
	sinkCols := make([]*model.Column, 0, len(cols))
	for name, col := range cols {
		c := col.ToRowChangeColumn(name)
		sinkCols = append(sinkCols, c)
	}
	if len(sinkCols) == 0 {
		return nil
	}
	sort.Slice(sinkCols, func(i, j int) bool {
		return strings.Compare(sinkCols[i].Name, sinkCols[j].Name) > 0
	})
	return sinkCols
}

func ddlEventToMsg(e *model.DDLEvent) (*internal.MQMessageKey, *mqMessageDDL) {
	key := &internal.MQMessageKey{
		Ts:     e.CommitTs,
		Schema: e.TableInfo.Schema,
		Table:  e.TableInfo.Table,
		Type:   model.MessageTypeDDL,
	}
	value := &mqMessageDDL{
		Query: e.Query,
		Type:  e.Type,
	}
	return key, value
}

func msgToDDLEvent(key *internal.MQMessageKey, value *mqMessageDDL) *model.DDLEvent {
	e := new(model.DDLEvent)
	e.TableInfo = new(model.SimpleTableInfo)
	// TODO: we lost the startTs from kafka message
	// startTs-based txn filter is out of work
	e.CommitTs = key.Ts
	e.TableInfo.Table = key.Table
	e.TableInfo.Schema = key.Schema
	e.Type = value.Type
	e.Query = value.Query
	return e
}
