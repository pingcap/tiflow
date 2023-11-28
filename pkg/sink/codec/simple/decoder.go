// Copyright 2023 PingCAP, Inc.
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

package simple

import (
	"encoding/json"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type decoder struct {
	value []byte

	msg *message

	memo TableInfoProvider
}

// NewDecoder returns a new decoder
func NewDecoder() *decoder {
	return &decoder{
		memo: newMemoryTableInfoProvider(),
	}
}

// AddKeyValue add the received key and values to the decoder,
func (d *decoder) AddKeyValue(_, value []byte) error {
	if d.value != nil {
		return cerror.ErrDecodeFailed.GenWithStack(
			"decoder value already exists, not consumed yet")
	}
	d.value = value
	return nil
}

// HasNext returns whether there is any event need to be consumed
func (d *decoder) HasNext() (model.MessageType, bool, error) {
	if d.value == nil {
		return model.MessageTypeUnknown, false, nil
	}

	var m message
	if err := json.Unmarshal(d.value, &m); err != nil {
		return model.MessageTypeUnknown, false, cerror.WrapError(cerror.ErrDecodeFailed, err)
	}
	d.msg = &m
	d.value = nil

	switch d.msg.Type {
	case WatermarkType:
		return model.MessageTypeResolved, true, nil
	case DDLType, BootstrapType:
		return model.MessageTypeDDL, true, nil
	default:
	}

	if d.msg.Data != nil || d.msg.Old != nil {
		return model.MessageTypeRow, true, nil
	}
	return model.MessageTypeUnknown, false, nil
}

// NextResolvedEvent returns the next resolved event if exists
func (d *decoder) NextResolvedEvent() (uint64, error) {
	if d.msg.Type != WatermarkType {
		return 0, cerror.ErrCodecDecode.GenWithStack(
			"not found resolved event message")
	}

	ts := d.msg.CommitTs
	d.msg = nil

	return ts, nil
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if d.msg == nil || (d.msg.Data == nil && d.msg.Old == nil) {
		return nil, cerror.ErrCodecDecode.GenWithStack(
			"invalid row changed event message")
	}

	tableInfo := d.memo.Read(d.msg.Database, d.msg.Table, d.msg.SchemaVersion)
	if tableInfo == nil {
		return nil, cerror.ErrCodecDecode.GenWithStack(
			"cannot found the table info, schema: %s, table: %s, version: %d",
			d.msg.Database, d.msg.Table, d.msg.SchemaVersion)
	}

	event, err := buildRowChangedEvent(d.msg, tableInfo)
	if err != nil {
		return nil, err
	}

	d.msg = nil
	return event, nil
}

// NextDDLEvent returns the next DDL event if exists
func (d *decoder) NextDDLEvent() (*model.DDLEvent, error) {
	if d.msg.Type != DDLType && d.msg.Type != BootstrapType {
		return nil, cerror.ErrCodecDecode.GenWithStack(
			"not found ddl event message")
	}

	ddl := newDDLEvent(d.msg)
	d.msg = nil

	d.memo.Write(ddl.TableInfo)

	return ddl, nil
}

// TableInfoProvider is used to store and read table info
type TableInfoProvider interface {
	Write(info *model.TableInfo)
	Read(schema, table string, version uint64) *model.TableInfo
}

type memoryTableInfoProvider struct {
	memo map[cacheKey]*model.TableInfo
}

func newMemoryTableInfoProvider() *memoryTableInfoProvider {
	return &memoryTableInfoProvider{
		memo: make(map[cacheKey]*model.TableInfo),
	}
}

func (m *memoryTableInfoProvider) Write(info *model.TableInfo) {
	key := cacheKey{
		schema: info.TableName.Schema,
		table:  info.TableName.Table,
	}

	entry, ok := m.memo[key]
	if ok && entry.UpdateTS >= info.UpdateTS {
		log.Warn("table info not stored, since the updateTs is stale",
			zap.String("schema", info.TableName.Schema),
			zap.String("table", info.TableName.Table),
			zap.Uint64("oldUpdateTs", entry.UpdateTS),
			zap.Uint64("updateTs", info.UpdateTS))
		return
	}
	m.memo[key] = info
	log.Info("table info stored",
		zap.String("schema", info.TableName.Schema),
		zap.String("table", info.TableName.Table),
		zap.Uint64("updateTs", info.UpdateTS))
}

func (m *memoryTableInfoProvider) Read(schema, table string, version uint64) *model.TableInfo {
	key := cacheKey{
		schema: schema,
		table:  table,
	}

	entry, ok := m.memo[key]
	if ok && entry.UpdateTS == version {
		return entry
	}
	return nil
}

type cacheKey struct {
	schema string
	table  string
}
