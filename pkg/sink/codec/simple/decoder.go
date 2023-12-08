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
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

type decoder struct {
	config *common.Config

	upstreamTiDB *sql.DB
	storage      storage.ExternalStorage

	value []byte
	msg   *message
	memo  TableInfoProvider
}

// NewDecoder returns a new decoder
func NewDecoder(ctx context.Context, config *common.Config, db *sql.DB) (*decoder, error) {
	var (
		externalStorage storage.ExternalStorage
		err             error
	)
	if config.LargeMessageHandle.EnableClaimCheck() {
		storageURI := config.LargeMessageHandle.ClaimCheckStorageURI
		externalStorage, err = util.GetExternalStorageFromURI(ctx, storageURI)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
	}

	if config.LargeMessageHandle.HandleKeyOnly() && db == nil {
		return nil, cerror.ErrCodecDecode.
			GenWithStack("handle-key-only is enabled, but upstream TiDB is not provided")
	}

	return &decoder{
		config:  config,
		storage: externalStorage,
		memo:    newMemoryTableInfoProvider(),
	}, nil
}

// AddKeyValue add the received key and values to the decoder,
func (d *decoder) AddKeyValue(_, value []byte) error {
	if d.value != nil {
		return cerror.ErrDecodeFailed.GenWithStack(
			"decoder value already exists, not consumed yet")
	}
	value, err := common.Decompress(d.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	if err != nil {
		return err
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

	if d.msg.ClaimCheckLocation != "" {
		return d.assembleClaimCheckRowChangedEvent(d.msg.ClaimCheckLocation)
	}

	if d.msg.HandleKeyOnly {
		return d.assembleHandleKeyOnlyRowChangedEvent(d.msg)
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

func (d *decoder) assembleClaimCheckRowChangedEvent(claimCheckLocation string) (*model.RowChangedEvent, error) {
	_, claimCheckFileName := filepath.Split(claimCheckLocation)
	data, err := d.storage.ReadFile(context.Background(), claimCheckFileName)
	if err != nil {
		return nil, err
	}
	claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
	if err != nil {
		return nil, err
	}

	value, err := common.Decompress(d.config.LargeMessageHandle.LargeMessageHandleCompression, claimCheckM.Value)
	if err != nil {
		return nil, err
	}

	var m message
	err = json.Unmarshal(value, &m)
	if err != nil {
		return nil, err
	}

	d.msg = &m
	return d.NextRowChangedEvent()
}

func (d *decoder) assembleHandleKeyOnlyRowChangedEvent(m *message) (*model.RowChangedEvent, error) {
	tableInfo := d.memo.Read(m.Database, m.Table, m.SchemaVersion)
	if tableInfo == nil {
		return nil, cerror.ErrCodecDecode.GenWithStack(
			"cannot found the table info, schema: %s, table: %s, version: %d",
			m.Database, m.Table, m.SchemaVersion)
	}

	fieldTypeMap := make(map[string]*types.FieldType, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		fieldTypeMap[col.Name.L] = &col.FieldType
	}

	result := &message{
		Version:       defaultVersion,
		Database:      m.Database,
		Table:         m.Table,
		Type:          m.Type,
		CommitTs:      m.CommitTs,
		SchemaVersion: m.SchemaVersion,
	}

	ctx := context.Background()
	switch m.Type {
	case InsertType:
		holder, err := common.SnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs, m.Database, m.Table, m.Data)
		if err != nil {
			return nil, err
		}
		data, err := d.buildData(holder, fieldTypeMap)
		if err != nil {
			return nil, err
		}
		result.Data = data
	case UpdateType:
		holder, err := common.SnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs, m.Database, m.Table, m.Data)
		if err != nil {
			return nil, err
		}
		data, err := d.buildData(holder, fieldTypeMap)
		if err != nil {
			return nil, err
		}
		result.Data = data

		holder, err = common.SnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs-1, m.Database, m.Table, m.Old)
		if err != nil {
			return nil, err
		}
		old, err := d.buildData(holder, fieldTypeMap)
		if err != nil {
			return nil, err
		}
		result.Old = old
	case DeleteType:
		holder, err := common.SnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs-1, m.Database, m.Table, m.Old)
		if err != nil {
			return nil, err
		}
		data, err := d.buildData(holder, fieldTypeMap)
		if err != nil {
			return nil, err
		}
		result.Old = data
	}

	d.msg = result
	return d.NextRowChangedEvent()
}

func (d *decoder) buildData(
	holder *common.ColumnsHolder, fieldTypeMap map[string]*types.FieldType,
) (map[string]interface{}, error) {
	columnsCount := holder.Length()
	result := make(map[string]interface{}, columnsCount)

	for i := 0; i < columnsCount; i++ {
		col := holder.Types[i]
		value := holder.Values[i]

		fieldType, ok := fieldTypeMap[col.Name()]
		if !ok {
			return nil, cerror.ErrCodecDecode.GenWithStack(
				"cannot found the field type, schema: %s, table: %s, column: %s",
				d.msg.Database, d.msg.Table, col.Name())
		}
		value, err := encodeValue(value, fieldType)
		if err != nil {
			return nil, err
		}
		result[col.Name()] = value
	}
	return result, nil
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
