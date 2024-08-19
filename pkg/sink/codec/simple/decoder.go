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
	"container/list"
	"context"
	"database/sql"
	"path/filepath"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// Decoder implement the RowEventDecoder interface
type Decoder struct {
	config *common.Config

	marshaller marshaller

	upstreamTiDB *sql.DB
	storage      storage.ExternalStorage

	value []byte
	msg   *message
	memo  TableInfoProvider

	// cachedMessages is used to store the messages which does not have received corresponding table info yet.
	cachedMessages *list.List
	// CachedRowChangedEvents are events just decoded from the cachedMessages
	CachedRowChangedEvents []*model.RowChangedEvent
}

// NewDecoder returns a new Decoder
func NewDecoder(ctx context.Context, config *common.Config, db *sql.DB) (*Decoder, error) {
	var (
		externalStorage storage.ExternalStorage
		err             error
	)
	if config.LargeMessageHandle.EnableClaimCheck() {
		storageURI := config.LargeMessageHandle.ClaimCheckStorageURI
		externalStorage, err = util.GetExternalStorage(ctx, storageURI, nil, util.NewS3Retryer(10, 10*time.Second, 10*time.Second))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
	}

	if config.LargeMessageHandle.HandleKeyOnly() && db == nil {
		return nil, cerror.ErrCodecDecode.
			GenWithStack("handle-key-only is enabled, but upstream TiDB is not provided")
	}

	m, err := newMarshaller(config)
	return &Decoder{
		config:     config,
		marshaller: m,

		storage:      externalStorage,
		upstreamTiDB: db,

		memo:           newMemoryTableInfoProvider(),
		cachedMessages: list.New(),
	}, errors.Trace(err)
}

// AddKeyValue add the received key and values to the Decoder,
func (d *Decoder) AddKeyValue(_, value []byte) (err error) {
	if d.value != nil {
		return cerror.ErrCodecDecode.GenWithStack(
			"Decoder value already exists, not consumed yet")
	}
	d.value, err = common.Decompress(d.config.LargeMessageHandle.LargeMessageHandleCompression, value)
	return err
}

// HasNext returns whether there is any event need to be consumed
func (d *Decoder) HasNext() (model.MessageType, bool, error) {
	if d.value == nil {
		return model.MessageTypeUnknown, false, nil
	}

	m := new(message)
	err := d.marshaller.Unmarshal(d.value, m)
	if err != nil {
		return model.MessageTypeUnknown, false, cerror.WrapError(cerror.ErrDecodeFailed, err)
	}
	d.msg = m
	d.value = nil

	if d.msg.Data != nil || d.msg.Old != nil {
		return model.MessageTypeRow, true, nil
	}

	if m.Type == MessageTypeWatermark {
		return model.MessageTypeResolved, true, nil
	}

	return model.MessageTypeDDL, true, nil
}

// NextResolvedEvent returns the next resolved event if exists
func (d *Decoder) NextResolvedEvent() (uint64, error) {
	if d.msg.Type != MessageTypeWatermark {
		return 0, cerror.ErrCodecDecode.GenWithStack(
			"not found resolved event message")
	}

	ts := d.msg.CommitTs
	d.msg = nil

	return ts, nil
}

// NextRowChangedEvent returns the next row changed event if exists
func (d *Decoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
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

	tableInfo := d.memo.Read(d.msg.Schema, d.msg.Table, d.msg.SchemaVersion)
	if tableInfo == nil {
		log.Debug("table info not found for the event, "+
			"the consumer should cache this event temporarily, and update the tableInfo after it's received",
			zap.String("schema", d.msg.Schema),
			zap.String("table", d.msg.Table),
			zap.Uint64("version", d.msg.SchemaVersion))
		d.cachedMessages.PushBack(d.msg)
		d.msg = nil
		return nil, nil
	}

	event, err := buildRowChangedEvent(d.msg, tableInfo, d.config.EnableRowChecksum, d.upstreamTiDB)
	d.msg = nil

	log.Debug("row changed event assembled", zap.Any("event", event))
	return event, err
}

func (d *Decoder) assembleClaimCheckRowChangedEvent(claimCheckLocation string) (*model.RowChangedEvent, error) {
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

	m := new(message)
	err = d.marshaller.Unmarshal(value, m)
	if err != nil {
		return nil, err
	}
	d.msg = m
	return d.NextRowChangedEvent()
}

func (d *Decoder) assembleHandleKeyOnlyRowChangedEvent(m *message) (*model.RowChangedEvent, error) {
	tableInfo := d.memo.Read(m.Schema, m.Table, m.SchemaVersion)
	if tableInfo == nil {
		log.Debug("table info not found for the event, "+
			"the consumer should cache this event temporarily, and update the tableInfo after it's received",
			zap.String("schema", d.msg.Schema),
			zap.String("table", d.msg.Table),
			zap.Uint64("version", d.msg.SchemaVersion))
		d.cachedMessages.PushBack(d.msg)
		d.msg = nil
		return nil, nil
	}

	fieldTypeMap := make(map[string]*types.FieldType, len(tableInfo.Columns))
	for _, col := range tableInfo.Columns {
		fieldTypeMap[col.Name.O] = &col.FieldType
	}

	result := &message{
		Version:       defaultVersion,
		Schema:        m.Schema,
		Table:         m.Table,
		TableID:       m.TableID,
		Type:          m.Type,
		CommitTs:      m.CommitTs,
		SchemaVersion: m.SchemaVersion,
	}

	ctx := context.Background()
	timezone := common.MustQueryTimezone(ctx, d.upstreamTiDB)
	switch m.Type {
	case DMLTypeInsert:
		holder := common.MustSnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs, m.Schema, m.Table, m.Data)
		result.Data = d.buildData(holder, fieldTypeMap, timezone)
	case DMLTypeUpdate:
		holder := common.MustSnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs, m.Schema, m.Table, m.Data)
		result.Data = d.buildData(holder, fieldTypeMap, timezone)

		holder = common.MustSnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs-1, m.Schema, m.Table, m.Old)
		result.Old = d.buildData(holder, fieldTypeMap, timezone)
	case DMLTypeDelete:
		holder := common.MustSnapshotQuery(ctx, d.upstreamTiDB, m.CommitTs-1, m.Schema, m.Table, m.Old)
		result.Old = d.buildData(holder, fieldTypeMap, timezone)
	}

	d.msg = result
	return d.NextRowChangedEvent()
}

func (d *Decoder) buildData(
	holder *common.ColumnsHolder, fieldTypeMap map[string]*types.FieldType, timezone string,
) map[string]interface{} {
	columnsCount := holder.Length()
	result := make(map[string]interface{}, columnsCount)

	for i := 0; i < columnsCount; i++ {
		col := holder.Types[i]
		value := holder.Values[i]

		fieldType := fieldTypeMap[col.Name()]
		result[col.Name()] = encodeValue(value, fieldType, timezone)
	}
	return result
}

// NextDDLEvent returns the next DDL event if exists
func (d *Decoder) NextDDLEvent() (*model.DDLEvent, error) {
	if d.msg == nil {
		return nil, cerror.ErrCodecDecode.GenWithStack(
			"no message found when decode DDL event")
	}
	ddl := newDDLEvent(d.msg)
	d.msg = nil

	d.memo.Write(ddl.TableInfo)
	d.memo.Write(ddl.PreTableInfo)

	for ele := d.cachedMessages.Front(); ele != nil; {
		d.msg = ele.Value.(*message)
		event, err := d.NextRowChangedEvent()
		if err != nil {
			return nil, err
		}
		d.CachedRowChangedEvents = append(d.CachedRowChangedEvents, event)

		next := ele.Next()
		d.cachedMessages.Remove(ele)
		ele = next
	}
	return ddl, nil
}

// GetCachedEvents returns the cached events
func (d *Decoder) GetCachedEvents() []*model.RowChangedEvent {
	result := d.CachedRowChangedEvents
	d.CachedRowChangedEvents = nil
	return result
}

// TableInfoProvider is used to store and read table info
// It works like a schema cache when consuming simple protocol messages
// It will store multiple versions of table info for a table
// The table info which has the exact (schema, table, version) will be returned when reading
type TableInfoProvider interface {
	Write(info *model.TableInfo)
	Read(schema, table string, version uint64) *model.TableInfo
}

type memoryTableInfoProvider struct {
	memo map[tableSchemaKey]*model.TableInfo
}

func newMemoryTableInfoProvider() *memoryTableInfoProvider {
	return &memoryTableInfoProvider{
		memo: make(map[tableSchemaKey]*model.TableInfo),
	}
}

func (m *memoryTableInfoProvider) Write(info *model.TableInfo) {
	if info == nil || info.TableName.Schema == "" || info.TableName.Table == "" {
		return
	}
	key := tableSchemaKey{
		schema:  info.TableName.Schema,
		table:   info.TableName.Table,
		version: info.UpdateTS,
	}

	_, ok := m.memo[key]
	if ok {
		log.Debug("table info not stored, since it already exists",
			zap.String("schema", info.TableName.Schema),
			zap.String("table", info.TableName.Table),
			zap.Uint64("version", info.UpdateTS))
		return
	}

	m.memo[key] = info
	log.Info("table info stored",
		zap.String("schema", info.TableName.Schema),
		zap.String("table", info.TableName.Table),
		zap.Uint64("version", info.UpdateTS))
}

// Read returns the table info with the exact (schema, table, version)
// Note: It's a blocking call, it will wait until the table info is stored
func (m *memoryTableInfoProvider) Read(schema, table string, version uint64) *model.TableInfo {
	key := tableSchemaKey{
		schema:  schema,
		table:   table,
		version: version,
	}
	return m.memo[key]
}

type tableSchemaKey struct {
	schema  string
	table   string
	version uint64
}
