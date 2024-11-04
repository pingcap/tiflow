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
	"context"
	"database/sql"
	"encoding/binary"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/internal"
	"go.uber.org/zap"
)

// BatchDecoder decodes the byte of a batch into the original messages.
type BatchDecoder struct {
	keyBytes   []byte
	valueBytes []byte

	nextKey   *internal.MessageKey
	nextEvent *model.RowChangedEvent

	upstreamTiDB *sql.DB
}

// NewBatchDecoder creates a new BatchDecoder.
func NewBatchDecoder(
	_ context.Context, config *common.Config, db *sql.DB) (codec.RowEventDecoder, error) {
	if config.LargeMessageHandle.HandleKeyOnly() && db == nil {
		return nil, cerror.ErrCodecDecode.
			GenWithStack("handle-key-only is enabled, but upstream TiDB is not provided")
	}

	return &BatchDecoder{
		upstreamTiDB: db,
	}, nil
}

// AddKeyValue implements the EventBatchDecoder interface
func (b *BatchDecoder) AddKeyValue(key, value []byte) error {
	if len(b.keyBytes) != 0 || len(b.valueBytes) != 0 {
		return cerror.ErrOpenProtocolCodecInvalidData.
			GenWithStack("decoder key and value not nil")
	}
	version := binary.BigEndian.Uint64(key[:8])
	key = key[8:]
	if version != codec.BatchVersion1 {
		return cerror.ErrOpenProtocolCodecInvalidData.
			GenWithStack("unexpected key format version")
	}

	b.keyBytes = key
	b.valueBytes = value

	return nil
}

func (b *BatchDecoder) hasNext() bool {
	keyLen := len(b.keyBytes)
	valueLen := len(b.valueBytes)

	if keyLen > 0 && valueLen > 0 {
		return true
	}

	if keyLen == 0 && valueLen != 0 || keyLen != 0 && valueLen == 0 {
		log.Panic("open-protocol meet invalid data",
			zap.Int("keyLen", keyLen), zap.Int("valueLen", valueLen))
	}

	return false
}

func (b *BatchDecoder) decodeNextKey() error {
	keyLen := binary.BigEndian.Uint64(b.keyBytes[:8])
	key := b.keyBytes[8 : keyLen+8]
	msgKey := new(internal.MessageKey)
	err := msgKey.Decode(key)
	if err != nil {
		return errors.Trace(err)
	}
	b.nextKey = msgKey
	b.keyBytes = b.keyBytes[keyLen+8:]
	return nil
}

// HasNext implements the RowEventDecoder interface
func (b *BatchDecoder) HasNext() (model.MessageType, bool, error) {
	if !b.hasNext() {
		return 0, false, nil
	}
	if err := b.decodeNextKey(); err != nil {
		return 0, false, err
	}

	if b.nextKey.Type == model.MessageTypeRow {
		valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
		value := b.valueBytes[8 : valueLen+8]
		b.valueBytes = b.valueBytes[valueLen+8:]

		rowMsg := new(messageRow)
		if err := rowMsg.decode(value); err != nil {
			return b.nextKey.Type, false, errors.Trace(err)
		}
		b.nextEvent = msgToRowChange(b.nextKey, rowMsg)
	}

	return b.nextKey.Type, true, nil
}

// NextResolvedEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextResolvedEvent() (uint64, error) {
	if b.nextKey.Type != model.MessageTypeResolved {
		return 0, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found resolved event message")
	}
	resolvedTs := b.nextKey.Ts
	b.nextKey = nil
	// resolved ts event's value part is empty, can be ignored.
	b.valueBytes = nil
	return resolvedTs, nil
}

// NextDDLEvent implements the RowEventDecoder interface
func (b *BatchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.nextKey.Type != model.MessageTypeDDL {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found ddl event message")
	}
	valueLen := binary.BigEndian.Uint64(b.valueBytes[:8])
	value := b.valueBytes[8 : valueLen+8]
	ddlMsg := new(messageDDL)
	if err := ddlMsg.decode(value); err != nil {
		return nil, errors.Trace(err)
	}
	ddlEvent := msgToDDLEvent(b.nextKey, ddlMsg)
	b.nextKey = nil
	b.valueBytes = nil
	return ddlEvent, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
func (b *BatchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.nextKey.Type != model.MessageTypeRow {
		return nil, cerror.ErrOpenProtocolCodecInvalidData.GenWithStack("not found row event message")
	}

	event := b.nextEvent
	ctx := context.Background()
	if b.nextKey.OnlyHandleKey {
		var err error
		event, err = b.assembleHandleKeyOnlyEvent(ctx, event)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	b.nextKey = nil
	return event, nil
}

func (b *BatchDecoder) buildColumns(
	holder *common.ColumnsHolder, handleKeyColumns map[string]interface{},
) []*model.Column {
	columnsCount := holder.Length()
	columns := make([]*model.Column, 0, columnsCount)
	for i := 0; i < columnsCount; i++ {
		columnType := holder.Types[i]
		name := columnType.Name()
		mysqlType := types.StrToType(strings.ToLower(columnType.DatabaseTypeName()))

		var value interface{}
		value = holder.Values[i].([]uint8)

		switch mysqlType {
		case mysql.TypeJSON:
			value = string(value.([]uint8))
		case mysql.TypeBit:
			value, _ = common.BinaryLiteralToInt(value.([]uint8))
		}

		column := &model.Column{
			Name:  name,
			Type:  mysqlType,
			Value: value,
		}

		if _, ok := handleKeyColumns[name]; ok {
			column.Flag = model.PrimaryKeyFlag | model.HandleKeyFlag
		}
		columns = append(columns, column)
	}
	return columns
}

func (b *BatchDecoder) assembleHandleKeyOnlyEvent(
	ctx context.Context, handleKeyOnlyEvent *model.RowChangedEvent,
) (*model.RowChangedEvent, error) {
	var (
		schema   = handleKeyOnlyEvent.Table.Schema
		table    = handleKeyOnlyEvent.Table.Table
		commitTs = handleKeyOnlyEvent.CommitTs
	)

	if handleKeyOnlyEvent.IsInsert() {
		conditions := make(map[string]interface{}, len(handleKeyOnlyEvent.Columns))
		for _, col := range handleKeyOnlyEvent.Columns {
			conditions[col.Name] = col.Value
		}
		holder, err := common.SnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		if err != nil {
			return nil, err
		}
		columns := b.buildColumns(holder, conditions)
		handleKeyOnlyEvent.Columns = columns
	} else if handleKeyOnlyEvent.IsDelete() {
		conditions := make(map[string]interface{}, len(handleKeyOnlyEvent.PreColumns))
		for _, col := range handleKeyOnlyEvent.PreColumns {
			conditions[col.Name] = col.Value
		}
		holder, err := common.SnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		if err != nil {
			return nil, err
		}
		preColumns := b.buildColumns(holder, conditions)
		handleKeyOnlyEvent.PreColumns = preColumns
	} else if handleKeyOnlyEvent.IsUpdate() {
		conditions := make(map[string]interface{}, len(handleKeyOnlyEvent.Columns))
		for _, col := range handleKeyOnlyEvent.Columns {
			conditions[col.Name] = col.Value
		}
		holder, err := common.SnapshotQuery(ctx, b.upstreamTiDB, commitTs, schema, table, conditions)
		if err != nil {
			return nil, err
		}
		columns := b.buildColumns(holder, conditions)
		handleKeyOnlyEvent.Columns = columns

		conditions = make(map[string]interface{}, len(handleKeyOnlyEvent.PreColumns))
		for _, col := range handleKeyOnlyEvent.PreColumns {
			conditions[col.Name] = col.Value
		}
		holder, err = common.SnapshotQuery(ctx, b.upstreamTiDB, commitTs-1, schema, table, conditions)
		if err != nil {
			return nil, err
		}
		preColumns := b.buildColumns(holder, conditions)
		handleKeyOnlyEvent.PreColumns = preColumns
	}

	return handleKeyOnlyEvent, nil
}
