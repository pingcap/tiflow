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

package canal

import (
	"context"
	"time"

	"github.com/goccy/go-json"
	"github.com/mailru/easyjson/jwriter"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/codec/utils"
	"github.com/pingcap/tiflow/pkg/sink/kafka/claimcheck"
	"go.uber.org/zap"
)

func fillColumns(
	columns []*model.Column,
	onlyOutputUpdatedColumn bool,
	onlyHandleKeyColumn bool,
	newColumnMap map[string]*model.Column,
	out *jwriter.Writer,
	builder *canalEntryBuilder,
) error {
	if len(columns) == 0 {
		out.RawString("null")
		return nil
	}
	out.RawByte('[')
	out.RawByte('{')
	isFirst := true
	for _, col := range columns {
		if col != nil {
			// column equal, do not output it
			if onlyOutputUpdatedColumn && shouldIgnoreColumn(col, newColumnMap) {
				continue
			}
			if onlyHandleKeyColumn && !col.Flag.IsHandleKey() {
				continue
			}
			if isFirst {
				isFirst = false
			} else {
				out.RawByte(',')
			}
			value, err := builder.formatValue(col.Value, col.Flag.IsBinary())
			if err != nil {
				return cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
			}
			out.String(col.Name)
			out.RawByte(':')
			if col.Value == nil {
				out.RawString("null")
			} else {
				out.String(value)
			}
		}
	}
	out.RawByte('}')
	out.RawByte(']')
	return nil
}

func newJSONMessageForDML(
	builder *canalEntryBuilder,
	e *model.RowChangedEvent,
	config *common.Config,
	messageTooLarge bool,
	claimCheckFileName string,
) ([]byte, error) {
	isDelete := e.IsDelete()

	onlyHandleKey := messageTooLarge
	if isDelete && config.DeleteOnlyHandleKeyColumns {
		onlyHandleKey = true
	}

	mysqlTypeMap := make(map[string]string, len(e.Columns))
	out := &jwriter.Writer{}
	out.RawByte('{')
	{
		const prefix string = ",\"id\":"
		out.RawString(prefix[1:])
		out.Int64(0) // ignored by both Canal Adapter and Flink
	}
	{
		const prefix string = ",\"database\":"
		out.RawString(prefix)
		out.String(e.TableInfo.GetSchemaName())
	}
	{
		const prefix string = ",\"table\":"
		out.RawString(prefix)
		out.String(e.TableInfo.GetTableName())
	}
	{
		const prefix string = ",\"pkNames\":"
		out.RawString(prefix)
		pkNames := e.PrimaryKeyColumnNames()
		if pkNames == nil {
			out.RawString("null")
		} else {
			out.RawByte('[')
			for v25, v26 := range pkNames {
				if v25 > 0 {
					out.RawByte(',')
				}
				out.String(v26)
			}
			out.RawByte(']')
		}
	}
	{
		const prefix string = ",\"isDdl\":"
		out.RawString(prefix)
		out.Bool(false)
	}
	{
		const prefix string = ",\"type\":"
		out.RawString(prefix)
		out.String(eventTypeString(e))
	}
	{
		const prefix string = ",\"es\":"
		out.RawString(prefix)
		out.Int64(convertToCanalTs(e.CommitTs))
	}
	{
		const prefix string = ",\"ts\":"
		out.RawString(prefix)
		out.Int64(time.Now().UnixMilli()) // ignored by both Canal Adapter and Flink
	}
	{
		const prefix string = ",\"sql\":"
		out.RawString(prefix)
		out.String("")
	}
	{
		columns := e.PreColumns
		if !isDelete {
			columns = e.Columns
		}
		const prefix string = ",\"sqlType\":"
		out.RawString(prefix)
		emptyColumn := true
		tableInfo := e.TableInfo
		for _, col := range columns {
			if col != nil {
				colFlag := tableInfo.ForceGetColumnFlagType(col.ColumnID)
				columnInfo := tableInfo.ForceGetColumnInfo(col.ColumnID)
				colType := columnInfo.GetType()
				colName := tableInfo.ForceGetColumnName(col.ColumnID)
				if onlyHandleKey && !colFlag.IsHandleKey() {
					continue
				}
				if emptyColumn {
					out.RawByte('{')
					emptyColumn = false
				} else {
					out.RawByte(',')
				}
				javaType, err := getJavaSQLType(col.Value, colType, *colFlag)
				if err != nil {
					return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
				}
				out.String(colName)
				out.RawByte(':')
				out.Int32(int32(javaType))
				mysqlTypeMap[colName] = utils.GetMySQLType(columnInfo, config.ContentCompatible)
			}
		}
		if emptyColumn {
			out.RawString(`null`)
		} else {
			out.RawByte('}')
		}
	}
	{
		const prefix string = ",\"mysqlType\":"
		out.RawString(prefix)
		if mysqlTypeMap == nil {
			out.RawString(`null`)
		} else {
			out.RawByte('{')
			isFirst := true
			for typeKey, typeValue := range mysqlTypeMap {
				if isFirst {
					isFirst = false
				} else {
					out.RawByte(',')
				}
				out.String(typeKey)
				out.RawByte(':')
				out.String(typeValue)
			}
			out.RawByte('}')
		}
	}

	if e.IsDelete() {
		out.RawString(",\"old\":null")
		out.RawString(",\"data\":")
		if err := fillColumns(
			e.GetPreColumns(),
			false, onlyHandleKey, nil, out, builder,
		); err != nil {
			return nil, err
		}
	} else if e.IsInsert() {
		out.RawString(",\"old\":null")
		out.RawString(",\"data\":")
		if err := fillColumns(
			e.GetColumns(),
			false, onlyHandleKey, nil, out, builder,
		); err != nil {
			return nil, err
		}
	} else if e.IsUpdate() {
		var newColsMap map[string]*model.Column
		if config.OnlyOutputUpdatedColumns {
			newColsMap = make(map[string]*model.Column, len(e.Columns))
			for _, col := range e.GetColumns() {
				newColsMap[col.Name] = col
			}
		}
		out.RawString(",\"old\":")
		if err := fillColumns(
			e.GetPreColumns(),
			config.OnlyOutputUpdatedColumns, onlyHandleKey, newColsMap, out, builder,
		); err != nil {
			return nil, err
		}
		out.RawString(",\"data\":")
		if err := fillColumns(
			e.GetColumns(),
			false, onlyHandleKey, nil, out, builder,
		); err != nil {
			return nil, err
		}
	} else {
		log.Panic("unreachable event type", zap.Any("event", e))
	}

	if config.EnableTiDBExtension {
		const prefix string = ",\"_tidb\":"
		out.RawString(prefix)
		out.RawByte('{')
		out.RawString("\"commitTs\":")
		out.Uint64(e.CommitTs)

		// only send handle key may happen in 2 cases:
		// 1. delete event, and set only handle key config. no need to encode `onlyHandleKey` field
		// 2. event larger than the max message size, and enable large message handle to the `handleKeyOnly`, encode `onlyHandleKey` field
		if messageTooLarge {
			if config.LargeMessageHandle.HandleKeyOnly() {
				out.RawByte(',')
				out.RawString("\"onlyHandleKey\":true")
			}
			if config.LargeMessageHandle.EnableClaimCheck() {
				out.RawByte(',')
				out.RawString("\"claimCheckLocation\":")
				out.String(claimCheckFileName)
			}
		}
		out.RawByte('}')
	}
	out.RawByte('}')

	value, err := out.BuildBytes()
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}
	return value, nil
}

func eventTypeString(e *model.RowChangedEvent) string {
	if e.IsDelete() {
		return "DELETE"
	}
	if len(e.PreColumns) == 0 {
		return "INSERT"
	}
	return "UPDATE"
}

// JSONRowEventEncoder encodes row event in JSON format
type JSONRowEventEncoder struct {
	builder  *canalEntryBuilder
	messages []*common.Message

	claimCheck *claimcheck.ClaimCheck

	config *common.Config
}

// newJSONRowEventEncoder creates a new JSONRowEventEncoder
func newJSONRowEventEncoder(
	config *common.Config, claimCheck *claimcheck.ClaimCheck,
) codec.RowEventEncoder {
	return &JSONRowEventEncoder{
		builder:    newCanalEntryBuilder(config),
		messages:   make([]*common.Message, 0, 1),
		config:     config,
		claimCheck: claimCheck,
	}
}

func (c *JSONRowEventEncoder) newJSONMessageForDDL(e *model.DDLEvent) canalJSONMessageInterface {
	msg := &JSONMessage{
		ID:            0, // ignored by both Canal Adapter and Flink
		Schema:        e.TableInfo.TableName.Schema,
		Table:         e.TableInfo.TableName.Table,
		IsDDL:         true,
		EventType:     convertDdlEventType(e).String(),
		ExecutionTime: convertToCanalTs(e.CommitTs),
		BuildTime:     time.Now().UnixMilli(), // timestamp
		Query:         e.Query,
	}

	if !c.config.EnableTiDBExtension {
		return msg
	}

	return &canalJSONMessageWithTiDBExtension{
		JSONMessage: msg,
		Extensions:  &tidbExtension{CommitTs: e.CommitTs},
	}
}

func (c *JSONRowEventEncoder) newJSONMessage4CheckpointEvent(
	ts uint64,
) *canalJSONMessageWithTiDBExtension {
	return &canalJSONMessageWithTiDBExtension{
		JSONMessage: &JSONMessage{
			ID:            0,
			IsDDL:         false,
			EventType:     tidbWaterMarkType,
			ExecutionTime: convertToCanalTs(ts),
			BuildTime:     time.Now().UnixNano() / int64(time.Millisecond), // converts to milliseconds
		},
		Extensions: &tidbExtension{WatermarkTs: ts},
	}
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (c *JSONRowEventEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	if !c.config.EnableTiDBExtension {
		return nil, nil
	}

	msg := c.newJSONMessage4CheckpointEvent(ts)
	value, err := json.Marshal(msg)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	value, err = common.Compress(
		c.config.ChangefeedID, c.config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return common.NewResolvedMsg(config.ProtocolCanalJSON, nil, value, ts), nil
}

// AppendRowChangedEvent implements the interface EventJSONBatchEncoder
func (c *JSONRowEventEncoder) AppendRowChangedEvent(
	ctx context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	value, err := newJSONMessageForDML(c.builder, e, c.config, false, "")
	if err != nil {
		return errors.Trace(err)
	}

	value, err = common.Compress(
		c.config.ChangefeedID, c.config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return errors.Trace(err)
	}
	m := &common.Message{
		Key:      nil,
		Value:    value,
		Ts:       e.CommitTs,
		Schema:   e.TableInfo.GetSchemaNamePtr(),
		Table:    e.TableInfo.GetTableNamePtr(),
		Type:     model.MessageTypeRow,
		Protocol: config.ProtocolCanalJSON,
		Callback: callback,
	}
	m.IncRowsCount()

	originLength := m.Length()
	if m.Length() > c.config.MaxMessageBytes {
		// for single message that is longer than max-message-bytes, do not send it.
		if c.config.LargeMessageHandle.Disabled() {
			log.Error("Single message is too large for canal-json",
				zap.Int("maxMessageBytes", c.config.MaxMessageBytes),
				zap.Int("length", originLength),
				zap.Any("table", e.TableInfo.TableName))
			return cerror.ErrMessageTooLarge.GenWithStackByArgs()
		}

		if c.config.LargeMessageHandle.HandleKeyOnly() {
			value, err = newJSONMessageForDML(c.builder, e, c.config, true, "")
			if err != nil {
				return cerror.ErrMessageTooLarge.GenWithStackByArgs()
			}
			value, err = common.Compress(
				c.config.ChangefeedID, c.config.LargeMessageHandle.LargeMessageHandleCompression, value,
			)
			if err != nil {
				return errors.Trace(err)
			}

			m.Value = value
			length := m.Length()
			if length > c.config.MaxMessageBytes {
				log.Error("Single message is still too large for canal-json only encode handle-key columns",
					zap.Int("maxMessageBytes", c.config.MaxMessageBytes),
					zap.Int("originLength", originLength),
					zap.Int("length", length),
					zap.Any("table", e.TableInfo.TableName))
				return cerror.ErrMessageTooLarge.GenWithStackByArgs()
			}
			log.Warn("Single message is too large for canal-json, only encode handle-key columns",
				zap.Int("maxMessageBytes", c.config.MaxMessageBytes),
				zap.Int("originLength", originLength),
				zap.Int("length", length),
				zap.Any("table", e.TableInfo.TableName))
		}

		if c.config.LargeMessageHandle.EnableClaimCheck() {
			claimCheckFileName := claimcheck.NewFileName()
			if err := c.claimCheck.WriteMessage(ctx, m.Key, m.Value, claimCheckFileName); err != nil {
				return errors.Trace(err)
			}

			m, err = c.newClaimCheckLocationMessage(e, callback, claimCheckFileName)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	c.messages = append(c.messages, m)
	return nil
}

func (c *JSONRowEventEncoder) newClaimCheckLocationMessage(
	event *model.RowChangedEvent, callback func(), fileName string,
) (*common.Message, error) {
	claimCheckLocation := c.claimCheck.FileNameWithPrefix(fileName)
	value, err := newJSONMessageForDML(c.builder, event, c.config, true, claimCheckLocation)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}

	value, err = common.Compress(
		c.config.ChangefeedID, c.config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	result := common.NewMsg(config.ProtocolCanalJSON, nil, value, 0, model.MessageTypeRow, nil, nil)
	result.Callback = callback
	result.IncRowsCount()

	length := result.Length()
	if length > c.config.MaxMessageBytes {
		log.Warn("Single message is too large for canal-json, when create the claim check location message",
			zap.Int("maxMessageBytes", c.config.MaxMessageBytes),
			zap.Int("length", length),
			zap.Any("table", event.TableInfo.TableName))
		return nil, cerror.ErrMessageTooLarge.GenWithStackByArgs(length)
	}
	return result, nil
}

// Build implements the RowEventEncoder interface
func (c *JSONRowEventEncoder) Build() []*common.Message {
	if len(c.messages) == 0 {
		return nil
	}

	result := c.messages
	c.messages = nil
	return result
}

// EncodeDDLEvent encodes DDL events
func (c *JSONRowEventEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
	message := c.newJSONMessageForDDL(e)
	value, err := json.Marshal(message)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrCanalEncodeFailed, err)
	}
	value, err = common.Compress(
		c.config.ChangefeedID, c.config.LargeMessageHandle.LargeMessageHandleCompression, value,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return common.NewDDLMsg(config.ProtocolCanalJSON, nil, value, e), nil
}

type jsonRowEventEncoderBuilder struct {
	config *common.Config

	claimCheck *claimcheck.ClaimCheck
}

// NewJSONRowEventEncoderBuilder creates a canal-json batchEncoderBuilder.
func NewJSONRowEventEncoderBuilder(ctx context.Context, config *common.Config) (codec.RowEventEncoderBuilder, error) {
	claimCheck, err := claimcheck.New(ctx, config.LargeMessageHandle, config.ChangefeedID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &jsonRowEventEncoderBuilder{
		config:     config,
		claimCheck: claimCheck,
	}, nil
}

// Build a `jsonRowEventEncoderBuilder`
func (b *jsonRowEventEncoderBuilder) Build() codec.RowEventEncoder {
	return newJSONRowEventEncoder(b.config, b.claimCheck)
}

func shouldIgnoreColumn(col *model.Column,
	newColumnMap map[string]*model.Column,
) bool {
	newCol, ok := newColumnMap[col.Name]
	if ok && newCol != nil {
		// sql type is not equal
		if newCol.Type != col.Type {
			return false
		}
		// value equal
		if codec.IsColumnValueEqual(newCol.Value, col.Value) {
			return true
		}
	}
	return false
}

func (b *jsonRowEventEncoderBuilder) CleanMetrics() {
	if b.claimCheck != nil {
		b.claimCheck.CleanMetrics()
	}
}
