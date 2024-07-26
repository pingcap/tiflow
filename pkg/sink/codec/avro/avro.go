// Copyright 2020 PingCAP, Inc.
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

package avro

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// BatchEncoder converts the events to binary Avro data
type BatchEncoder struct {
	result []*common.Message
	config *common.Config
	codec  codec.Format
}

// AppendRowChangedEvent appends a row change event to the encoder
// NOTE: the encoder can only store one RowChangedEvent!
func (a *BatchEncoder) AppendRowChangedEvent(
	ctx context.Context,
	topic string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	topic = sanitizeTopic(topic)

	key, err := a.codec.EncodeKey(ctx, topic, e)
	if err != nil {
		log.Error("avro encoding key failed", zap.Error(err), zap.Any("event", e))
		return errors.Trace(err)
	}

	value, err := a.codec.EncodeValue(ctx, topic, e)
	if err != nil {
		log.Error("avro encoding value failed", zap.Error(err), zap.Any("event", e))
		return errors.Trace(err)
	}

	message := common.NewMsg(
		config.ProtocolAvro,
		key,
		value,
		e.CommitTs,
		model.MessageTypeRow,
		e.TableInfo.GetSchemaNamePtr(),
		e.TableInfo.GetTableNamePtr(),
	)
	message.Callback = callback
	message.IncRowsCount()

	if message.Length() > a.config.MaxMessageBytes {
		log.Warn("Single message is too large for avro",
			zap.Int("maxMessageBytes", a.config.MaxMessageBytes),
			zap.Int("length", message.Length()),
			zap.Any("table", e.TableInfo.TableName))
		return cerror.ErrMessageTooLarge.GenWithStackByArgs(message.Length())
	}

	a.result = append(a.result, message)
	return nil
}

// EncodeCheckpointEvent only encode checkpoint event if the watermark event is enabled
// it's only used for the testing purpose.
func (a *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	if a.config.EnableTiDBExtension && a.config.AvroEnableWatermark {
		buf := new(bytes.Buffer)
		data := []interface{}{checkpointByte, ts}
		for _, v := range data {
			err := binary.Write(buf, binary.BigEndian, v)
			if err != nil {
				return nil, cerror.WrapError(cerror.ErrAvroToEnvelopeError, err)
			}
		}

		value := buf.Bytes()
		return common.NewResolvedMsg(config.ProtocolAvro, nil, value, ts), nil
	}
	return nil, nil
}

type ddlEvent struct {
	Query    string             `json:"query"`
	Type     timodel.ActionType `json:"type"`
	Schema   string             `json:"schema"`
	Table    string             `json:"table"`
	CommitTs uint64             `json:"commitTs"`
}

// EncodeDDLEvent only encode DDL event if the watermark event is enabled
// it's only used for the testing purpose.
func (a *BatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
	if a.config.EnableTiDBExtension && a.config.AvroEnableWatermark {
		buf := new(bytes.Buffer)
		_ = binary.Write(buf, binary.BigEndian, ddlByte)

		event := &ddlEvent{
			Query:    e.Query,
			Type:     e.Type,
			Schema:   e.TableInfo.TableName.Schema,
			Table:    e.TableInfo.TableName.Table,
			CommitTs: e.CommitTs,
		}
		data, err := json.Marshal(event)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrAvroToEnvelopeError, err)
		}
		buf.Write(data)

		value := buf.Bytes()
		return common.NewDDLMsg(config.ProtocolAvro, nil, value, e), nil
	}

	return nil, nil
}

// Build Messages
func (a *BatchEncoder) Build() (messages []*common.Message) {
	result := a.result
	a.result = nil
	return result
}

const (
	// avro does not send ddl and checkpoint message, the following 2 field is used to distinguish
	// TiCDC DDL event and checkpoint event, only used for testing purpose, not for production
	ddlByte        = uint8(1)
	checkpointByte = uint8(2)
)

type batchEncoderBuilder struct {
	namespace    string
	changeFeedID string
	config       *common.Config
	schemaM      SchemaManager
}

// NewBatchEncoderBuilder creates an avro batchEncoderBuilder.
func NewBatchEncoderBuilder(
	ctx context.Context, config *common.Config,
) (codec.RowEventEncoderBuilder, error) {
	schemaM, err := NewSchemaManager(ctx, config)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &batchEncoderBuilder{
		namespace:    config.ChangefeedID.Namespace,
		changeFeedID: config.ChangefeedID.ID,
		config:       config,
		schemaM:      schemaM,
	}, nil
}

// Build an AvroEventBatchEncoder.
func (b *batchEncoderBuilder) Build() codec.RowEventEncoder {
	return NewAvroEncoder(b.namespace, b.changeFeedID, b.schemaM, b.config)
}

// CleanMetrics is a no-op for AvroEventBatchEncoder.
func (b *batchEncoderBuilder) CleanMetrics() {}

// NewAvroEncoder return a avro encoder.
func NewAvroEncoder(namespace string, changeFeedID string, schemaM SchemaManager, config *common.Config) codec.RowEventEncoder {
	return &BatchEncoder{
		result: make([]*common.Message, 0, 1),
		config: config,
		codec:  NewAvroCodec(namespace, changeFeedID, schemaM, config),
	}
}

// SetupEncoderAndSchemaRegistry4Testing start a local schema registry for testing.
func SetupEncoderAndSchemaRegistry4Testing(
	ctx context.Context,
	config *common.Config,
) (*BatchEncoder, error) {
	StartHTTPInterceptForTestingRegistry()
	schemaM, err := NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &BatchEncoder{
		result: make([]*common.Message, 0, 1),
		config: config,
		codec:  NewAvroCodec(model.DefaultNamespace, "default", schemaM, config),
	}, nil
}

// TeardownEncoderAndSchemaRegistry4Testing stop the local schema registry for testing.
func TeardownEncoderAndSchemaRegistry4Testing() {
	StopHTTPInterceptForTestingRegistry()
}
