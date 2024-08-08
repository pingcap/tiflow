// Copyright 2024 PingCAP, Inc.
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

package debezium

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/avro"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// BatchEncoder encodes message into Debezium format.
type BatchEncoder struct {
	messages []*common.Message

	config *common.Config

	codec codec.Format
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeCheckpointEvent(_ uint64) (*common.Message, error) {
	// Currently ignored. Debezium MySQL Connector does not emit such event.
	return nil, nil
}

// AppendRowChangedEvent implements the RowEventEncoder interface
func (d *BatchEncoder) AppendRowChangedEvent(
	ctx context.Context,
	topic string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	op := avro.GetOperation(e)
	if op == "" {
		log.Warn("skip unknown operation", zap.Any("rowChangedEvent", e))
		return nil
	}

	key, err := d.codec.EncodeKey(ctx, topic, e)
	if err != nil {
		log.Error("debezium encoding key failed",
			zap.String("format", string(d.config.EncodingFormat)),
			zap.Error(err),
			zap.Any("event", e))
		return errors.Trace(err)
	}

	value, err := d.codec.EncodeValue(ctx, topic, e)
	if err != nil {
		log.Error("debezium encoding value failed",
			zap.String("format", string(d.config.EncodingFormat)),
			zap.Error(err),
			zap.Any("event", e))
		return errors.Trace(err)
	}

	if d.config.IsDebeziumJsonFormat() {
		// TODO: Use a streaming compression is better.
		value, err = common.Compress(
			d.config.ChangefeedID,
			d.config.LargeMessageHandle.LargeMessageHandleCompression,
			value,
		)
	}

	if err != nil {
		log.Error("compress debezium encoding value failed",
			zap.String("format", string(d.config.EncodingFormat)),
			zap.Error(err),
			zap.Any("event", e))
		return errors.Trace(err)
	}

	m := &common.Message{
		Key:      key,
		Value:    value,
		Ts:       e.CommitTs,
		Schema:   e.TableInfo.GetSchemaNamePtr(),
		Table:    e.TableInfo.GetTableNamePtr(),
		Type:     model.MessageTypeRow,
		Protocol: config.ProtocolDebezium,
		Callback: callback,
	}
	m.IncRowsCount()

	if m.Length() > d.config.MaxMessageBytes {
		log.Warn("Single message is too large for avro",
			zap.Int("maxMessageBytes", d.config.MaxMessageBytes),
			zap.Int("length", m.Length()),
			zap.Any("table", e.TableInfo.TableName))
		return cerror.ErrMessageTooLarge.GenWithStackByArgs(m.Length())
	}

	d.messages = append(d.messages, m)
	return nil
}

// EncodeDDLEvent implements the RowEventEncoder interface
// DDL message unresolved tso
func (d *BatchEncoder) EncodeDDLEvent(_ *model.DDLEvent) (*common.Message, error) {
	// Schema Change Events are currently not supported.
	return nil, nil
}

// Build implements the RowEventEncoder interface
func (d *BatchEncoder) Build() []*common.Message {
	if len(d.messages) == 0 {
		return nil
	}

	result := d.messages
	d.messages = nil
	return result
}

// newBatchEncoder creates a new Debezium BatchEncoder.
func newBatchEncoder(c *common.Config, format codec.Format) codec.RowEventEncoder {
	return &BatchEncoder{
		messages: nil,
		config:   c,
		codec:    format,
	}
}

type batchEncoderBuilder struct {
	config *common.Config
	format codec.Format
}

// NewBatchEncoderBuilder creates a Debezium batchEncoderBuilder.
func NewBatchEncoderBuilder(
	ctx context.Context,
	config *common.Config,
	clusterID string,
) (codec.RowEventEncoderBuilder, error) {
	var format codec.Format
	if config.EncodingFormat == common.EncodingFormatAvro {
		schemaM, err := avro.NewSchemaManager(ctx, config)
		if err != nil {
			return nil, errors.Trace(err)
		}
		format = avro.NewAvroCodec(config.ChangefeedID.Namespace, config.ChangefeedID.ID, schemaM, config)
	} else {
		format = newDebeziumJSONCodec(config, clusterID)
	}
	return &batchEncoderBuilder{
		config: config,
		format: format,
	}, nil
}

// Build a `BatchEncoder`
func (b *batchEncoderBuilder) Build() codec.RowEventEncoder {
	return newBatchEncoder(b.config, b.format)
}

// CleanMetrics do nothing
func (b *batchEncoderBuilder) CleanMetrics() {}

// SetupEncoderAndSchemaRegistry4Testing start a local schema registry for testing.
func SetupEncoderAndSchemaRegistry4Testing(
	ctx context.Context,
	config *common.Config,
) (*BatchEncoder, error) {
	avro.StartHTTPInterceptForTestingRegistry()
	schemaM, err := avro.NewConfluentSchemaManager(ctx, "http://127.0.0.1:8081", nil)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &BatchEncoder{
		messages: make([]*common.Message, 0, 1),
		config:   config,
		codec:    avro.NewAvroCodec(model.DefaultNamespace, "default", schemaM, config),
	}, nil
}

// TeardownEncoderAndSchemaRegistry4Testing stop the local schema registry for testing.
func TeardownEncoderAndSchemaRegistry4Testing() {
	avro.StopHTTPInterceptForTestingRegistry()
}
