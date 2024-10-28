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
	"bytes"
	"context"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

// BatchEncoder encodes message into Debezium format.
type BatchEncoder struct {
	messages []*common.Message

	config *common.Config
	codec  *dbzCodec
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	// Currently ignored. Debezium MySQL Connector does not emit such event.
	return nil, nil
}

func (d *BatchEncoder) encodeKey(e *model.RowChangedEvent) ([]byte, error) {
	keyBuf := bytes.Buffer{}
	err := d.codec.EncodeKey(e, &keyBuf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: Use a streaming compression is better.
	key, err := common.Compress(
		d.config.ChangefeedID,
		d.config.LargeMessageHandle.LargeMessageHandleCompression,
		keyBuf.Bytes(),
	)
	return key, err
}

func (d *BatchEncoder) encodeValue(e *model.RowChangedEvent) ([]byte, error) {
	valueBuf := bytes.Buffer{}
	err := d.codec.EncodeValue(e, &valueBuf)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// TODO: Use a streaming compression is better.
	value, err := common.Compress(
		d.config.ChangefeedID,
		d.config.LargeMessageHandle.LargeMessageHandleCompression,
		valueBuf.Bytes(),
	)
	return value, err
}

// AppendRowChangedEvent implements the RowEventEncoder interface
func (d *BatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	var key []byte
	var value []byte
	var err error
	if key, err = d.encodeKey(e); err != nil {
		return errors.Trace(err)
	}
	if value, err = d.encodeValue(e); err != nil {
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

	d.messages = append(d.messages, m)
	return nil
}

// EncodeDDLEvent implements the RowEventEncoder interface
// DDL message unresolved tso
func (d *BatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
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
func newBatchEncoder(c *common.Config, clusterID string) codec.RowEventEncoder {
	batch := &BatchEncoder{
		messages: nil,
		config:   c,
		codec: &dbzCodec{
			config:    c,
			clusterID: clusterID,
			nowFunc:   time.Now,
		},
	}
	return batch
}

type batchEncoderBuilder struct {
	config    *common.Config
	clusterID string
}

// NewBatchEncoderBuilder creates a Debezium batchEncoderBuilder.
func NewBatchEncoderBuilder(config *common.Config, clusterID string) codec.RowEventEncoderBuilder {
	return &batchEncoderBuilder{
		config:    config,
		clusterID: clusterID,
	}
}

// Build a `BatchEncoder`
func (b *batchEncoderBuilder) Build() codec.RowEventEncoder {
	return newBatchEncoder(b.config, b.clusterID)
}

// CleanMetrics do nothing
func (b *batchEncoderBuilder) CleanMetrics() {}
