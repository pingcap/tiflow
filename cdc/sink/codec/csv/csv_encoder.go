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

package csv

import (
	"bytes"
	"context"
	"errors"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// BatchEncoder encodes the events into the byte of a batch into.
type BatchEncoder struct {
	valueBuf    *bytes.Buffer
	callbackBuf []func()
	batchSize   int
	csvConfig   *config.CSVConfig
}

// AppendRowChangedEvent implements the EventBatchEncoder interface
func (b *BatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	if b.csvConfig == nil {
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("no csv config provided"))
	}

	row, err := buildRowData(b.csvConfig, e)
	if err != nil {
		return err
	}
	b.valueBuf.Write(row)
	b.batchSize++
	if callback != nil {
		b.callbackBuf = append(b.callbackBuf, callback)
	}
	return nil
}

// EncodeDDLEvent implements the EventBatchEncoder interface
func (b *BatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
	return nil, nil
}

// EncodeCheckpointEvent implements the EventBatchEncoder interface
func (b *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	return nil, nil
}

// Build implements the EventBatchEncoder interface
func (b *BatchEncoder) Build() (messages []*common.Message) {
	if b.batchSize == 0 {
		return nil
	}

	ret := common.NewMsg(config.ProtocolCsv, nil, b.valueBuf.Bytes(), 0, model.MessageTypeRow, nil, nil)
	ret.SetRowsCount(b.batchSize)
	if len(b.callbackBuf) != 0 && len(b.callbackBuf) == b.batchSize {
		callbacks := b.callbackBuf
		ret.Callback = func() {
			for _, cb := range callbacks {
				cb()
			}
		}
		b.valueBuf.Reset()
		b.callbackBuf = make([]func(), 0)
	}
	return []*common.Message{ret}
}

// newBatchEncoder creates a new csv BatchEncoder.
func newBatchEncoder(config *config.CSVConfig) codec.EventBatchEncoder {
	return &BatchEncoder{
		csvConfig:   config,
		valueBuf:    &bytes.Buffer{},
		callbackBuf: make([]func(), 0),
	}
}

type batchEncoderBuilder struct {
	config *common.Config
}

// NewBatchEncoderBuilder creates a csv batchEncoderBuilder.
func NewBatchEncoderBuilder(config *common.Config) codec.EncoderBuilder {
	return &batchEncoderBuilder{config: config}
}

// Build a csv BatchEncoder
func (b *batchEncoderBuilder) Build() codec.EventBatchEncoder {
	return newBatchEncoder(b.config.CSVConfig)
}
