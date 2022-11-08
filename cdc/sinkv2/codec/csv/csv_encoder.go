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

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/codec"
	"github.com/pingcap/tiflow/cdc/sinkv2/codec/common"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
)

// BatchEncoder encodes the events into the byte of a batch into.
type BatchEncoder struct {
	valueBuf  *bytes.Buffer
	callback  func()
	batchSize int
	csvConfig *config.CSVConfig
}

// AppendRowChangedEvents implements the EventBatchEncoder interface
func (b *BatchEncoder) AppendRowChangedEvents(_ context.Context, _ string, events []*eventsink.RowChangeCallbackableEvent) error {
	return nil
}

// AppendTxnEvent implements the EventBatchEncoder interface
func (b *BatchEncoder) AppendTxnEvent(txn *eventsink.TxnCallbackableEvent) error {
	if b.csvConfig == nil {
		return cerror.WrapError(cerror.ErrSinkInvalidConfig,
			errors.New("no csv config provided"))
	}

	for _, e := range txn.Event.Rows {
		row, err := rowChangedEvent2CSVMsg(b.csvConfig, e)
		if err != nil {
			return err
		}
		b.valueBuf.Write(row.encode())
		b.batchSize++
	}

	b.callback = txn.Callback

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
	ret.Callback = b.callback
	b.callback = nil
	return []*common.Message{ret}
}

// newBatchEncoder creates a new csv BatchEncoder.
func newBatchEncoder(config *config.CSVConfig) codec.EventBatchEncoder {
	return &BatchEncoder{
		csvConfig: config,
		valueBuf:  &bytes.Buffer{},
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
