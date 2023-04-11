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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

// BatchEncoder encodes the events into the byte of a batch into.
type BatchEncoder struct {
	valueBuf  *bytes.Buffer
	callback  func()
	batchSize int
	config    *common.Config
}

// AppendTxnEvent implements the TxnEventEncoder interface
func (b *BatchEncoder) AppendTxnEvent(
	e *model.SingleTableTxn,
	callback func(),
) error {
	for _, rowEvent := range e.Rows {
		row, err := rowChangedEvent2CSVMsg(b.config, rowEvent)
		if err != nil {
			return err
		}
		b.valueBuf.Write(row.encode())
		b.batchSize++
	}
	b.callback = callback
	return nil
}

// Build implements the RowEventEncoder interface
func (b *BatchEncoder) Build() (messages []*common.Message) {
	if b.batchSize == 0 {
		return nil
	}

	ret := common.NewMsg(config.ProtocolCsv, nil,
		b.valueBuf.Bytes(), 0, model.MessageTypeRow, nil, nil)
	ret.SetRowsCount(b.batchSize)
	ret.Callback = b.callback
	b.valueBuf.Reset()
	b.callback = nil
	b.batchSize = 0

	return []*common.Message{ret}
}

// newBatchEncoder creates a new csv BatchEncoder.
func newBatchEncoder(config *common.Config) codec.TxnEventEncoder {
	return &BatchEncoder{
		config:   config,
		valueBuf: &bytes.Buffer{},
	}
}

type batchEncoderBuilder struct {
	config *common.Config
}

// NewTxnEventEncoderBuilder creates a csv batchEncoderBuilder.
func NewTxnEventEncoderBuilder(config *common.Config) codec.TxnEventEncoderBuilder {
	return &batchEncoderBuilder{config: config}
}

// Build a csv BatchEncoder
func (b *batchEncoderBuilder) Build() codec.TxnEventEncoder {
	return newBatchEncoder(b.config)
}
