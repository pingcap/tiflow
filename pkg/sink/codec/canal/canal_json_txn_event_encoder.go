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

package canal

import (
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"go.uber.org/zap"
)

// JSONTxnEventEncoder encodes txn event in JSON format
type JSONTxnEventEncoder struct {
	builder *canalEntryBuilder

	config *common.Config

	// the symbol separating two lines
	terminator []byte
	valueBuf   *bytes.Buffer
	batchSize  int
	callback   func()

	// Store some fields of the txn event.
	txnCommitTs uint64
	txnSchema   *string
	txnTable    *string
}

// AppendTxnEvent appends a txn event to the encoder.
func (j *JSONTxnEventEncoder) AppendTxnEvent(
	txn *model.SingleTableTxn,
	callback func(),
) error {
	for _, row := range txn.Rows {
		value, err := newJSONMessageForDML(j.builder, row, j.config, false, "")
		if err != nil {
			return errors.Trace(err)
		}
		length := len(value) + common.MaxRecordOverhead
		// For single message that is longer than max-message-bytes, do not send it.
		if length > j.config.MaxMessageBytes {
			log.Warn("Single message is too large for canal-json",
				zap.Int("maxMessageBytes", j.config.MaxMessageBytes),
				zap.Int("length", length),
				zap.Any("table", row.Table))
			return cerror.ErrMessageTooLarge.GenWithStackByArgs()
		}
		j.valueBuf.Write(value)
		j.valueBuf.Write(j.terminator)
		j.batchSize++
	}
	j.callback = callback
	j.txnCommitTs = txn.CommitTs
	j.txnSchema = &txn.Table.Schema
	j.txnTable = &txn.Table.Table
	return nil
}

// Build builds a message from the encoder and resets the encoder.
func (j *JSONTxnEventEncoder) Build() []*common.Message {
	if j.batchSize == 0 {
		return nil
	}

	ret := common.NewMsg(config.ProtocolCanalJSON, nil,
		j.valueBuf.Bytes(), j.txnCommitTs, model.MessageTypeRow, j.txnSchema, j.txnTable)
	ret.SetRowsCount(j.batchSize)
	ret.Callback = j.callback
	j.valueBuf.Reset()
	j.callback = nil
	j.batchSize = 0
	j.txnCommitTs = 0
	j.txnSchema = nil
	j.txnTable = nil

	return []*common.Message{ret}
}

// newJSONTxnEventEncoder creates a new JSONTxnEventEncoder
func newJSONTxnEventEncoder(config *common.Config) codec.TxnEventEncoder {
	encoder := &JSONTxnEventEncoder{
		builder:    newCanalEntryBuilder(config),
		valueBuf:   &bytes.Buffer{},
		terminator: []byte(config.Terminator),

		config: config,
	}
	return encoder
}

type jsonTxnEventEncoderBuilder struct {
	config *common.Config
}

// NewJSONTxnEventEncoderBuilder creates a jsonTxnEventEncoderBuilder.
func NewJSONTxnEventEncoderBuilder(config *common.Config) codec.TxnEventEncoderBuilder {
	return &jsonTxnEventEncoderBuilder{config: config}
}

// Build a `jsonTxnEventEncoderBuilder`
func (b *jsonTxnEventEncoderBuilder) Build() codec.TxnEventEncoder {
	return newJSONTxnEventEncoder(b.config)
}
