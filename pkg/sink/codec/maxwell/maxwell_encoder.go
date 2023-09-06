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

package maxwell

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

// BatchEncoder is a maxwell format encoder implementation
type BatchEncoder struct {
	keyBuf      *bytes.Buffer
	valueBuf    *bytes.Buffer
	callbackBuf []func()
	batchSize   int

	config *common.Config
}

// EncodeCheckpointEvent implements the RowEventEncoder interface
func (d *BatchEncoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	// For maxwell now, there is no such a corresponding type to ResolvedEvent so far.
	// Therefore the event is ignored.
	return nil, nil
}

// AppendRowChangedEvent implements the RowEventEncoder interface
func (d *BatchEncoder) AppendRowChangedEvent(
	_ context.Context,
	_ string,
	e *model.RowChangedEvent,
	callback func(),
) error {
	_, valueMsg := rowChangeToMaxwellMsg(e, d.config.DeleteOnlyHandleKeyColumns)
	value, err := valueMsg.encode()
	if err != nil {
		return errors.Trace(err)
	}
	d.valueBuf.Write(value)
	d.batchSize++
	if callback != nil {
		d.callbackBuf = append(d.callbackBuf, callback)
	}
	return nil
}

// EncodeDDLEvent implements the RowEventEncoder interface
// DDL message unresolved tso
func (d *BatchEncoder) EncodeDDLEvent(e *model.DDLEvent) (*common.Message, error) {
	keyMsg, valueMsg := ddlEventToMaxwellMsg(e)
	key, err := keyMsg.Encode()
	if err != nil {
		return nil, errors.Trace(err)
	}
	value, err := valueMsg.encode()
	if err != nil {
		return nil, errors.Trace(err)
	}

	return common.NewDDLMsg(config.ProtocolMaxwell, key, value, e), nil
}

// Build implements the RowEventEncoder interface
func (d *BatchEncoder) Build() []*common.Message {
	if d.batchSize == 0 {
		return nil
	}

	ret := common.NewMsg(config.ProtocolMaxwell,
		d.keyBuf.Bytes(), d.valueBuf.Bytes(), 0, model.MessageTypeRow, nil, nil)
	ret.SetRowsCount(d.batchSize)
	if len(d.callbackBuf) != 0 && len(d.callbackBuf) == d.batchSize {
		callbacks := d.callbackBuf
		ret.Callback = func() {
			for _, cb := range callbacks {
				cb()
			}
		}
		d.callbackBuf = make([]func(), 0)
	}
	d.reset()
	return []*common.Message{ret}
}

// reset implements the RowEventEncoder interface
func (d *BatchEncoder) reset() {
	d.keyBuf.Reset()
	d.valueBuf.Reset()
	d.batchSize = 0
	var versionByte [8]byte
	binary.BigEndian.PutUint64(versionByte[:], codec.BatchVersion1)
	d.keyBuf.Write(versionByte[:])
}

// newBatchEncoder creates a new maxwell BatchEncoder.
func newBatchEncoder(config *common.Config) codec.RowEventEncoder {
	batch := &BatchEncoder{
		keyBuf:      &bytes.Buffer{},
		valueBuf:    &bytes.Buffer{},
		callbackBuf: make([]func(), 0),
		config:      config,
	}
	batch.reset()
	return batch
}

type batchEncoderBuilder struct {
	config *common.Config
}

// NewBatchEncoderBuilder creates a maxwell batchEncoderBuilder.
func NewBatchEncoderBuilder(config *common.Config) codec.RowEventEncoderBuilder {
	return &batchEncoderBuilder{
		config: config,
	}
}

// Build a `maxwellBatchEncoder`
func (b *batchEncoderBuilder) Build() codec.RowEventEncoder {
	return newBatchEncoder(b.config)
}

// CleanMetrics do nothing
func (b *batchEncoderBuilder) CleanMetrics() {}
