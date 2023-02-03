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
	"bytes"

	"github.com/goccy/go-json"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// batchDecoder decodes the byte into the original message.
type batchDecoder struct {
	data                []byte
	msg                 canalJSONMessageInterface
	enableTiDBExtension bool
	terminator          string
}

// NewBatchDecoder return a decoder for canal-json
func NewBatchDecoder(data []byte,
	enableTiDBExtension bool,
	terminator string,
) codec.EventBatchDecoder {
	return &batchDecoder{
		data:                data,
		msg:                 nil,
		enableTiDBExtension: enableTiDBExtension,
		terminator:          terminator,
	}
}

// HasNext implements the EventBatchDecoder interface
func (b *batchDecoder) HasNext() (model.MessageType, bool, error) {
	var (
		msg         canalJSONMessageInterface = &JSONMessage{}
		encodedData []byte
	)

	if b.enableTiDBExtension {
		msg = &canalJSONMessageWithTiDBExtension{
			JSONMessage: &JSONMessage{},
			Extensions:  &tidbExtension{},
		}
	}
	if len(b.terminator) > 0 {
		idx := bytes.IndexAny(b.data, b.terminator)
		if idx >= 0 {
			encodedData = b.data[:idx]
			b.data = b.data[idx+len(b.terminator):]
		} else {
			encodedData = b.data
			b.data = nil
		}
	} else {
		encodedData = b.data
		b.data = nil
	}

	if len(encodedData) == 0 {
		return model.MessageTypeUnknown, false, nil
	}

	if err := json.Unmarshal(encodedData, msg); err != nil {
		log.Error("canal-json decoder unmarshal data failed",
			zap.Error(err), zap.ByteString("data", encodedData))
		return model.MessageTypeUnknown, false, err
	}
	b.msg = msg

	return b.msg.messageType(), true, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
// `HasNext` should be called before this.
func (b *batchDecoder) NextRowChangedEvent() (*model.DetailedRowChangedEvent, error) {
	if b.msg == nil || b.msg.messageType() != model.MessageTypeRow {
		return nil, cerror.ErrCanalDecodeFailed.
			GenWithStack("not found row changed event message")
	}
	result, err := canalJSONMessage2RowChange(b.msg)
	if err != nil {
		return nil, err
	}
	b.msg = nil
	return result, nil
}

// NextDDLEvent implements the EventBatchDecoder interface
// `HasNext` should be called before this.
func (b *batchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.msg == nil || b.msg.messageType() != model.MessageTypeDDL {
		return nil, cerror.ErrCanalDecodeFailed.
			GenWithStack("not found ddl event message")
	}

	result := canalJSONMessage2DDLEvent(b.msg)
	b.msg = nil
	return result, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
// `HasNext` should be called before this.
func (b *batchDecoder) NextResolvedEvent() (uint64, error) {
	if b.msg == nil || b.msg.messageType() != model.MessageTypeResolved {
		return 0, cerror.ErrCanalDecodeFailed.
			GenWithStack("not found resolved event message")
	}

	withExtensionEvent, ok := b.msg.(*canalJSONMessageWithTiDBExtension)
	if !ok {
		log.Error("canal-json resolved event message should have tidb extension, but not found",
			zap.Any("msg", b.msg))
		return 0, cerror.ErrCanalDecodeFailed.
			GenWithStack("MessageTypeResolved tidb extension not found")
	}
	b.msg = nil
	return withExtensionEvent.Extensions.WatermarkTs, nil
}
