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

package codec

import (
	"encoding/json"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// canalJSONBatchDecoder decodes the byte into the original message.
type canalJSONBatchDecoder struct {
	data                []byte
	msg                 canalJSONMessageInterface
	enableTiDBExtension bool
}

// NewCanalJSONBatchDecoder return a decoder for canal-json
func NewCanalJSONBatchDecoder(data []byte, enableTiDBExtension bool) EventBatchDecoder {
	return &canalJSONBatchDecoder{
		data:                data,
		msg:                 nil,
		enableTiDBExtension: enableTiDBExtension,
	}
}

// HasNext implements the EventBatchDecoder interface
func (b *canalJSONBatchDecoder) HasNext() (model.MessageType, bool, error) {
	if len(b.data) == 0 {
		return model.MessageTypeUnknown, false, nil
	}
	var msg canalJSONMessageInterface = &canalJSONMessage{}
	if b.enableTiDBExtension {
		msg = &canalJSONMessageWithTiDBExtension{
			canalJSONMessage: &canalJSONMessage{},
			Extensions:       &tidbExtension{},
		}
	}
	if err := json.Unmarshal(b.data, msg); err != nil {
		log.Error("canal-json decoder unmarshal data failed",
			zap.Error(err), zap.ByteString("data", b.data))
		return model.MessageTypeUnknown, false, err
	}
	b.msg = msg
	b.data = nil

	return b.msg.mqMessageType(), true, nil
}

// NextRowChangedEvent implements the EventBatchDecoder interface
// `HasNext` should be called before this.
func (b *canalJSONBatchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.msg == nil || b.msg.mqMessageType() != model.MessageTypeRow {
		return nil, cerrors.ErrCanalDecodeFailed.
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
func (b *canalJSONBatchDecoder) NextDDLEvent() (*model.DDLEvent, error) {
	if b.msg == nil || b.msg.mqMessageType() != model.MessageTypeDDL {
		return nil, cerrors.ErrCanalDecodeFailed.
			GenWithStack("not found ddl event message")
	}

	result := canalJSONMessage2DDLEvent(b.msg)
	b.msg = nil
	return result, nil
}

// NextResolvedEvent implements the EventBatchDecoder interface
// `HasNext` should be called before this.
func (b *canalJSONBatchDecoder) NextResolvedEvent() (uint64, error) {
	if b.msg == nil || b.msg.mqMessageType() != model.MessageTypeResolved {
		return 0, cerrors.ErrCanalDecodeFailed.
			GenWithStack("not found resolved event message")
	}

	withExtensionEvent, ok := b.msg.(*canalJSONMessageWithTiDBExtension)
	if !ok {
		log.Error("canal-json resolved event message should have tidb extension, but not found",
			zap.Any("msg", b.msg))
		return 0, cerrors.ErrCanalDecodeFailed.
			GenWithStack("MqMessageTypeResolved tidb extension not found")
	}
	b.msg = nil
	return withExtensionEvent.Extensions.WatermarkTs, nil
}
