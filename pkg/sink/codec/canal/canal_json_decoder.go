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
	"context"

	"github.com/goccy/go-json"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// batchDecoder decodes the byte into the original message.
type batchDecoder struct {
	data []byte
	msg  canalJSONMessageInterface

	config *common.Config

	storage storage.ExternalStorage
}

// NewBatchDecoder return a decoder for canal-json
func NewBatchDecoder(
	ctx context.Context, codecConfig *common.Config,
) (codec.RowEventDecoder, error) {
	var (
		storage storage.ExternalStorage
		err     error
	)
	if codecConfig.LargeMessageHandle.EnableClaimCheck() {
		storageURI := codecConfig.LargeMessageHandle.ClaimCheckStorageURI
		storage, err = util.GetExternalStorageFromURI(ctx, storageURI)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
	}
	return &batchDecoder{
		config:  codecConfig,
		storage: storage,
	}, nil
}

// AddKeyValue implements the RowEventDecoder interface
func (b *batchDecoder) AddKeyValue(_, value []byte) error {
	b.data = value
	return nil
}

// HasNext implements the RowEventDecoder interface
func (b *batchDecoder) HasNext() (model.MessageType, bool, error) {
	var (
		msg         canalJSONMessageInterface = &JSONMessage{}
		encodedData []byte
	)

	if b.config.EnableTiDBExtension {
		msg = &canalJSONMessageWithTiDBExtension{
			JSONMessage: &JSONMessage{},
			Extensions:  &tidbExtension{},
		}
	}
	if len(b.config.Terminator) > 0 {
		idx := bytes.IndexAny(b.data, b.config.Terminator)
		if idx >= 0 {
			encodedData = b.data[:idx]
			b.data = b.data[idx+len(b.config.Terminator):]
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

// NextRowChangedEvent implements the RowEventDecoder interface
// `HasNext` should be called before this.
func (b *batchDecoder) NextRowChangedEvent() (*model.RowChangedEvent, error) {
	if b.msg == nil || b.msg.messageType() != model.MessageTypeRow {
		return nil, cerror.ErrCanalDecodeFailed.
			GenWithStack("not found row changed event message")
	}

	message, withExtension := b.msg.(*canalJSONMessageWithTiDBExtension)
	if withExtension && message.Extensions.ClaimCheckLocation != "" {
		data, err := b.storage.ReadFile(context.Background(), message.Extensions.ClaimCheckLocation)
		if err != nil {
			return nil, err
		}
		claimCheckM, err := common.UnmarshalClaimCheckMessage(data)
		if err != nil {
			return nil, err
		}

		message = &canalJSONMessageWithTiDBExtension{}
		err = json.Unmarshal(claimCheckM.Value, message)
		if err != nil {
			return nil, err
		}
		b.msg = message

		return b.NextRowChangedEvent()

	}

	result, err := canalJSONMessage2RowChange(b.msg)
	if err != nil {
		return nil, err
	}
	b.msg = nil
	return result, nil
}

// NextDDLEvent implements the RowEventDecoder interface
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

// NextResolvedEvent implements the RowEventDecoder interface
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
