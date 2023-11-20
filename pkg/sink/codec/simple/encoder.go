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

package simple

import (
	"context"
	"encoding/json"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

//nolint:unused
type encoder struct {
	config *common.Config

	messages []*common.Message
}

// AppendRowChangedEvent implement the RowEventEncoder interface
func (e *encoder) AppendRowChangedEvent(
	_ context.Context, _ string, event *model.RowChangedEvent, callback func(),
) error {
	m := newDMLMessage(event)
	value, err := json.Marshal(m)
	if err != nil {
		return cerror.WrapError(cerror.ErrEncodeFailed, err)
	}

	result := &common.Message{
		Key:      nil,
		Value:    value,
		Ts:       event.CommitTs,
		Schema:   &event.Table.Schema,
		Table:    &event.Table.Table,
		Type:     model.MessageTypeRow,
		Protocol: config.ProtocolSimple,
		Callback: callback,
	}
	result.IncRowsCount()
	e.messages = append(e.messages, result)
	return nil
}

// Build implement the RowEventEncoder interface
func (e *encoder) Build() []*common.Message {
	if len(e.messages) == 0 {
		return nil
	}
	result := e.messages
	e.messages = nil
	return result
}

// EncodeCheckpointEvent implement the DDLEventBatchEncoder interface
func (e *encoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	message := newResolvedMessage(ts)
	value, err := json.Marshal(message)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrEncodeFailed, err)
	}
	return common.NewResolvedMsg(config.ProtocolSimple, nil, value, ts), nil
}

// EncodeDDLEvent implement the DDLEventBatchEncoder interface
func (e *encoder) EncodeDDLEvent(event *model.DDLEvent) (*common.Message, error) {
	message := newDDLMessage(event)
	value, err := json.Marshal(message)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrEncodeFailed, err)
	}
	return common.NewDDLMsg(config.ProtocolSimple, nil, value, event), nil
}

type builder struct {
	config *common.Config
}

// NewBuilder returns a new builder
func NewBuilder(config *common.Config) *builder {
	return &builder{
		config: config,
	}
}

// Build implement the RowEventEncoderBuilder interface
func (b *builder) Build() codec.RowEventEncoder {
	return &encoder{
		config:   b.config,
		messages: make([]*common.Message, 0, 1),
	}
}

// CleanMetrics implement the RowEventEncoderBuilder interface
func (b *builder) CleanMetrics() {
	// do nothing
}
