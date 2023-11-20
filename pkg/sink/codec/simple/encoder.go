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
	cfg *common.Config
}

type builder struct {
	cfg *common.Config
}

// NewBuilder returns a new builder
func NewBuilder(cfg *common.Config) *builder {
	return &builder{
		cfg: cfg,
	}
}

// Build implement the RowEventEncoderBuilder interface
func (b *builder) Build() codec.RowEventEncoder {
	return &encoder{
		cfg: b.cfg,
	}
}

// CleanMetrics implement the RowEventEncoderBuilder interface
func (b *builder) CleanMetrics() {}

// AppendRowChangedEvent implement the RowEventEncoder interface
//
//nolint:unused
func (e *encoder) AppendRowChangedEvent(
	ctx context.Context, s string, event *model.RowChangedEvent, callback func(),
) error {
	// TODO implement me
	panic("implement me")
}

// Build implement the RowEventEncoder interface
//
//nolint:unused
func (e *encoder) Build() []*common.Message {
	// TODO implement me
	panic("implement me")
}

// EncodeCheckpointEvent implement the DDLEventBatchEncoder interface
//
//nolint:unused
func (e *encoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	message := newResolvedMessage(ts)
	value, err := json.Marshal(message)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrEncodeFailed, err)
	}
	return common.NewResolvedMsg(config.ProtocolSimple, nil, value, ts), nil
}

// EncodeDDLEvent implement the DDLEventBatchEncoder interface
//
//nolint:unused
func (e *encoder) EncodeDDLEvent(event *model.DDLEvent) (*common.Message, error) {
	var message *message
	if event.IsBootstrap {
		message = newBootstrapMessage(event)
	} else {
		message = newDDLMessage(event)
	}
	value, err := json.Marshal(message)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrEncodeFailed, err)
	}
	return common.NewDDLMsg(config.ProtocolSimple, nil, value, event), nil
}
