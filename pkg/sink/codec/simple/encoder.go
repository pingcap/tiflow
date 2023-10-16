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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
)

type encoder struct {
}

type builder struct {
}

// NewBuilder returns a new builder
func NewBuilder() *builder {
	//TODO implement me
	panic("implement me")
}

// AppendRowChangedEvent implement the RowEventEncoder interface
func (e *encoder) AppendRowChangedEvent(
	ctx context.Context, s string, event *model.RowChangedEvent, callback func(),
) error {
	//TODO implement me
	panic("implement me")
}

// Build implement the RowEventEncoder interface
func (e *encoder) Build() []*common.Message {
	//TODO implement me
	panic("implement me")
}

// EncodeCheckpointEvent implement the DDLEventBatchEncoder interface
func (e *encoder) EncodeCheckpointEvent(ts uint64) (*common.Message, error) {
	//TODO implement me
	panic("implement me")
}

// EncodeDDLEvent implement the DDLEventBatchEncoder interface
func (e *encoder) EncodeDDLEvent(event *model.DDLEvent) (*common.Message, error) {
	//TODO implement me
	panic("implement me")
}
