// Copyright 2020 PingCAP, Inc.
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

package cdclog

import (
	"context"

	"github.com/pingcap/ticdc/cdc/model"
)

type fileSink struct {
}

func (f *fileSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	return nil
}

func (f *fileSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	return nil
}

func (f *fileSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	return nil
}

func (f *fileSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return nil
}

func (f *fileSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	return nil
}

func (f *fileSink) Close() error {
	return nil
}

// NewLocalFileSink support log data to file.
func NewLocalFileSink() *fileSink {
	return &fileSink{}
}
