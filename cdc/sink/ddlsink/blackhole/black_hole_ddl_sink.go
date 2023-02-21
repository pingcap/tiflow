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
// limitations under the License

package blackhole

import (
	"context"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/ddlsink"
	"go.uber.org/zap"
)

// Assert Sink implementation
var _ ddlsink.Sink = (*DDLSink)(nil)

// DDLSink is a black hole DDL sink.
type DDLSink struct{}

// NewDDLSink create a black hole DDL sink.
func NewDDLSink() *DDLSink {
	return &DDLSink{}
}

// WriteDDLEvent do nothing.
func (d *DDLSink) WriteDDLEvent(ctx context.Context,
	ddl *model.DDLEvent,
) error {
	log.Debug("BlackHoleSink: DDL Event", zap.Any("ddl", ddl))
	return nil
}

// WriteCheckpointTs do nothing.
func (d *DDLSink) WriteCheckpointTs(ctx context.Context,
	ts uint64, tables []*model.TableInfo,
) error {
	log.Debug("BlackHoleSink: Checkpoint Ts Event", zap.Uint64("ts", ts), zap.Any("tables", tables))
	return nil
}

// Close do nothing.
func (d *DDLSink) Close() {}
