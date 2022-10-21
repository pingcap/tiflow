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
	"github.com/pingcap/tiflow/cdc/sinkv2/ddlsink"
	"go.uber.org/zap"
)

// Assert DDLEventSink implementation
var _ ddlsink.DDLEventSink = (*ddlSink)(nil)

type ddlSink struct{}

// New create a black hole DDL sink.
func New() *ddlSink {
	return &ddlSink{}
}

func (d *ddlSink) WriteDDLEvent(ctx context.Context,
	ddl *model.DDLEvent,
) error {
	log.Debug("BlackHoleSink: DDL Event", zap.Any("ddl", ddl))
	return nil
}

func (d *ddlSink) WriteCheckpointTs(ctx context.Context,
	ts uint64, tables []*model.TableInfo,
) error {
	log.Debug("BlackHoleSink: Checkpoint Ts Event", zap.Uint64("ts", ts), zap.Any("tables", tables))
	return nil
}

// Close do nothing.
func (d *ddlSink) Close() error {
	return nil
}
