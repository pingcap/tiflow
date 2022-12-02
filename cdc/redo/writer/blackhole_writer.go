// Copyright 2021 PingCAP, Inc.
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

package writer

import (
	"context"
	"errors"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

// blackHoleSink defines a blackHole storage, it receives events and persists
// without any latency
type blackHoleWriter struct{}

func (bs *blackHoleWriter) DeleteAllLogs(ctx context.Context) error {
	return nil
}

func (bs *blackHoleWriter) GC(ctx context.Context, checkpointTs model.Ts) error {
	return nil
}

// NewBlackHoleWriter creates a blackHole writer
func NewBlackHoleWriter() *blackHoleWriter {
	return &blackHoleWriter{}
}

func (bs *blackHoleWriter) WriteLog(_ context.Context, logs []*model.RedoRowChangedEvent) (err error) {
	if len(logs) == 0 {
		return nil
	}
	current := logs[len(logs)-1].Row.CommitTs
	log.Debug("write row redo logs", zap.Int("count", len(logs)),
		zap.Uint64("current", current))
	return
}

func (bs *blackHoleWriter) FlushLog(_ context.Context, checkpointTs, resolvedTs model.Ts) error {
	return nil
}

func (ibs *blackHoleWriter) GetMeta() (checkpointTs, resolvedTs model.Ts) {
	return 0, 0
}

func (bs *blackHoleWriter) SendDDL(_ context.Context, ddl *model.RedoDDLEvent) error {
	log.Debug("send ddl event", zap.Any("ddl", ddl))
	return nil
}

func (bs *blackHoleWriter) Close() error {
	return nil
}

type invalidBlackHoleWriter struct {
	*blackHoleWriter
}

// NewInvalidBlackHoleWriter creates a invalid blackHole writer
func NewInvalidBlackHoleWriter(rl RedoLogWriter) *invalidBlackHoleWriter {
	return &invalidBlackHoleWriter{
		blackHoleWriter: rl.(*blackHoleWriter),
	}
}

func (ibs *invalidBlackHoleWriter) WriteLog(
	_ context.Context, _ []*model.RedoRowChangedEvent,
) (err error) {
	return errors.New("[WriteLog] invalid black hole writer")
}

func (ibs *invalidBlackHoleWriter) FlushLog(
	_ context.Context, _, _ model.Ts,
) error {
	return errors.New("[FlushLog] invalid black hole writer")
}
