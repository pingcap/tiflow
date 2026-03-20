// Copyright 2026 PingCAP, Inc.
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

package diff

import (
	"context"
	"path/filepath"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/progress"
	"go.uber.org/zap"
)

func (df *Diff) initChecksumCheckpoint() error {
	finishTableNums := 0
	path := filepath.Join(df.CheckpointDir, checkpointFile)
	if fileExists(path) {
		checksumState, reportInfo, err := df.cp.LoadChecksumState(path)
		if err != nil {
			return errors.Annotate(err, "the checksum checkpoint load process failed")
		}
		if checksumState != nil {
			df.checksumCheckpoint = checksumState
			if reportInfo != nil {
				df.report.LoadReport(reportInfo)
			}
			log.Info("load checksum checkpoint", zap.Int("table index", checksumState.TableIndex))
			finishTableNums = checksumState.TableIndex
			if checksumState.Upstream.Done && checksumState.Downstream.Done {
				finishTableNums++
			}
		}
	}
	progress.Init(len(df.downstream.GetTables()), finishTableNums)
	return nil
}

func (df *Diff) flushChecksumCheckpoint(
	ctx context.Context,
	state *checkpoints.ChecksumState,
) error {
	df.checksumCheckpointMu.Lock()
	defer df.checksumCheckpointMu.Unlock()

	tableDiff := df.downstream.GetTables()[state.TableIndex]
	r, err := df.report.GetSnapshot(
		&chunk.CID{TableIndex: state.TableIndex},
		tableDiff.Schema,
		tableDiff.Table,
	)
	if err != nil {
		return errors.Trace(err)
	}
	return df.cp.SaveChecksumState(ctx, filepath.Join(df.CheckpointDir, checkpointFile), state, r)
}
