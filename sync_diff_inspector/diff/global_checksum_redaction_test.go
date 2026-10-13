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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package diff

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/log"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/progress"
	"github.com/pingcap/tiflow/sync_diff_inspector/report"
	"github.com/pingcap/tiflow/sync_diff_inspector/source"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestGlobalChecksumCheckpointWarningRedaction(t *testing.T) {
	for _, missingTable := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("missing-table=%t/redact=%t", missingTable, enabled), func(t *testing.T) {
				previous := dmlog.IsRedactLogEnabled()
				dmlog.SetRedactLog(enabled)
				t.Cleanup(func() { dmlog.SetRedactLog(previous) })
				core, observed := observer.New(zap.WarnLevel)
				t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{}))

				// Leave the directory absent so the real checkpoint writer fails
				// with a filesystem error containing a sensitive directory name.
				checkpointDir := filepath.Join(t.TempDir(), "private-checkpoint-directory")
				table := &common.TableDiff{Schema: "test", Table: "tbl"}
				if missingTable {
					table.TableLack = common.UpstreamTableLackFlag
				}
				tables := []*common.TableDiff{table}
				newSource := func() *mockChecksumSource {
					return &mockChecksumSource{
						iterator: &mockChecksumIterator{chunks: []*chunk.Range{{
							Index: &chunk.CID{ChunkCnt: 1},
						}}},
						tables: tables,
						infos:  map[int]*source.ChecksumInfo{0: {Count: 7, Checksum: 19}},
					}
				}
				upstream, downstream := newSource(), newSource()
				df := &Diff{
					upstream:         upstream,
					downstream:       downstream,
					checkThreadCount: 1,
					cp:               new(checkpoints.Checkpoint),
					report:           report.NewReport(&config.TaskConfig{OutputDir: checkpointDir}),
					CheckpointDir:    checkpointDir,
				}
				df.report.Init(tables, nil, nil)
				progress.Init(1, 0)
				progress.SetOutput(io.Discard)
				t.Cleanup(progress.Close)

				require.NoError(t, df.equalByGlobalChecksum(context.Background()))
				_, statErr := os.Stat(checkpointDir)
				require.True(t, os.IsNotExist(statErr))

				warning := "fail to flush checksum checkpoint"
				if missingTable {
					warning = "failed to save checksum checkpoint"
				}
				entries := observed.FilterMessage(warning).All()
				require.Len(t, entries, 1)
				require.Equal(t, 1, observed.Len())
				fields := entries[0].ContextMap()
				if enabled {
					require.Equal(t, "?", fields["error"])
					require.NotContains(t, fields, "errorVerbose")
					require.NotContains(t, fmt.Sprint(fields), "private-checkpoint-directory")
				} else {
					require.Len(t, entries[0].Context, 1)
					field := entries[0].Context[0]
					require.Equal(t, zapcore.ErrorType, field.Type)
					pathErr, ok := field.Interface.(*os.PathError)
					require.True(t, ok, "expected the original checkpoint filesystem error")
					require.True(t, os.IsNotExist(pathErr))
					require.Contains(t, pathErr.Path, checkpointDir)
					require.Equal(t, pathErr.Error(), fields["error"])
				}

				result := df.report.TableResults["test"]["tbl"]
				require.True(t, result.DataEqual)
				require.Nil(t, result.MeetError)
				require.Empty(t, result.ChunkMap)
				require.NotNil(t, df.checksumCheckpoint)
				require.Zero(t, df.checksumCheckpoint.TableIndex)
				require.True(t, df.checksumCheckpoint.Upstream.Done)
				require.True(t, df.checksumCheckpoint.Downstream.Done)
				if missingTable {
					require.Zero(t, result.UpCount)
					require.Zero(t, result.DownCount)
					require.Zero(t, df.checksumCheckpoint.Upstream.Count)
					require.Zero(t, df.checksumCheckpoint.Downstream.Count)
					require.Nil(t, df.checksumCheckpoint.Upstream.LastRange)
					require.Nil(t, df.checksumCheckpoint.Downstream.LastRange)
					require.Empty(t, upstream.getIteratorCalls)
					require.Empty(t, downstream.getIteratorCalls)
				} else {
					require.Equal(t, int64(7), result.UpCount)
					require.Equal(t, int64(7), result.DownCount)
					for _, state := range []*checkpoints.ChecksumSourceState{df.checksumCheckpoint.Upstream, df.checksumCheckpoint.Downstream} {
						require.Equal(t, int64(7), state.Count)
						require.Equal(t, uint64(19), state.Checksum)
						require.NotNil(t, state.LastRange)
						require.Zero(t, state.LastRange.Index.ChunkIndex)
					}
					require.Equal(t, []int{0}, upstream.getIteratorCalls)
					require.Equal(t, []int{0}, downstream.getIteratorCalls)
				}
			})
		}
	}
}
