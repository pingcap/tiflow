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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/log"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/sync_diff_inspector/checkpoints"
	"github.com/pingcap/tiflow/sync_diff_inspector/chunk"
	"github.com/pingcap/tiflow/sync_diff_inspector/config"
	"github.com/pingcap/tiflow/sync_diff_inspector/report"
	"github.com/pingcap/tiflow/sync_diff_inspector/source/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestFatalFilesystemErrorRedaction(t *testing.T) {
	cases := []struct {
		name    string
		message string
		prepare func(*testing.T) (func(), string)
	}{
		{
			name:    "commit-report",
			message: "failed to commit report",
			prepare: func(t *testing.T) (func(), string) {
				outputDir := filepath.Join(t.TempDir(), "private-report-directory")
				df := &Diff{
					downstream: &mockChecksumSource{},
					report:     report.NewReport(&config.TaskConfig{OutputDir: outputDir}),
				}
				return func() { df.PrintSummary(context.Background()) }, filepath.Join(outputDir, "summary.txt")
			},
		},
		{
			name:    "remove-checkpoint",
			message: "fail to remove the checkpoint file",
			prepare: func(t *testing.T) (func(), string) {
				checkpointDir := filepath.Join(t.TempDir(), "private-checkpoint-directory")
				checkpointPath := filepath.Join(checkpointDir, checkpointFile)
				// A nonempty directory produces a real removal error on every
				// supported platform, including when tests run as root.
				require.NoError(t, os.MkdirAll(checkpointPath, 0o700))
				require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, "child"), nil, 0o600))
				df := &Diff{CheckpointDir: checkpointDir}
				return df.Close, checkpointPath
			},
		},
		{
			name:    "create-fix-sql",
			message: "write sql failed: cannot create file",
			prepare: func(t *testing.T) (func(), string) {
				fixDir := filepath.Join(t.TempDir(), "private-fix-directory")
				dml := &ChunkDML{
					node: &checkpoints.Node{ChunkRange: &chunk.Range{Index: &chunk.CID{}}},
					sqls: []string{"INSERT INTO tbl VALUES ('private-sql-value')"},
				}
				df := &Diff{
					downstream: &mockChecksumSource{tables: []*common.TableDiff{{Schema: "test", Table: "tbl"}}},
					FixSQLDir:  fixDir,
					sqlCh:      make(chan *ChunkDML, 1),
				}
				df.sqlCh <- dml
				df.sqlWg.Add(1)
				t.Cleanup(func() {
					df.sqlWg.Wait()
					require.Equal(t, []string{"INSERT INTO tbl VALUES ('private-sql-value')"}, dml.sqls)
					_, err := os.Stat(fixDir)
					require.True(t, os.IsNotExist(err))
				})
				return func() { df.writeSQLs(context.Background()) }, filepath.Join(fixDir, "test:tbl:0:0-0:0.sql")
			},
		},
	}
	for _, tc := range cases {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/redact=%t", tc.name, enabled), func(t *testing.T) {
				previous := dmlog.IsRedactLogEnabled()
				dmlog.SetRedactLog(enabled)
				t.Cleanup(func() { dmlog.SetRedactLog(previous) })
				core, observed := observer.New(zap.FatalLevel)
				// Preserve Fatal's control-flow termination while observing the
				// actual error path without terminating the entire test process.
				logger := zap.New(core, zap.WithFatalHook(zapcore.WriteThenPanic))
				t.Cleanup(log.ReplaceGlobals(logger, &log.ZapProperties{}))
				run, errorPath := tc.prepare(t)
				require.PanicsWithValue(t, tc.message, run)
				entries := observed.All()
				require.Len(t, entries, 1)
				require.Equal(t, tc.message, entries[0].Message)
				fields := entries[0].ContextMap()
				if enabled {
					require.Equal(t, "?", fields["error"])
					require.NotContains(t, fields, "errorVerbose")
					encoded, err := json.Marshal(fields)
					require.NoError(t, err)
					require.NotContains(t, string(encoded), "private-")
				} else {
					var originalError error
					for _, field := range entries[0].Context {
						if field.Key == "error" {
							require.Equal(t, zapcore.ErrorType, field.Type)
							originalError = field.Interface.(error)
						}
					}
					require.NotNil(t, originalError)
					var pathError *os.PathError
					require.True(t, errors.As(originalError, &pathError))
					require.Equal(t, errorPath, pathError.Path)
					require.Equal(t, originalError.Error(), fields["error"])
				}
				if tc.name == "create-fix-sql" {
					if enabled {
						require.Equal(t, "?", fields["sql"])
					} else {
						require.Equal(t, []any{"INSERT INTO tbl VALUES ('private-sql-value')"}, fields["sql"])
					}
				}
			})
		}
	}
}
