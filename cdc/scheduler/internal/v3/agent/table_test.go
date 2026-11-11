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
// limitations under the License.

package agent

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/logutil/redact"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestTableManager(t *testing.T) {
	t.Parallel()

	// pretend there are 4 tables
	mockTableExecutor := newMockTableExecutor()

	tableM := newTableSpanManager(model.ChangeFeedID{}, mockTableExecutor)

	span1 := spanz.TableIDToComparableSpan(1)
	tableM.addTableSpan(span1)
	require.Equal(t, tablepb.TableStateAbsent, tableM.tables.GetV(span1).state)

	tableM.dropTableSpan(span1)
	require.False(t, tableM.tables.Has(span1))
}

func TestInjectDispatchTableTaskLogRedaction(t *testing.T) {
	span := tablepb.Span{
		TableID: 42, StartKey: []byte("table-start-secret"), EndKey: []byte("table-end-secret"),
	}
	taskSpan := tablepb.Span{
		TableID: 42, StartKey: []byte("task-start-secret"), EndKey: []byte("task-end-secret"),
	}
	for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
		t.Run(mode.String(), func(t *testing.T) {
			originalMode := errors.RedactLogEnabled.Load()
			redact.SetRedactionMode(mode)
			t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
			var output bytes.Buffer
			core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&output), zap.DebugLevel)
			t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.DebugLevel)}))

			table := newTableSpan(model.DefaultChangeFeedID("redact-task"), span, newMockTableExecutor())
			task := &dispatchTableTask{Span: taskSpan}
			require.PanicsWithValue(t, "schedulerv3: tableID not match", func() { table.injectDispatchTableTask(task) })
			require.Nil(t, table.task)
			require.Equal(t, span, table.span)
			require.Equal(t, taskSpan, task.Span)

			var entry map[string]interface{}
			require.NoError(t, json.Unmarshal(output.Bytes(), &entry))
			require.Equal(t, "schedulerv3: tableID not match", entry["msg"])
			require.Equal(t, "redact-task", entry["changefeed"])
			switch mode {
			case redact.RedactionModeOff:
				require.Equal(t, taskSpan.String(), entry["task.TableID"])
				for _, key := range []tablepb.Key{span.StartKey, span.EndKey, taskSpan.StartKey, taskSpan.EndKey} {
					require.Contains(t, output.String(), hex.EncodeToString(key))
				}
			case redact.RedactionModeOn:
				require.Equal(t, "?", entry["tableSpan"])
				require.Equal(t, "?", entry["task.TableID"])
			case redact.RedactionModeMarker:
				require.Equal(t, "‹"+taskSpan.String()+"›", entry["task.TableID"])
				for _, field := range []string{"tableSpan", "task.TableID"} {
					value := entry[field].(string)
					require.True(t, strings.HasPrefix(value, "‹") && strings.HasSuffix(value, "›"))
				}
			}

			// A matching dispatch still becomes the active task without altering its keys.
			matchingTask := &dispatchTableTask{Span: span}
			table.injectDispatchTableTask(matchingTask)
			require.Same(t, matchingTask, table.task)
			require.Equal(t, span, matchingTask.Span)
			if mode == redact.RedactionModeOn {
				for _, key := range []tablepb.Key{span.StartKey, span.EndKey, taskSpan.StartKey, taskSpan.EndKey} {
					require.NotContains(t, output.String(), string(key))
					require.NotContains(t, output.String(), hex.EncodeToString(key))
				}
			}
		})
	}
}
