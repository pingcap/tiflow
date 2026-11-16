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

package blackhole

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/pkg/logutil/redact"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestWriteEventsLogRedaction(t *testing.T) {
	const secret = "blackhole-secret-value"
	cols := []*model.Column{{Name: "value", Type: 1, Value: secret}}
	tableInfo := model.BuildTableInfo("db", "tbl", cols, nil)
	row := &model.RowChangedEvent{
		StartTs: 1, CommitTs: 2, TableInfo: tableInfo,
		Columns: model.Columns2ColumnDatas(cols, tableInfo),
	}
	for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
		t.Run(mode.String(), func(t *testing.T) {
			originalMode := errors.RedactLogEnabled.Load()
			redact.SetRedactionMode(mode)
			t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
			var output bytes.Buffer
			core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&output), zap.DebugLevel)
			t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.DebugLevel)}))

			callbacks := 0
			err := NewDMLSink().WriteEvents(&dmlsink.RowChangeCallbackableEvent{
				Event: row, Callback: func() { callbacks++ },
			})
			require.NoError(t, err)
			require.Equal(t, 1, callbacks)
			require.Equal(t, secret, row.Columns[0].Value)
			var entry map[string]interface{}
			require.NoError(t, json.Unmarshal(output.Bytes(), &entry))
			require.Equal(t, "BlackHoleSink: WriteEvents", entry["msg"])
			switch mode {
			case redact.RedactionModeOff:
				loggedRow := entry["row"].(map[string]interface{})
				loggedCols := loggedRow["Columns"].([]interface{})
				require.Equal(t, secret, loggedCols[0].(map[string]interface{})["value"])
			case redact.RedactionModeOn:
				require.Equal(t, "?", entry["row"])
				require.NotContains(t, output.String(), secret)
			case redact.RedactionModeMarker:
				require.Equal(t, redact.String(fmt.Sprint(row)), entry["row"])
			}
		})
	}
}
