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

package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/pkg/logutil/redact"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func captureModelRedactionLogs(t *testing.T, mode redact.RedactionMode) *bytes.Buffer {
	t.Helper()
	originalMode := errors.RedactLogEnabled.Load()
	redact.SetRedactionMode(mode)
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
	output := &bytes.Buffer{}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(output), zap.DebugLevel)
	t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.DebugLevel)}))
	return output
}

func TestSplitUpdateEventLogRedaction(t *testing.T) {
	const oldSecret, newSecret = "old-row-secret", "new-row-secret"
	columns := []*Column{{Name: "value", Type: mysql.TypeVarchar, Value: newSecret}}
	tableInfo := BuildTableInfo("db", "tbl", columns, nil)
	row := &RowChangedEvent{
		StartTs: 1, CommitTs: 2, TableInfo: tableInfo,
		Columns:    Columns2ColumnDatas(columns, tableInfo),
		PreColumns: Columns2ColumnDatas([]*Column{{Name: "value", Type: mysql.TypeVarchar, Value: oldSecret}}, tableInfo),
	}
	for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
		t.Run(mode.String(), func(t *testing.T) {
			output := captureModelRedactionLogs(t, mode)

			deleted, inserted, err := SplitUpdateEvent(row)
			require.NoError(t, err)
			require.True(t, deleted.IsDelete())
			require.True(t, inserted.IsInsert())
			require.Equal(t, oldSecret, deleted.PreColumns[0].Value)
			require.Equal(t, newSecret, inserted.Columns[0].Value)
			var entry map[string]interface{}
			require.NoError(t, json.Unmarshal(output.Bytes(), &entry))
			require.Equal(t, "split update event", entry["msg"])
			require.Equal(t, "db", entry["schema"])
			require.Equal(t, "tbl", entry["table"])
			switch mode {
			case redact.RedactionModeOff:
				require.Equal(t, oldSecret, entry["preCols"].([]interface{})[0].(map[string]interface{})["value"])
				require.Equal(t, newSecret, entry["cols"].([]interface{})[0].(map[string]interface{})["value"])
			case redact.RedactionModeOn:
				require.Equal(t, "?", entry["preCols"])
				require.Equal(t, "?", entry["cols"])
				require.NotContains(t, output.String(), oldSecret)
				require.NotContains(t, output.String(), newSecret)
			case redact.RedactionModeMarker:
				require.Equal(t, redact.String(fmt.Sprint(row.PreColumns)), entry["preCols"])
				require.Equal(t, redact.String(fmt.Sprint(row.Columns)), entry["cols"])
			}
		})
	}
}

func TestSingleTableTxnAppendLogRedaction(t *testing.T) {
	const secret = "unexpected-row-secret"
	columns := []*Column{{Name: "value", Type: mysql.TypeVarchar, Value: secret}}
	tableInfo := BuildTableInfo("db", "tbl", columns, nil)
	row := &RowChangedEvent{StartTs: 2, CommitTs: 3, TableInfo: tableInfo, Columns: Columns2ColumnDatas(columns, tableInfo)}
	for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
		t.Run(mode.String(), func(t *testing.T) {
			output := captureModelRedactionLogs(t, mode)
			txn := &SingleTableTxn{StartTs: 1, CommitTs: 3, TableInfo: tableInfo}
			require.PanicsWithValue(t, "unexpected row change event", func() { txn.Append(row) })
			require.Empty(t, txn.Rows)
			var entry map[string]interface{}
			require.NoError(t, json.Unmarshal(output.Bytes(), &entry))
			switch mode {
			case redact.RedactionModeOff:
				loggedRow := entry["row"].(map[string]interface{})
				require.Equal(t, secret, loggedRow["Columns"].([]interface{})[0].(map[string]interface{})["value"])
			case redact.RedactionModeOn:
				require.Equal(t, "?", entry["row"])
				require.NotContains(t, output.String(), secret)
			case redact.RedactionModeMarker:
				require.Equal(t, redact.String(fmt.Sprint(row)), entry["row"])
			}
		})
	}
}

func TestRedoRowSchemaInvariantLogRedaction(t *testing.T) {
	const secret = "redo-primary-key-secret"
	// Redo restoration must reject a primary-key column without a handle-key flag.
	row := &RowChangedEventInRedoLog{
		Table: &TableName{Schema: "db", Table: "tbl"},
		Columns: []*Column{{
			Name: "value", Type: mysql.TypeVarchar, Flag: PrimaryKeyFlag, Value: secret,
		}},
		IndexColumns: [][]int{{0}},
	}
	for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
		t.Run(mode.String(), func(t *testing.T) {
			output := captureModelRedactionLogs(t, mode)
			require.PanicsWithValue(t, "Primary key must be handle key", func() { row.ToRowChangedEvent() })
			var entry map[string]interface{}
			require.NoError(t, json.Unmarshal(output.Bytes(), &entry))
			require.Equal(t, "tbl", entry["table"])
			switch mode {
			case redact.RedactionModeOff:
				require.Equal(t, secret, entry["columns"].([]interface{})[0].(map[string]interface{})["Value"])
			case redact.RedactionModeOn:
				require.Equal(t, "?", entry["columns"])
				require.NotContains(t, output.String(), secret)
			case redact.RedactionModeMarker:
				require.Equal(t, redact.String(fmt.Sprint(row.Columns)), entry["columns"])
			}
		})
	}
}
