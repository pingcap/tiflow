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

package sqlmodel

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/logutil/redact"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestGenUpdateSQLInvariantLogRedaction(t *testing.T) {
	const secret = "sql-primary-key-secret"
	table := &cdcmodel.TableName{Schema: "db", Table: "tbl"}
	firstInfo := mockTableInfo(t, "CREATE TABLE tbl (a VARCHAR(64) PRIMARY KEY, b VARCHAR(64))")
	secondInfo := mockTableInfo(t, "CREATE TABLE tbl (a VARCHAR(64), b VARCHAR(64), PRIMARY KEY (a, b))")
	first := NewRowChange(table, table, []interface{}{"first", "before"}, []interface{}{"first", "after"}, firstInfo, firstInfo, nil)
	second := NewRowChange(table, table, []interface{}{secret, "before"}, []interface{}{secret, "after"}, secondInfo, secondInfo, nil)
	for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
		t.Run(mode.String(), func(t *testing.T) {
			originalMode := errors.RedactLogEnabled.Load()
			redact.SetRedactionMode(mode)
			t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
			var output bytes.Buffer
			core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&output), zap.DebugLevel)
			t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.DebugLevel)}))

			require.PanicsWithValue(t, "len(whereValues) != len(whereColumns)", func() { GenUpdateSQL(first, second) })
			var entry map[string]interface{}
			require.NoError(t, json.Unmarshal(output.Bytes(), &entry))
			require.Equal(t, "len(whereValues) != len(whereColumns)", entry["msg"])
			require.Equal(t, float64(2), entry["len(whereValues)"])
			require.Equal(t, float64(1), entry["len(whereColumns)"])
			switch mode {
			case redact.RedactionModeOff:
				require.Equal(t, []interface{}{secret, "before"}, entry["whereValues"])
			case redact.RedactionModeOn:
				require.Equal(t, "?", entry["whereValues"])
				require.NotContains(t, output.String(), secret)
			case redact.RedactionModeMarker:
				require.Equal(t, "‹["+secret+" before]›", entry["whereValues"])
			}
		})
	}
}
