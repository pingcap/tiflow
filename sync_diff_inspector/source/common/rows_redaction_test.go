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

package common

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestRowDataFatalRedaction(t *testing.T) {
	const sensitiveValue = "private-row-value"
	for _, tc := range []struct {
		name       string
		row        int
		missingCol bool
	}{
		{name: "missing-left-column", row: 0, missingCol: true},
		{name: "missing-right-column", row: 1, missingCol: true},
		{name: "invalid-left-number", row: 0},
		{name: "invalid-right-number", row: 1},
	} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/redact=%t", tc.name, enabled), func(t *testing.T) {
				previous := dmlog.IsRedactLogEnabled()
				dmlog.SetRedactLog(enabled)
				t.Cleanup(func() { dmlog.SetRedactLog(previous) })

				var output bytes.Buffer
				encoder := zapcore.NewJSONEncoder(zapcore.EncoderConfig{
					MessageKey:  "message",
					LevelKey:    "level",
					EncodeLevel: zapcore.LowercaseLevelEncoder,
				})
				core := zapcore.NewCore(encoder, zapcore.AddSync(&output), zap.FatalLevel)
				// Keep the fatal call's control-flow interruption without exiting
				// the test process, and inspect the actual encoded log entry.
				logger := zap.New(core, zap.WithFatalHook(zapcore.WriteThenPanic))
				t.Cleanup(log.ReplaceGlobals(logger, &log.ZapProperties{}))

				rows := RowDatas{
					Rows: []RowData{
						{Data: map[string]*dbutil.ColumnData{"id": {Data: []byte("1")}}},
						{Data: map[string]*dbutil.ColumnData{"id": {Data: []byte("2")}}},
					},
					OrderKeyCols: []*model.ColumnInfo{{
						Name:      ast.NewCIStr("id"),
						FieldType: *types.NewFieldType(mysql.TypeLong),
					}},
				}
				message := "convert string to float failed"
				if tc.missingCol {
					message = "data don't have column"
					rows.Rows[tc.row].Data = map[string]*dbutil.ColumnData{
						"payload": {Data: []byte(sensitiveValue)},
					}
				} else {
					rows.Rows[tc.row].Data["id"].Data = []byte(sensitiveValue)
				}
				require.PanicsWithValue(t, message, func() { rows.Less(0, 1) })

				var fields map[string]any
				require.NoError(t, json.Unmarshal(output.Bytes(), &fields))
				require.Equal(t, "fatal", fields["level"])
				require.Equal(t, message, fields["message"])
				require.Equal(t, "id", fields["column"])
				if enabled {
					require.Equal(t, "?", fields["data"])
					require.NotContains(t, output.String(), sensitiveValue)
					require.NotContains(t, output.String(), base64.StdEncoding.EncodeToString([]byte(sensitiveValue)))
					if !tc.missingCol {
						require.Equal(t, "?", fields["error"])
					}
				} else if tc.missingCol {
					require.Equal(t, map[string]any{
						"payload": map[string]any{
							"Data":   base64.StdEncoding.EncodeToString([]byte(sensitiveValue)),
							"IsNull": false,
						},
					}, fields["data"])
				} else {
					require.Equal(t, sensitiveValue, fields["data"])
					_, err := strconv.ParseFloat(sensitiveValue, 64)
					require.Equal(t, err.Error(), fields["error"])
				}
				require.NotContains(t, fields, "errorVerbose")
			})
		}
	}
}
