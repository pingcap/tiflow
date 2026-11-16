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

package avro

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/linkedin/goavro/v2"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/logutil/redact"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func captureAvroRedactionLogs(t *testing.T, mode redact.RedactionMode) *bytes.Buffer {
	t.Helper()
	originalMode := errors.RedactLogEnabled.Load()
	redact.SetRedactionMode(mode)
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
	output := &bytes.Buffer{}
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(output), zap.DebugLevel)
	t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.DebugLevel)}))
	return output
}

func avroLogEntry(t *testing.T, output *bytes.Buffer, message string) map[string]interface{} {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		var entry map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		if entry["msg"] == message {
			return entry
		}
	}
	t.Fatalf("log entry %q missing from %s", message, output.String())
	return nil
}

// Avoid an external schema registry while exercising the normal encoder paths.
type redactionSchemaManager struct {
	SchemaManager
	err error
}

func (m redactionSchemaManager) GetCachedOrRegister(_ context.Context, _ string, _ uint64, schemaGen SchemaGenerator) (*goavro.Codec, []byte, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	schema, err := schemaGen()
	if err != nil {
		return nil, nil, err
	}
	codec, err := goavro.NewCodec(schema)
	return codec, nil, err
}

func TestAvroRowDataLogRedaction(t *testing.T) {
	const secret = "avro-row-secret"
	columns := []*model.Column{{Name: "value", Type: mysql.TypeVarchar, Value: secret}}
	tableInfo := model.BuildTableInfo("db", "tbl", columns, nil)
	input := avroEncodeInput{
		TableInfo: tableInfo,
		columns:   model.Columns2ColumnDatas(columns, tableInfo),
		colInfos:  tableInfo.GetColInfosForRowChangedEvent(),
	}
	for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
		t.Run(mode.String(), func(t *testing.T) {
			output := captureAvroRedactionLogs(t, mode)
			encoder := NewAvroEncoder(model.DefaultNamespace, nil, common.NewConfig(config.ProtocolAvro)).(*BatchEncoder)
			data, err := encoder.columns2AvroData(input)
			require.NoError(t, err)
			require.Equal(t, map[string]interface{}{"value": secret}, data)
			entry := avroLogEntry(t, output, "rowToAvroData")
			switch mode {
			case redact.RedactionModeOff:
				require.Equal(t, data, entry["data"])
			case redact.RedactionModeOn:
				require.Equal(t, "?", entry["data"])
				require.NotContains(t, output.String(), secret)
			case redact.RedactionModeMarker:
				require.Equal(t, "‹map[value:"+secret+"]›", entry["data"])
			}
		})
	}
}

func TestAvroEncodeFailureLogRedaction(t *testing.T) {
	const secret = "avro-encode-error-secret"
	for _, keyFailure := range []bool{true, false} {
		for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
			t.Run(fmt.Sprintf("key=%t/%s", keyFailure, mode), func(t *testing.T) {
				output := captureAvroRedactionLogs(t, mode)
				column := &model.Column{Name: "value", Type: mysql.TypeVarchar, Value: secret}
				var indices [][]int
				if keyFailure {
					column.Flag = model.HandleKeyFlag | model.PrimaryKeyFlag
					indices = [][]int{{0}}
				}
				columns := []*model.Column{column}
				tableInfo := model.BuildTableInfo("db", "tbl", columns, indices)
				row := &model.RowChangedEvent{TableInfo: tableInfo, Columns: model.Columns2ColumnDatas(columns, tableInfo)}
				registryErr := errors.New("schema registry unavailable")
				encoder := NewAvroEncoder(model.DefaultNamespace, redactionSchemaManager{err: registryErr}, common.NewConfig(config.ProtocolAvro))
				callbacks := 0
				err := encoder.AppendRowChangedEvent(context.Background(), "topic", row, func() { callbacks++ })
				require.Same(t, registryErr, errors.Cause(err))
				require.Empty(t, encoder.Build())
				require.Zero(t, callbacks)
				message := "avro encoding value failed"
				if keyFailure {
					message = "avro encoding key failed"
				}
				entry := avroLogEntry(t, output, message)
				switch mode {
				case redact.RedactionModeOff:
					loggedEvent := entry["event"].(map[string]interface{})
					require.Equal(t, secret, loggedEvent["Columns"].([]interface{})[0].(map[string]interface{})["value"])
				case redact.RedactionModeOn:
					require.Equal(t, "?", entry["event"])
					require.NotContains(t, output.String(), secret)
				case redact.RedactionModeMarker:
					require.Equal(t, redact.String(fmt.Sprint(row)), entry["event"])
				}
			})
		}
	}
}

func TestAvroYearParseErrorRedaction(t *testing.T) {
	const secret = "invalid-year-secret"
	columns := []*model.Column{{Name: "value", Type: mysql.TypeYear, Value: secret}}
	tableInfo := model.BuildTableInfo("db", "tbl", columns, nil)
	row := &model.RowChangedEvent{TableInfo: tableInfo, Columns: model.Columns2ColumnDatas(columns, tableInfo)}
	for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
		t.Run(mode.String(), func(t *testing.T) {
			output := captureAvroRedactionLogs(t, mode)
			encoder := NewAvroEncoder(model.DefaultNamespace, redactionSchemaManager{}, common.NewConfig(config.ProtocolAvro))
			err := encoder.AppendRowChangedEvent(context.Background(), "topic", row, func() {})
			require.Error(t, err)
			var numErr *strconv.NumError
			require.ErrorAs(t, err, &numErr)
			require.Same(t, strconv.ErrSyntax, numErr.Err)
			require.Equal(t, "ParseInt", numErr.Func)
			require.Equal(t, secret, row.Columns[0].Value)
			entry := avroLogEntry(t, output, "avro encoder parse year value failed")
			switch mode {
			case redact.RedactionModeOff:
				require.Equal(t, secret, entry["value"])
				require.Equal(t, secret, numErr.Num)
				require.Contains(t, entry["error"], secret)
			case redact.RedactionModeOn:
				require.Equal(t, "?", entry["value"])
				require.Equal(t, "?", numErr.Num)
				require.NotContains(t, output.String(), secret)
				require.NotContains(t, err.Error(), secret)
			case redact.RedactionModeMarker:
				require.Equal(t, "‹"+secret+"›", entry["value"])
				require.Equal(t, "‹"+secret+"›", numErr.Num)
				require.Contains(t, entry["error"], "‹"+secret+"›")
			}
		})
	}
}

func TestAvroUnknownTypeLogRedaction(t *testing.T) {
	const secret = "unknown-avro-type-secret"
	columns := []*model.Column{{Name: "value", Type: mysql.TypeUnspecified, Value: secret}}
	tableInfo := model.BuildTableInfo("db", "tbl", columns, nil)
	input := avroEncodeInput{TableInfo: tableInfo, columns: model.Columns2ColumnDatas(columns, tableInfo)}
	for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
		t.Run(mode.String(), func(t *testing.T) {
			output := captureAvroRedactionLogs(t, mode)
			encoder := NewAvroEncoder(model.DefaultNamespace, nil, common.NewConfig(config.ProtocolAvro)).(*BatchEncoder)
			_, err := encoder.columns2AvroData(input)
			require.Error(t, err)
			entry := avroLogEntry(t, output, "unknown mysql type")
			require.Equal(t, float64(mysql.TypeUnspecified), entry["mysqlType"])
			switch mode {
			case redact.RedactionModeOff:
				require.Equal(t, secret, entry["value"])
			case redact.RedactionModeOn:
				require.Equal(t, "?", entry["value"])
				require.NotContains(t, output.String(), secret)
			case redact.RedactionModeMarker:
				require.Equal(t, "‹"+secret+"›", entry["value"])
			}
		})
	}
}
