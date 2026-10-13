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

package config

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/dm/pb"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestGetDMTaskCfgRedaction(t *testing.T) {
	const encryptedPassword = "/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs="
	const taskConfig = `name = "test"
source-id = "mysql-replica-01"
[from]
host = "127.0.0.1"
password = "/Q7B9DizNLLTTfiZHv9WoEAKamfpIUs="
[to]
host = "127.0.0.2"
password = "private-password"
[[mapping-rule]]
schema-pattern = "test"
table-pattern = "t"
target-column = "c"
expression = "add-prefix"
arguments = ["private-argument"]
create-table-query = "CREATE TABLE t (c VARCHAR(100) DEFAULT 'private-default')"
`
	body, err := json.Marshal(&pb.GetSubTaskCfgResponse{Result: true, Cfgs: []string{taskConfig}})
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)

	buffer := new(zaptest.Buffer)
	level := zap.NewAtomicLevelAt(zap.InfoLevel)
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), buffer, level)
	t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Core: core, Syncer: buffer, Level: level}))
	original := dmlog.IsRedactLogEnabled()
	t.Cleanup(func() { dmlog.SetRedactLog(original) })

	for _, redact := range []bool{false, true} {
		t.Run(fmt.Sprintf("redact=%t", redact), func(t *testing.T) {
			dmlog.SetRedactLog(redact)
			buffer.Reset()
			cfgs, err := getDMTaskCfg(server.URL, "test")
			require.NoError(t, err)
			require.Len(t, cfgs, 1)
			require.Equal(t, "mysql-replica-01", cfgs[0].SourceID)
			require.Equal(t, DecryptOrPlaintext(encryptedPassword), cfgs[0].From.Password)
			require.NotEqual(t, encryptedPassword, cfgs[0].From.Password)
			require.Equal(t, "private-password", cfgs[0].To.Password)
			require.Len(t, cfgs[0].ColumnMappingRules, 1)
			rule := cfgs[0].ColumnMappingRules[0]
			require.Equal(t, []string{"private-argument"}, rule.Arguments)
			require.Equal(t, "CREATE TABLE t (c VARCHAR(100) DEFAULT 'private-default')", rule.CreateTableQuery)

			require.Len(t, buffer.Lines(), 1)
			var entry map[string]any
			require.NoError(t, json.Unmarshal([]byte(buffer.Lines()[0]), &entry))
			require.Equal(t, "dm sub task configs", entry["msg"])
			require.Equal(t, "info", entry["level"])
			require.NotContains(t, buffer.Stripped(), "private-password")
			require.NotContains(t, buffer.Stripped(), encryptedPassword)
			if redact {
				require.Equal(t, "?", entry["cfgs"])
				require.NotContains(t, buffer.Stripped(), "private-argument")
				require.NotContains(t, buffer.Stripped(), "private-default")
			} else {
				encoded, err := json.Marshal(cfgs)
				require.NoError(t, err)
				var expected any
				require.NoError(t, json.Unmarshal(encoded, &expected))
				require.Equal(t, expected, entry["cfgs"])
				require.Contains(t, buffer.Stripped(), "private-argument")
				require.Contains(t, buffer.Stripped(), "private-default")
			}
		})
	}
}
