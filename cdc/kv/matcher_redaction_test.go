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

package kv

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/logutil/redact"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMatchCachedRowLogRedaction(t *testing.T) {
	key := []byte("unmatched-primary-key-secret")
	encodedKey := base64.StdEncoding.EncodeToString(key)
	for _, mode := range []redact.RedactionMode{redact.RedactionModeOff, redact.RedactionModeOn, redact.RedactionModeMarker} {
		t.Run(mode.String(), func(t *testing.T) {
			originalMode := errors.RedactLogEnabled.Load()
			redact.SetRedactionMode(mode)
			t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })
			var output bytes.Buffer
			core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&output), zap.DebugLevel)
			t.Cleanup(log.ReplaceGlobals(zap.New(core), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.DebugLevel)}))

			matcher := newMatcher()
			matcher.cacheCommitRow(&cdcpb.Event_Row{StartTs: 1, CommitTs: 2, Key: key})
			require.Empty(t, matcher.matchCachedRow(true))
			require.Empty(t, matcher.cachedCommit)
			var entry map[string]interface{}
			require.NoError(t, json.Unmarshal(output.Bytes(), &entry))
			require.Equal(t, "ignore commit event without prewrite", entry["msg"])
			require.Equal(t, float64(1), entry["startTs"])
			switch mode {
			case redact.RedactionModeOff:
				require.Equal(t, encodedKey, entry["key"])
			case redact.RedactionModeOn:
				require.Equal(t, "?", entry["key"])
				require.NotContains(t, output.String(), string(key))
				require.NotContains(t, output.String(), encodedKey)
				require.NotContains(t, output.String(), fmt.Sprint(key))
			case redact.RedactionModeMarker:
				require.Equal(t, redact.String(fmt.Sprint(key)), entry["key"])
			}
		})
	}
}
