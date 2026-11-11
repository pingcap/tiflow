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

package logutil

import (
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
	tikvredact "github.com/tikv/client-go/v2/util/redact"
	"go.uber.org/zap/zapcore"
)

func TestInitLogRedactionControlsDependencies(t *testing.T) {
	originalMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(originalMode) })

	const sensitiveValue = "customer‹›"
	for _, tc := range []struct {
		mode       RedactInfoLogType
		sharedMode string
		wantValue  string
		wantKey    string
	}{
		{RedactInfoLogOFF, errors.RedactLogDisable, sensitiveValue, "736563726574"},
		{RedactInfoLogON, errors.RedactLogEnable, "?", "?"},
		{RedactInfoLogMarker, errors.RedactLogMarker, "‹customer‹‹›››", "?"},
	} {
		t.Run(tc.mode.String(), func(t *testing.T) {
			require.NoError(t, InitLogRedactionFromType(tc.mode))
			require.Equal(t, tc.sharedMode, errors.RedactLogEnabled.Load())

			enc := zapcore.NewMapObjectEncoder()
			ZapRedactString("value", sensitiveValue).AddTo(enc)
			require.Equal(t, tc.wantValue, enc.Fields["value"])

			// TiKV's existing key helper suppresses values in both ON and MARKER.
			require.Equal(t, tc.wantKey, tikvredact.Key([]byte("secret")))

			// TiDB annotates the incorrect value while preserving its type and error code.
			err := types.ErrWrongValue.GenWithStackByArgs("datetime", sensitiveValue)
			require.Contains(t, err.Error(), "Incorrect datetime value: '"+tc.wantValue+"'")
			if tc.mode == RedactInfoLogON {
				require.NotContains(t, err.Error(), sensitiveValue)
			}
		})
	}
}
