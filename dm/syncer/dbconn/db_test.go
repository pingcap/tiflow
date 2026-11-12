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
// See the License for the specific language governing permissions and
// limitations under the License.

package dbconn

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"

	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	dmlog "github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestResetConnRetryableFnRedactsArguments(t *testing.T) {
	originalRedaction := dmlog.IsRedactLogEnabled()
	t.Cleanup(func() { dmlog.SetRedactLog(originalRedaction) })

	for _, tc := range []struct {
		name      string
		redact    bool
		arguments string
	}{
		{name: "disabled", arguments: "[[sensitive-value 42]]"},
		{name: "enabled", redact: true, arguments: "?"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dmlog.SetRedactLog(tc.redact)
			core, logs := observer.New(zap.ErrorLevel)
			tctx := tcontext.NewContext(context.Background(), dmlog.Logger{Logger: zap.New(core)})
			isRetryable := resetConnRetryableFn(tctx,
				[]string{"INSERT INTO test (value) VALUES (?)"},
				[][]interface{}{{"sensitive-value", 42}})

			require.True(t, isRetryable(3, driver.ErrBadConn))
			entries := logs.All()
			require.Len(t, entries, 1)
			require.Equal(t, "retrying reset connection", entries[0].Message)
			fields := entries[0].ContextMap()
			require.Equal(t, tc.arguments, fields["arguments"])
			require.Equal(t, "[INSERT INTO test (value) VALUES (?)]", fields["queries"])
			require.Equal(t, int64(3), fields["retry"])
			require.Equal(t, int64(3), fields["resetRetries"])

			require.False(t, isRetryable(3, errors.New("non-retryable error")))
			require.Equal(t, 1, logs.Len())
		})
	}
}
