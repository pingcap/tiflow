// Copyright 2020 PingCAP, Inc.
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

package logutil

import (
	"context"
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestInitLoggerAndSetLogLevel(t *testing.T) {
	cfg := &Config{
		Level: "warning",
	}

	cfg.Adjust()
	err := InitLogger(cfg)
	require.Nil(t, err)
	require.Equal(t, log.GetLevel(), zapcore.WarnLevel)

	// Set a different level.
	err = SetLogLevel("info")
	require.Nil(t, err)
	require.Equal(t, log.GetLevel(), zapcore.InfoLevel)

	// Set the same level.
	err = SetLogLevel("info")
	require.Nil(t, err)
	require.Equal(t, log.GetLevel(), zapcore.InfoLevel)

	// Set an invalid level.
	err = SetLogLevel("badlevel")
	require.NotNil(t, err)
}

func TestZapErrorFilter(t *testing.T) {
	var (
		err       = errors.New("test error")
		testCases = []struct {
			err      error
			filters  []error
			expected zap.Field
		}{
			{nil, []error{}, zap.Error(nil)},
			{err, []error{}, zap.Error(err)},
			{err, []error{context.Canceled}, zap.Error(err)},
			{err, []error{err}, zap.Error(nil)},
			{context.Canceled, []error{context.Canceled}, zap.Error(nil)},
			{errors.Annotate(context.Canceled, "annotate error"), []error{context.Canceled}, zap.Error(nil)},
		}
	)
	for _, tc := range testCases {
		require.EqualValues(t, ZapErrorFilter(tc.err, tc.filters...), tc.expected)
		// c.Assert(ZapErrorFilter(tc.err, tc.filters...), check.DeepEquals, tc.expected)
	}
}

func TestTimeoutWarning(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	rescueStdout := os.Stdout
	r, w, err := os.Pipe()
	require.Nil(t, err)
	os.Stdout = w

	cfg := &Config{
		Level: defaultLogLevel,
	}
	InitLogger(cfg)

	func() {
		defer TimeoutWarning(time.Now(), 1)
		time.Sleep(2 * time.Second)
	}()

	w.Close()
	out, err := ioutil.ReadAll(r)
	require.Nil(t, err)
	require.Contains(t, string(out), "TestTimeoutWarning")
	os.Stdout = rescueStdout
}
