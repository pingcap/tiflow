// Copyright 2024 PingCAP, Inc.
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

package pulsar

import (
	"errors"
	"runtime"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar/log"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// TestPulsarLog ensures Pulsar's use of our logger behaves as expected of a typical zap logger:
//  1. all fields injected via .With*() are present in the final log
//  2. the [file:line] points to the actual call site rather than `logger.go`.
func TestPulsarLog(t *testing.T) {
	core, observedLogs := observer.New(zapcore.DebugLevel)
	logger := NewPulsarLogger(zap.New(core).WithOptions(zap.AddCaller()))

	pc, file, line, ok := runtime.Caller(0)
	assert.True(t, ok)
	functionName := runtime.FuncForPC(pc).Name()

	logger.Infof("1 + 2 = %d", 1+2)
	logger.WithError(errors.ErrUnsupported).Warn("3", "+", "4", "=", 3+4)
	logger.WithFields(log.Fields{"x": 1234, "y": 9.75}).Info("connected")

	allEntries := observedLogs.AllUntimed()
	// we can't reliably test the `pc` address of the caller information. remove them.
	for i := range allEntries {
		allEntries[i].Caller.PC = 0
	}

	assert.Len(t, allEntries, 3)
	assert.Equal(t, allEntries[0:2], []observer.LoggedEntry{
		{
			Entry: zapcore.Entry{
				Level:   zapcore.InfoLevel,
				Message: "1 + 2 = 3",
				Caller: zapcore.EntryCaller{
					Defined:  true,
					File:     file,
					Line:     line + 4,
					Function: functionName,
				},
			},
			Context: []zap.Field{},
		},
		{
			Entry: zapcore.Entry{
				Level:   zapcore.WarnLevel,
				Message: "3+4=7",
				Caller: zapcore.EntryCaller{
					Defined:  true,
					File:     file,
					Line:     line + 5,
					Function: functionName,
				},
			},
			Context: []zap.Field{zap.Error(errors.ErrUnsupported)},
		},
	})
	assert.Equal(t, allEntries[2].Entry, zapcore.Entry{
		Level:   zapcore.InfoLevel,
		Message: "connected",
		Caller: zapcore.EntryCaller{
			Defined:  true,
			File:     file,
			Line:     line + 6,
			Function: functionName,
		},
	})
	assert.ElementsMatch(t, allEntries[2].Context, []zap.Field{
		zap.Int("x", 1234),
		zap.Float64("y", 9.75),
	})
}
