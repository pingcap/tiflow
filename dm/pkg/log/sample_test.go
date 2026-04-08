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

package log

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewRetrySampleLogger(t *testing.T) {
	core, observed := observer.New(zap.InfoLevel)
	base := Logger{zap.New(core).With(zap.String("base", "logger"))}
	logger := NewRetrySampleLogger(base, zap.String("component", "retry-unit-test"))

	logger.Error("retryable operation failed", zap.Int("retryNum", 1))
	logger.Error("retryable operation failed", zap.Int("retryNum", 2))
	logger.Warn("retryable operation failed", zap.Int("retryNum", 3))
	logger.Error("another retryable operation failed", zap.Int("retryNum", 4))

	entries := observed.All()
	require.Len(t, entries, 3)

	require.Equal(t, "retryable operation failed", entries[0].Message)
	require.Equal(t, zap.ErrorLevel, entries[0].Level)
	require.Equal(t, map[string]any{
		"base":      "logger",
		"component": "retry-unit-test",
		"retryNum":  int64(1),
		"sampled":   "",
	}, entries[0].ContextMap())

	require.Equal(t, "retryable operation failed", entries[1].Message)
	require.Equal(t, zap.WarnLevel, entries[1].Level)

	require.Equal(t, "another retryable operation failed", entries[2].Message)
	require.Equal(t, zap.ErrorLevel, entries[2].Level)
}
