// Copyright 2022 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestContextWithLogger(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()
	require.Equal(t, log.L(), FromContext(ctx), log.L())

	logger := log.L().With(zap.String("inner", "logger"))
	ctx = NewContextWithLogger(ctx, logger)
	require.Equal(t, log.L().With(zap.String("inner", "logger")), FromContext(ctx))
}
