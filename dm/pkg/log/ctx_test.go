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

package log

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLogCtxBasics(t *testing.T) {
	ctx := context.Background()
	ctx1 := AppendZapFieldToCtx(ctx, zap.String("key1", "value1"))
	ctx2 := AppendZapFieldToCtx(ctx1, zap.String("key2", "value2"))

	require.Nil(t, getZapFieldsFromCtx(ctx))
	require.Equal(t, []zap.Field{zap.String("key1", "value1")}, getZapFieldsFromCtx(ctx1))
	require.Equal(t, []zap.Field{
		zap.String("key1", "value1"),
		zap.String("key2", "value2"),
	}, getZapFieldsFromCtx(ctx2))
}

func TestLogCtxTooManyFields(t *testing.T) {
	ctx := context.Background()
	var (
		ctxs   []context.Context
		fields [][]zap.Field
	)

	ctxs = append(ctxs, ctx)
	fields = append(fields, nil)
	for i := 1; i < 100; i++ {
		k := fmt.Sprintf("key%d", i)
		v := fmt.Sprintf("value%d", i)
		ctxs = append(ctxs, AppendZapFieldToCtx(ctxs[i-1], zap.String(k, v)))
		fields = append(fields, append(fields[i-1], zap.String(k, v)))
	}

	for i := 0; i < 100; i++ {
		require.Equal(t, fields[i], getZapFieldsFromCtx(ctxs[i]), "failed, index = %d", i)
	}
}
