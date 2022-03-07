// Copyright 2021 PingCAP, Inc.
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

package pipeline

import (
	"context"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestTryRun(t *testing.T) {
	var pN asyncMessageHolderFunc = func() *pipeline.Message { return nil }
	var dp asyncMessageProcessorFunc = func(ctx context.Context, msg pipeline.Message) (bool, error) {
		return false, errors.New("error")
	}
	n := NewActorNode(pN, dp)
	require.Nil(t, n.TryRun(context.TODO()))
	require.Nil(t, n.messageStash)
	// process failed
	pN = func() *pipeline.Message {
		return &pipeline.Message{
			Tp:        pipeline.MessageTypeBarrier,
			BarrierTs: 1,
		}
	}
	n = NewActorNode(pN, dp)
	require.NotNil(t, n.TryRun(context.TODO()))
	require.NotNil(t, n.messageStash)
	require.Equal(t, pipeline.MessageTypeBarrier, n.messageStash.Tp)
	require.Equal(t, model.Ts(1), n.messageStash.BarrierTs)
	// data process is blocked
	dp = func(ctx context.Context, msg pipeline.Message) (bool, error) {
		return false, nil
	}
	n.messageProcessor = dp
	require.Nil(t, n.TryRun(context.TODO()))
	require.NotNil(t, n.messageStash)
	require.Equal(t, pipeline.MessageTypeBarrier, n.messageStash.Tp)
	require.Equal(t, model.Ts(1), n.messageStash.BarrierTs)

	// data process is ok
	dp = func(ctx context.Context, msg pipeline.Message) (bool, error) { return true, nil }
	msg := 0
	pN = func() *pipeline.Message {
		if msg > 0 {
			return nil
		}
		msg++
		return &pipeline.Message{
			Tp:        pipeline.MessageTypeBarrier,
			BarrierTs: 1,
		}
	}
	n = NewActorNode(pN, dp)
	n.parentNode = pN
	n.messageProcessor = dp
	require.Nil(t, n.TryRun(context.TODO()))
	require.Nil(t, n.messageStash)
}
