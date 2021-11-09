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

	"github.com/pingcap/ticdc/pkg/actor"
	cdcContext "github.com/pingcap/ticdc/pkg/context"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestTryHandleDataMessage(t *testing.T) {
	puller := &pullerNode{tableActorRouter: &actor.Router{}, outputCh: make(chan pipeline.Message)}
	puller.isTableActorMode = true
	ok, err := puller.TryHandleDataMessage(context.TODO(), pipeline.BarrierMessage(1))
	require.Nil(t, err)
	require.False(t, ok)
	puller.outputCh = make(chan pipeline.Message, 2)
	ok, err = puller.TryHandleDataMessage(context.TODO(), pipeline.BarrierMessage(1))
	require.Nil(t, err)
	require.True(t, ok)

	puller.isTableActorMode = false
	ctx := cdcContext.NewContext(context.Background(), &cdcContext.GlobalVars{})
	ok, err = puller.TryHandleDataMessage(pipeline.MockNodeContext4Test(ctx, pipeline.TickMessage(), make(chan pipeline.Message, 4)), pipeline.TickMessage())
	require.Nil(t, err)
	require.True(t, ok)
}
