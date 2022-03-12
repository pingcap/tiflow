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
	sdtContext "context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/processor/pipeline/system"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestContext(t *testing.T) {
	t.Parallel()
	ctx := NewContext(sdtContext.TODO(), nil, 1, &context.ChangefeedVars{ID: "zzz"}, &context.GlobalVars{})
	require.NotNil(t, ctx.GlobalVars())
	require.Equal(t, "zzz", ctx.ChangefeedVars().ID)
	require.Equal(t, actor.ID(1), ctx.tableActorID)
	ctx.SendToNextNode(pipeline.BarrierMessage(1))
	require.Equal(t, int32(1), ctx.noTickMessageCount)
	wait(t, 500*time.Millisecond, func() {
		msg := ctx.Message()
		require.Equal(t, pipeline.MessageTypeBarrier, msg.Tp)
	})
}

func TestTryGetProcessedMessageFromChan(t *testing.T) {
	t.Parallel()
	ctx := NewContext(sdtContext.TODO(), nil, 1, nil, nil)
	ctx.outputCh = make(chan pipeline.Message, 1)
	require.Nil(t, ctx.tryGetProcessedMessage())
	ctx.outputCh <- pipeline.TickMessage()
	require.NotNil(t, ctx.tryGetProcessedMessage())
	close(ctx.outputCh)
	require.Nil(t, ctx.tryGetProcessedMessage())
}

func TestThrow(t *testing.T) {
	t.Parallel()
	ctx, cancel := sdtContext.WithCancel(sdtContext.TODO())
	sys := system.NewSystem()
	defer func() {
		cancel()
		require.Nil(t, sys.Stop())
	}()

	require.Nil(t, sys.Start(ctx))
	actorID := sys.ActorID("abc", 1)
	mb := actor.NewMailbox(actorID, defaultOutputChannelSize)
	ch := make(chan message.Message, defaultOutputChannelSize)
	fa := &forwardActor{ch: ch}
	require.Nil(t, sys.System().Spawn(mb, fa))
	actorContext := NewContext(ctx, sys.Router(), actorID, nil, nil)
	actorContext.Throw(nil)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, len(ch))
	actorContext.Throw(errors.New("error"))
	tick := time.After(500 * time.Millisecond)
	select {
	case <-tick:
		t.Fatal("timeout")
	case m := <-ch:
		require.Equal(t, message.TypeStop, m.Tp)
	}
}

func TestActorNodeContextTrySendToNextNode(t *testing.T) {
	t.Parallel()
	ctx := NewContext(sdtContext.TODO(), nil, 1, &context.ChangefeedVars{ID: "zzz"}, &context.GlobalVars{})
	ctx.outputCh = make(chan pipeline.Message, 1)
	require.True(t, ctx.TrySendToNextNode(pipeline.BarrierMessage(1)))
	require.False(t, ctx.TrySendToNextNode(pipeline.BarrierMessage(1)))
	ctx.outputCh = make(chan pipeline.Message, 1)
	close(ctx.outputCh)
	require.Panics(t, func() { ctx.TrySendToNextNode(pipeline.BarrierMessage(1)) })
}

func TestSendToNextNodeNoTickMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := sdtContext.WithCancel(sdtContext.TODO())
	sys := system.NewSystem()
	defer func() {
		cancel()
		require.Nil(t, sys.Stop())
	}()

	require.Nil(t, sys.Start(ctx))
	actorID := sys.ActorID("abc", 1)
	mb := actor.NewMailbox(actorID, defaultOutputChannelSize)
	ch := make(chan message.Message, defaultOutputChannelSize)
	fa := &forwardActor{ch: ch}
	require.Nil(t, sys.System().Spawn(mb, fa))
	actorContext := NewContext(ctx, sys.Router(), actorID, nil, nil)
	actorContext.setTickMessageThreshold(2)
	actorContext.SendToNextNode(pipeline.BarrierMessage(1))
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, len(ch))
	actorContext.SendToNextNode(pipeline.BarrierMessage(2))
	tick := time.After(500 * time.Millisecond)
	select {
	case <-tick:
		t.Fatal("timeout")
	case m := <-ch:
		require.Equal(t, message.TypeTick, m.Tp)
	}
	actorContext.SendToNextNode(pipeline.BarrierMessage(1))
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, len(ch))
}

type forwardActor struct {
	contextAware bool

	ch chan<- message.Message
}

func (f *forwardActor) Poll(ctx sdtContext.Context, msgs []message.Message) bool {
	for _, msg := range msgs {
		if f.contextAware {
			select {
			case f.ch <- msg:
			case <-ctx.Done():
			}
		} else {
			f.ch <- msg
		}
	}
	return true
}

func (f *forwardActor) OnClose() {}

func wait(t *testing.T, timeout time.Duration, f func()) {
	wait := make(chan int)
	go func() {
		f()
		wait <- 0
	}()
	select {
	case <-wait:
	case <-time.After(timeout):
		t.Fatal("Timed out")
	}
}
