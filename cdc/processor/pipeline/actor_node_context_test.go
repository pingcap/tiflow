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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/pipeline/system"
	"github.com/pingcap/tiflow/pkg/actor"
	"github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/context"
	pmessage "github.com/pingcap/tiflow/pkg/pipeline/message"
	"github.com/stretchr/testify/require"
)

func TestContext(t *testing.T) {
	t.Parallel()
	ctx := newContext(sdtContext.TODO(), t.Name(), nil, 1,
		&context.ChangefeedVars{
			ID:   model.DefaultChangeFeedID("zzz"),
			Info: &model.ChangeFeedInfo{},
		},
		&context.GlobalVars{}, throwDoNothing)
	require.NotNil(t, ctx.GlobalVars())
	require.Equal(t, "zzz", ctx.ChangefeedVars().ID.ID)
	require.Equal(t, actor.ID(1), ctx.tableActorID)
	ctx.SendToNextNode(pmessage.BarrierMessage(1))
	require.Equal(t, uint32(1), ctx.eventCount)
	wait(t, 500*time.Millisecond, func() {
		msg := ctx.Message()
		require.Equal(t, pmessage.MessageTypeBarrier, msg.Tp)
	})
}

func TestTryGetProcessedMessageFromChan(t *testing.T) {
	t.Parallel()
	ctx := newContext(sdtContext.TODO(), t.Name(), nil, 1, nil, nil, throwDoNothing)
	ctx.outputCh = make(chan pmessage.Message, 1)
	require.Nil(t, ctx.tryGetProcessedMessage())
	ctx.outputCh <- pmessage.TickMessage()
	require.NotNil(t, ctx.tryGetProcessedMessage())
	close(ctx.outputCh)
	require.Nil(t, ctx.tryGetProcessedMessage())
}

func TestThrow(t *testing.T) {
	t.Parallel()
	a := 0
	tf := func(err error) { a++ }
	actorContext := newContext(sdtContext.TODO(), t.Name(), nil, 1, nil, nil, tf)
	actorContext.Throw(nil)
	require.Equal(t, 1, a)
	actorContext.Throw(errors.New("error"))
	require.Equal(t, 2, a)
}

func TestActorNodeContextTrySendToNextNode(t *testing.T) {
	t.Parallel()
	ctx := newContext(sdtContext.TODO(), t.Name(), nil, 1,
		&context.ChangefeedVars{ID: model.DefaultChangeFeedID("zzz")},
		&context.GlobalVars{}, throwDoNothing)
	ctx.outputCh = make(chan pmessage.Message, 1)
	require.True(t, ctx.TrySendToNextNode(pmessage.BarrierMessage(1)))
	require.False(t, ctx.TrySendToNextNode(pmessage.BarrierMessage(1)))
	ctx.outputCh = make(chan pmessage.Message, 1)
	close(ctx.outputCh)
	require.Panics(t, func() { ctx.TrySendToNextNode(pmessage.BarrierMessage(1)) })
}

func TestSendToNextNodeNoTickMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := sdtContext.WithCancel(sdtContext.TODO())
	sys := system.NewSystem()
	defer func() {
		cancel()
		sys.Stop()
	}()

	require.Nil(t, sys.Start(ctx))
	actorID := sys.ActorID()
	mb := actor.NewMailbox[pmessage.Message](actorID, defaultOutputChannelSize)
	ch := make(chan message.Message[pmessage.Message], defaultOutputChannelSize)
	fa := &forwardActor{ch: ch}
	require.Nil(t, sys.System().Spawn(mb, fa))
	actorContext := newContext(ctx, t.Name(), sys.Router(), actorID,
		&context.ChangefeedVars{ID: model.DefaultChangeFeedID("abc")},
		&context.GlobalVars{}, throwDoNothing)
	actorContext.setEventBatchSize(2)
	actorContext.SendToNextNode(pmessage.BarrierMessage(1))
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, len(ch))
	actorContext.SendToNextNode(pmessage.BarrierMessage(2))
	tick := time.After(500 * time.Millisecond)
	select {
	case <-tick:
		t.Fatal("timeout")
	case m := <-ch:
		require.Equal(t, pmessage.MessageTypeTick, m.Value.Tp)
	}
	actorContext.SendToNextNode(pmessage.BarrierMessage(1))
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 0, len(ch))
}

type forwardActor struct {
	contextAware bool

	ch chan<- message.Message[pmessage.Message]
}

func (f *forwardActor) Poll(
	ctx sdtContext.Context, msgs []message.Message[pmessage.Message],
) bool {
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

func throwDoNothing(_ error) {
}
