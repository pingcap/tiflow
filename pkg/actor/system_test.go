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

package actor

import (
	"context"
	"fmt"
	"math"
	_ "net/http/pprof"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tiflow/pkg/actor/message"
	"github.com/pingcap/tiflow/pkg/leakutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	leakutil.SetUpLeakTest(m)
}

func makeTestSystem[T any](name string) (*System[T], *Router[T]) {
	return NewSystemBuilder[T](name).
		WorkerNumber(2).
		handleFatal(func(s string, i ID) {
			panic(fmt.Sprintf("%s actorID: %d", s, i))
		}).
		Build()
}

func TestSystemBuilder(t *testing.T) {
	t.Parallel()
	b := NewSystemBuilder[any]("test")
	require.LessOrEqual(t, b.numWorker, maxWorkerNum)
	require.Greater(t, b.numWorker, 0)

	b.WorkerNumber(0)
	require.Equal(t, 1, b.numWorker)

	b.WorkerNumber(2)
	require.Equal(t, 2, b.numWorker)

	require.Greater(t, b.actorBatchSize, 0)
	require.Greater(t, b.msgBatchSizePerActor, 0)

	b.Throughput(0, 0)
	require.Greater(t, b.actorBatchSize, 0)
	require.Greater(t, b.msgBatchSizePerActor, 0)

	b.Throughput(7, 8)
	require.Equal(t, 7, b.actorBatchSize)
	require.Equal(t, 8, b.msgBatchSizePerActor)
}

func TestMailboxSendAndSendB(t *testing.T) {
	t.Parallel()
	mb := NewMailbox[any](ID(0), 1)
	err := mb.Send(message.ValueMessage[any](nil))
	require.Nil(t, err)

	err = mb.Send(message.ValueMessage[any](nil))
	require.True(t, strings.Contains(err.Error(), "mailbox is full"))

	msg, ok := mb.Receive()
	require.Equal(t, true, ok)
	require.Equal(t, message.ValueMessage[any](nil), msg)

	// Test SendB can be canceled by context.
	ch := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := mb.Send(message.ValueMessage[any](nil))
		ch <- err
		err = mb.SendB(ctx, message.ValueMessage[any](nil))
		ch <- err
	}()

	require.Nil(t, <-ch)
	cancel()
	require.Equal(t, context.Canceled, <-ch)
}

func TestRouterSendAndSendB(t *testing.T) {
	t.Parallel()
	id := ID(0)
	mb := NewMailbox[any](id, 1)
	router := NewRouter[any](t.Name())
	err := router.insert(id, &proc[any]{mb: mb})
	require.Nil(t, err)
	err = router.Send(id, message.ValueMessage[any](nil))
	require.Nil(t, err)

	err = router.Send(id, message.ValueMessage[any](nil))
	require.True(t, strings.Contains(err.Error(), "mailbox is full"))

	msg, ok := mb.Receive()
	require.Equal(t, true, ok)
	require.Equal(t, message.ValueMessage[any](nil), msg)

	// Test SendB can be canceled by context.
	ch := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := router.Send(id, message.ValueMessage[any](nil))
		ch <- err
		err = router.SendB(ctx, id, message.ValueMessage[any](nil))
		ch <- err
	}()

	require.Nil(t, <-ch)
	cancel()
	require.Equal(t, context.Canceled, <-ch)
}

func wait(t *testing.T, f func()) {
	wait := make(chan int)
	go func() {
		f()
		wait <- 0
	}()
	select {
	case <-wait:
	case <-time.After(5 * time.Second):
		// There may be a deadlock if f takes more than 5 seconds.
		t.Fatal("Timed out")
	}
}

func TestSystemStartStop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sys, _ := makeTestSystem[any](t.Name())
	sys.Start(ctx)
	sys.Stop()
}

func TestSystemSpawnDuplicateActor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sys, _ := makeTestSystem[any](t.Name())
	sys.Start(ctx)

	id := 1
	fa := &forwardActor[any]{ch: make(chan<- message.Message[any], 1)}
	mb := NewMailbox[any](ID(id), 1)
	require.Nil(t, sys.Spawn(mb, fa))
	require.NotNil(t, sys.Spawn(mb, fa))

	wait(t, sys.Stop)
}

type forwardActor[T any] struct {
	contextAware bool

	id ID
	ch chan<- message.Message[T]
}

func (f *forwardActor[T]) Poll(ctx context.Context, msgs []message.Message[T]) bool {
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

func (f *forwardActor[T]) OnClose() {}

func TestActorSendReceive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sys, router := makeTestSystem[int](t.Name())
	sys.Start(ctx)

	// Send to a non-existing actor.
	id := ID(777)
	err := router.Send(id, message.ValueMessage(0))
	require.Equal(t, errActorNotFound, err)

	ch := make(chan message.Message[int], 1)
	fa := &forwardActor[int]{
		ch: ch,
	}
	mb := NewMailbox[int](id, 1)

	// The actor is not in router yet.
	err = router.Send(id, message.ValueMessage(1))
	require.Equal(t, errActorNotFound, err)

	// Spawn adds the actor to the router.
	require.Nil(t, sys.Spawn(mb, fa))
	err = router.Send(id, message.ValueMessage(2))
	require.Nil(t, err)
	select {
	case msg := <-ch:
		require.Equal(t, message.ValueMessage(2), msg)
	case <-time.After(time.Second):
		t.Fatal("Timed out")
	}

	wait(t, sys.Stop)
}

func testBroadcast(t *testing.T, actorNum, workerNum int) {
	ctx := context.Background()
	sys, router := NewSystemBuilder[any]("test").WorkerNumber(workerNum).Build()
	sys.Start(ctx)

	ch := make(chan message.Message[any], actorNum)

	for id := 0; id < actorNum; id++ {
		fa := &forwardActor[any]{
			ch: ch,
		}
		mb := NewMailbox[any](ID(id), 1)
		require.Nil(t, sys.Spawn(mb, fa))
	}

	// Broadcase tick to actors.
	router.Broadcast(context.TODO(), message.ValueMessage[any](nil))
	for i := 0; i < actorNum; i++ {
		select {
		case msg := <-ch:
			require.Equal(t, message.ValueMessage[any](nil), msg)
		case <-time.After(time.Second):
			t.Fatal("Timed out")
		}
	}
	select {
	case msg := <-ch:
		t.Fatal("Unexpected message", msg)
	case <-time.After(200 * time.Millisecond):
	}

	wait(t, sys.Stop)
}

func TestBroadcast(t *testing.T) {
	t.Parallel()
	for _, workerNum := range []int{1, 2, 16, 32, 64} {
		for _, actorNum := range []int{0, 1, 64, 128, 195, 1024} {
			testBroadcast(t, actorNum, workerNum)
		}
	}
}

func TestSystemStopCancelActors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sys, router := makeTestSystem[any](t.Name())
	sys.Start(ctx)

	id := ID(777)
	ch := make(chan message.Message[any], 1)
	fa := &forwardActor[any]{
		id:           id,
		ch:           ch,
		contextAware: true,
	}
	mb := NewMailbox[any](id, 1)
	require.Nil(t, sys.Spawn(mb, fa))
	err := router.Send(id, message.ValueMessage[any](nil))
	require.Nil(t, err)

	id = ID(778)
	fa = &forwardActor[any]{
		id:           id,
		ch:           ch,
		contextAware: true,
	}
	mb = NewMailbox[any](id, 1)
	require.Nil(t, sys.Spawn(mb, fa))
	err = router.Send(id, message.ValueMessage[any](nil))
	require.Nil(t, err)

	// Do not receive ch.
	_ = ch

	wait(t, sys.Stop)
}

func TestActorManyMessageOneSchedule(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sys, router := makeTestSystem[any](t.Name())
	sys.Start(ctx)

	id := ID(777)
	// To avoid blocking, use a large buffer.
	size := DefaultMsgBatchSizePerActor * 4
	ch := make(chan message.Message[any], size)
	fa := &forwardActor[any]{
		ch: ch,
	}
	mb := NewMailbox[any](id, size)
	require.Nil(t, sys.Spawn(mb, fa))

	for total := 1; total < size; total *= 2 {
		for j := 0; j < total-1; j++ {
			require.Nil(t, mb.Send(message.ValueMessage[any](nil)))
		}

		// Sending to mailbox does not trigger scheduling.
		select {
		case msg := <-ch:
			t.Fatal("Unexpected message", msg)
		case <-time.After(100 * time.Millisecond):
		}

		require.Nil(t, router.Send(id, message.ValueMessage[any](nil)))

		acc := 0
		for i := 0; i < total; i++ {
			select {
			case <-ch:
				acc++
			case <-time.After(time.Second):
				t.Fatal("Timed out, get ", acc, " expect ", total)
			}
		}
		select {
		case msg := <-ch:
			t.Fatal("Unexpected message", msg, total, acc)
		case <-time.After(100 * time.Millisecond):
		}
	}

	wait(t, sys.Stop)
}

type flipflopActor struct {
	t     *testing.T
	level int64

	syncCount int
	ch        chan int64
	acc       int64
}

func (f *flipflopActor) Poll(ctx context.Context, msgs []message.Message[any]) bool {
	for range msgs {
		level := atomic.LoadInt64(&f.level)
		newLevel := 0
		if level == 0 {
			newLevel = 1
		} else {
			newLevel = 0
		}
		swapped := atomic.CompareAndSwapInt64(&f.level, level, int64(newLevel))
		require.True(f.t, swapped)

		if atomic.AddInt64(&f.acc, 1)%int64(f.syncCount) == 0 {
			f.ch <- 0
		}
	}
	return true
}

func (f *flipflopActor) OnClose() {}

// An actor can only be polled by one goroutine at the same time.
func TestConcurrentPollSameActor(t *testing.T) {
	t.Parallel()
	concurrency := 4
	sys, router := NewSystemBuilder[any]("test").WorkerNumber(concurrency).Build()
	sys.Start(context.Background())

	syncCount := 1_000_000
	ch := make(chan int64)
	fa := &flipflopActor{
		t:         t,
		ch:        ch,
		syncCount: syncCount,
	}
	id := ID(777)
	mb := NewMailbox[any](id, DefaultMsgBatchSizePerActor)
	require.Nil(t, sys.Spawn(mb, fa))

	// Test 5 seconds
	timer := time.After(5 * time.Second)
	for {
		total := int64(0)
		for i := 0; i < syncCount; i++ {
			_ = router.Send(id, message.ValueMessage[any](nil))
		}
		total += int64(syncCount)
		select {
		case acc := <-ch:
			require.Equal(t, total, acc)
		case <-timer:
			wait(t, sys.Stop)
			return
		}
	}
}

type closedActor struct {
	acc int
	ch  chan int
}

func (c *closedActor) Poll(ctx context.Context, msgs []message.Message[any]) bool {
	c.acc += len(msgs)
	c.ch <- c.acc
	// closed
	return false
}

func (c *closedActor) OnClose() {}

func TestPollStoppedActor(t *testing.T) {
	ctx := context.Background()
	sys, router := makeTestSystem[any](t.Name())
	sys.Start(ctx)

	id := ID(777)
	// To avoid blocking, use a large buffer.
	cap := DefaultMsgBatchSizePerActor * 4
	mb := NewMailbox[any](id, cap)
	ch := make(chan int)
	require.Nil(t, sys.Spawn(mb, &closedActor{ch: ch}))

	for i := 0; i < (cap - 1); i++ {
		require.Nil(t, mb.Send(message.ValueMessage[any](nil)))
	}
	// Trigger scheduling
	require.Nil(t, router.Send(id, message.ValueMessage[any](nil)))

	<-ch
	select {
	case <-time.After(500 * time.Millisecond):
	case <-ch:
		t.Fatal("must timeout")
	}
	wait(t, sys.Stop)
}

func TestStoppedActorIsRemovedFromRouter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sys, router := makeTestSystem[any](t.Name())
	sys.Start(ctx)

	id := ID(777)
	mb := NewMailbox[any](id, DefaultMsgBatchSizePerActor)
	ch := make(chan int)
	require.Nil(t, sys.Spawn(mb, &closedActor{ch: ch}))

	// Trigger scheduling
	require.Nil(t, router.Send(id, message.ValueMessage[any](nil)))
	timeout := time.After(5 * time.Second)
	select {
	case <-timeout:
		t.Fatal("timeout")
	case <-ch:
	}

	for i := 0; i < 50; i++ {
		// Wait for actor to be removed.
		time.Sleep(100 * time.Millisecond)
		err := router.Send(id, message.ValueMessage[any](nil))
		if strings.Contains(err.Error(), "actor not found") {
			break
		}
		if i == 49 {
			t.Fatal("actor is still in router")
		}
	}

	wait(t, sys.Stop)
}

type slowActor struct {
	ch chan struct{}
}

func (c *slowActor) Poll(ctx context.Context, msgs []message.Message[any]) bool {
	c.ch <- struct{}{}
	<-c.ch
	// closed
	return false
}

func (c *slowActor) OnClose() {}

// Test router send during actor poll and before close.
//
//	----------------------> time
//	   '-----------' Poll
//	      ' Send
//	               ' Close
func TestSendBeforeClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sys, router := makeTestSystem[any](t.Name())
	sys.Start(ctx)

	id := ID(777)
	mb := NewMailbox[any](id, DefaultMsgBatchSizePerActor)
	ch := make(chan struct{})
	require.Nil(t, sys.Spawn(mb, &slowActor{ch: ch}))

	// Trigger scheduling
	require.Nil(t, router.Send(id, message.ValueMessage[any](nil)))

	// Wait for actor to be polled.
	a := <-ch

	// Send message before close.
	err := router.Send(id, message.ValueMessage[any](nil))
	require.Nil(t, err)

	// Unblock poll.
	ch <- a

	// Wait for actor to be removed.
	for {
		time.Sleep(100 * time.Millisecond)
		_, ok := router.procs.Load(id)
		if !ok {
			break
		}
	}
	// Must drop 1 message.
	m := &dto.Metric{}
	require.Nil(t, sys.rd.metricDropMessage.Write(m))
	dropped := int(*m.Counter.Value)
	require.Equal(t, 1, dropped)

	// Let send and close race
	// sys.rd.Lock()

	wait(t, sys.Stop)
}

// Test router send after close and before enqueue.
//
//	----------------------> time
//	 '-----' Poll
//	       ' Close
//	          ' Send
//	              'Enqueue
func TestSendAfterClose(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sys, router := makeTestSystem[any](t.Name())
	sys.Start(ctx)

	id := ID(777)
	dropCount := 1
	cap := DefaultMsgBatchSizePerActor + dropCount
	mb := NewMailbox[any](id, cap)
	ch := make(chan struct{})
	require.Nil(t, sys.Spawn(mb, &slowActor{ch: ch}))
	pi, ok := router.procs.Load(id)
	require.True(t, ok)
	p := pi.(*proc[any])

	for i := 0; i < cap-1; i++ {
		require.Nil(t, mb.Send(message.ValueMessage[any](nil)))
	}
	// Trigger scheduling
	require.Nil(t, router.Send(id, message.ValueMessage[any](nil)))

	// Wait for actor to be polled.
	a := <-ch

	// Block enqueue.
	sys.rd.Lock()

	// Unblock poll.
	ch <- a

	// Wait for actor to be closed.
	for {
		time.Sleep(100 * time.Millisecond)
		if p.isClosed() {
			break
		}
	}

	// enqueue must return actor stopped error.
	err := router.rd.enqueueLocked(p, false)
	require.Equal(t, errActorStopped, err)

	// Unblock enqueue.
	sys.rd.Unlock()
	// Wait for actor to be removed.
	for {
		time.Sleep(100 * time.Millisecond)
		_, ok := router.procs.Load(id)
		if !ok {
			break
		}
	}

	// Must drop 1 message.
	m := &dto.Metric{}
	require.Nil(t, sys.rd.metricDropMessage.Write(m))
	dropped := int(*m.Counter.Value)
	require.Equal(t, dropCount, dropped)

	wait(t, sys.Stop)
}

type stopActor struct {
	wait *int64
}

func (s *stopActor) Poll(ctx context.Context, msgs []message.Message[any]) bool {
	return true
}

func (s *stopActor) OnClose() {
	atomic.AddInt64(s.wait, 1)
}

func TestStopSystem(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sys, _ := makeTestSystem[any](t.Name())
	sys.Start(ctx)

	w := new(int64)
	for i := 0; i < 20_000; i++ {
		mb := NewMailbox[any](ID(i), 1)
		require.Nil(t, sys.Spawn(mb, &stopActor{w}))
	}

	wait(t, sys.Stop)
	require.EqualValues(t, 20_000, atomic.LoadInt64(w))
}

func TestSendAfterMailboxClosed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	router := NewRouter[any](t.Name())

	id := ID(1)
	cap := 1
	mb := NewMailbox[any](id, cap)
	router.InsertMailbox4Test(mb.ID(), mb)

	val, _ := router.procs.Load(id)
	proc := val.(*proc[any])
	msg := message.ValueMessage[any](nil)
	// To avoid racing between send and close, fill mailbox first,
	// so later Send and SendB always return actor stop.
	require.Nil(t, router.Send(id, msg))
	proc.onSystemStop()
	require.EqualValues(t, errActorStopped, router.Send(id, msg))
	require.EqualValues(t, errActorStopped, router.SendB(ctx, id, msg))
	wait(t, func() {
		router.Broadcast(ctx, msg)
	})
}

// Run the benchmark
// go test -benchmem -run='^$' -bench '^(BenchmarkActorSendReceive)$' github.com/pingcap/tiflow/pkg/actor
func BenchmarkActorSendReceive(b *testing.B) {
	ctx := context.Background()
	sys, router := makeTestSystem[any](b.Name())
	sys.Start(ctx)

	id := ID(777)
	size := DefaultMsgBatchSizePerActor * 4
	ch := make(chan message.Message[any], size)
	fa := &forwardActor[any]{
		ch: ch,
	}
	mb := NewMailbox[any](id, size)
	err := sys.Spawn(mb, fa)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("BenchmarkActorSendReceive", func(b *testing.B) {
		for total := 1; total <= size; total *= 2 {
			b.Run(fmt.Sprintf("%d message(s)", total), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					for j := 0; j < total; j++ {
						err = router.Send(id, message.ValueMessage[any](nil))
						if err != nil {
							b.Fatal(err)
						}
					}
					for j := 0; j < total; j++ {
						<-ch
					}
				}
			})
		}
	})

	sys.Stop()
}

// Run the benchmark
// go test -benchmem -run='^$' -bench '^(BenchmarkPollActor)$' github.com/pingcap/tiflow/pkg/actor
func BenchmarkPollActor(b *testing.B) {
	ctx := context.Background()
	sys, router := makeTestSystem[any](b.Name())
	sys.Start(ctx)

	actorCount := int(math.Exp2(15))
	// To avoid blocking, use a large buffer.
	ch := make(chan message.Message[any], actorCount)

	b.Run("BenchmarkPollActor", func(b *testing.B) {
		id := 1
		for total := 1; total <= actorCount; total *= 2 {
			for ; id <= total; id++ {
				fa := &forwardActor[any]{
					ch: ch,
				}
				mb := NewMailbox[any](ID(id), 1)
				err := sys.Spawn(mb, fa)
				if err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			b.Run(fmt.Sprintf("%d actor(s)", total), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					for j := 1; j <= total; j++ {
						err := router.Send(ID(j), message.ValueMessage[any](nil))
						if err != nil {
							b.Fatal(err)
						}
					}
					for j := 1; j <= total; j++ {
						<-ch
					}
				}
			})
			b.StopTimer()
		}
	})

	sys.Stop()
}
