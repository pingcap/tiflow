package actor

import (
	"context"
	"fmt"
	"math"
	_ "net/http/pprof"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/pkg/pipeline"
)

func Test(t *testing.T) { check.TestingT(t) }

type actorSuite struct{}

var _ = check.Suite(&actorSuite{})

func (s *actorSuite) TestSystemBuilder(c *check.C) {
	b := NewSystemBuilder("test")
	c.Assert(b.numWorker, check.LessEqual, maxWorkerNum)
	c.Assert(b.numWorker, check.Greater, 0)

	b.WorkerNumber(0)
	c.Assert(b.numWorker, check.Equals, 1)

	b.WorkerNumber(2)
	c.Assert(b.numWorker, check.Equals, 2)

	c.Assert(b.actorBatchSize, check.Greater, 0)
	c.Assert(b.msgBatchSizePerActor, check.Greater, 0)

	b.Throughput(0, 0)
	c.Assert(b.actorBatchSize, check.Greater, 0)
	c.Assert(b.msgBatchSizePerActor, check.Greater, 0)

	b.Throughput(7, 8)
	c.Assert(b.actorBatchSize, check.Equals, 7)
	c.Assert(b.msgBatchSizePerActor, check.Equals, 8)
}

func (s *actorSuite) TestMailboxSendAndSendB(c *check.C) {
	mb := NewMailbox(ID(0), 1)
	err := mb.Send(pipeline.TickMessage())
	c.Assert(err, check.IsNil)

	err = mb.Send(pipeline.TickMessage())
	c.Assert(err, check.ErrorMatches, ".*mailbox is full.*")

	msg, ok := mb.tryReceive()
	c.Assert(ok, check.Equals, true)
	c.Assert(msg, check.Equals, pipeline.TickMessage())

	// Test SendB can be canceled by context.
	ch := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := mb.Send(pipeline.TickMessage())
		ch <- err
		err = mb.SendB(ctx, pipeline.TickMessage())
		ch <- err
	}()

	c.Assert(<-ch, check.IsNil)
	cancel()
	c.Assert(<-ch, check.Equals, context.Canceled)
}

func (s *actorSuite) TestRouterSendAndSendB(c *check.C) {
	id := ID(0)
	mb := NewMailbox(id, 1)
	router := newRouter()
	router.insert(id, &proc{mb: mb})
	err := router.Send(id, pipeline.TickMessage())
	c.Assert(err, check.IsNil)

	err = router.Send(id, pipeline.TickMessage())
	c.Assert(err, check.ErrorMatches, ".*mailbox is full.*")

	msg, ok := mb.tryReceive()
	c.Assert(ok, check.Equals, true)
	c.Assert(msg, check.Equals, pipeline.TickMessage())

	// Test SendB can be canceled by context.
	ch := make(chan error)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		err := router.Send(id, pipeline.TickMessage())
		ch <- err
		err = router.SendB(ctx, id, pipeline.TickMessage())
		ch <- err
	}()

	c.Assert(<-ch, check.IsNil)
	cancel()
	c.Assert(<-ch, check.Equals, context.Canceled)
}

func wait(c *check.C, timeout time.Duration, f func()) {
	wait := make(chan int)
	go func() {
		f()
		wait <- 0
	}()
	select {
	case <-wait:
	case <-time.After(time.Second):
		c.Fatal("Timed out")
	}
}

func (s *actorSuite) TestSystemStartStop(c *check.C) {
	ctx := context.Background()
	sys, _ := NewSystemBuilder("test").WorkerNumber(2).Build()
	sys.Start(ctx)
	err := sys.Stop()
	c.Assert(err, check.IsNil)
}

func (s *actorSuite) TestSystemSpawnDuplicateActor(c *check.C) {
	ctx := context.Background()
	sys, _ := NewSystemBuilder("test").WorkerNumber(2).Build()
	sys.Start(ctx)

	id := 1
	fa := &forwardActor{}
	mb := NewMailbox(ID(id), 1)
	c.Assert(sys.Spawn(mb, fa), check.IsNil)
	c.Assert(sys.Spawn(mb, fa), check.NotNil)

	wait(c, time.Second, func() {
		err := sys.Stop()
		c.Assert(err, check.IsNil)
	})
}

type forwardActor struct {
	contextAware bool

	ch chan<- pipeline.Message
}

func (f *forwardActor) Poll(ctx context.Context, msgs []pipeline.Message) bool {
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

func (s *actorSuite) TestActorSendReceive(c *check.C) {
	ctx := context.Background()
	sys, router := NewSystemBuilder("test").WorkerNumber(2).Build()
	sys.Start(ctx)

	// Send to a non-existing actor.
	id := ID(777)
	err := router.Send(id, pipeline.BarrierMessage(0))
	c.Assert(err, check.Equals, errActorNotFound)

	ch := make(chan pipeline.Message, 1)
	fa := &forwardActor{
		ch: ch,
	}
	mb := NewMailbox(id, 1)

	// The actor is not in router yet.
	err = router.Send(id, pipeline.BarrierMessage(1))
	c.Assert(err, check.Equals, errActorNotFound)

	// Spawn adds the actor to the router.
	c.Assert(sys.Spawn(mb, fa), check.IsNil)
	err = router.Send(id, pipeline.BarrierMessage(2))
	c.Assert(err, check.IsNil)
	select {
	case msg := <-ch:
		c.Assert(msg, check.Equals, pipeline.BarrierMessage(2))
	case <-time.After(time.Second):
		c.Fatal("Timed out")
	}

	wait(c, time.Second, func() {
		err := sys.Stop()
		c.Assert(err, check.IsNil)
	})
}

func testBroadcast(c *check.C, actorNum, workerNum int) {
	ctx := context.Background()
	sys, router := NewSystemBuilder("test").WorkerNumber(workerNum).Build()
	sys.Start(ctx)

	ch := make(chan pipeline.Message, 1)

	for id := 0; id < actorNum; id++ {
		fa := &forwardActor{
			ch: ch,
		}
		mb := NewMailbox(ID(id), 1)
		c.Assert(sys.Spawn(mb, fa), check.IsNil)
	}

	// Broadcase tick to actors.
	router.Broadcast(pipeline.TickMessage())
	for i := 0; i < actorNum; i++ {
		select {
		case msg := <-ch:
			c.Assert(msg, check.Equals, pipeline.TickMessage())
		case <-time.After(time.Second):
			c.Fatal("Timed out")
		}
	}
	select {
	case msg := <-ch:
		c.Fatal("Unexpected message", msg)
	case <-time.After(200 * time.Millisecond):
	}

	wait(c, time.Second, func() {
		err := sys.Stop()
		c.Assert(err, check.IsNil)
	})
}

func (s *actorSuite) TestBroadcast(c *check.C) {
	for _, workerNum := range []int{1, 2, 16, 32, 64} {
		for _, actorNum := range []int{0, 1, 64, 128, 195, 1024} {
			testBroadcast(c, actorNum, workerNum)
		}
	}
}

func (s *actorSuite) TestSystemStopCancelActors(c *check.C) {
	ctx := context.Background()
	sys, router := NewSystemBuilder("test").WorkerNumber(2).Build()
	sys.Start(ctx)

	id := ID(777)
	ch := make(chan pipeline.Message, 1)
	fa := &forwardActor{
		ch:           ch,
		contextAware: true,
	}
	mb := NewMailbox(id, 1)
	c.Assert(sys.Spawn(mb, fa), check.IsNil)
	err := router.Send(id, pipeline.TickMessage())
	c.Assert(err, check.IsNil)

	id = ID(778)
	fa = &forwardActor{
		ch:           ch,
		contextAware: true,
	}
	mb = NewMailbox(id, 1)
	c.Assert(sys.Spawn(mb, fa), check.IsNil)
	err = router.Send(id, pipeline.TickMessage())
	c.Assert(err, check.IsNil)

	// Do not receive ch.
	_ = ch

	wait(c, time.Second, func() {
		err := sys.Stop()
		c.Assert(err, check.IsNil)
	})
}

func (s *actorSuite) TestActorManyMessageOneSchedule(c *check.C) {
	ctx := context.Background()
	sys, router := NewSystemBuilder("test").WorkerNumber(2).Build()
	sys.Start(ctx)

	id := ID(777)
	// To avoid blocking, use a large buffer.
	size := defaultMsgBatchSizePerActor * 4
	ch := make(chan pipeline.Message, size)
	fa := &forwardActor{
		ch: ch,
	}
	mb := NewMailbox(id, size)
	c.Assert(sys.Spawn(mb, fa), check.IsNil)

	for total := 1; total < size; total *= 2 {
		for j := 0; j < total-1; j++ {
			c.Assert(mb.Send(pipeline.TickMessage()), check.IsNil)
		}

		// Sending to mailbox does not trigger scheduling.
		select {
		case msg := <-ch:
			c.Fatal("Unexpected message", msg)
		case <-time.After(100 * time.Millisecond):
		}

		c.Assert(router.Send(id, pipeline.TickMessage()), check.IsNil)

		acc := 0
		for i := 0; i < total; i++ {
			select {
			case <-ch:
				acc++
			case <-time.After(time.Second):
				c.Fatal("Timed out, get ", acc, " expect ", total)
			}
		}
	}

	wait(c, time.Second, func() {
		err := sys.Stop()
		c.Assert(err, check.IsNil)
	})
}

type flipflopActor struct {
	c     *check.C
	level int64

	syncCount int
	ch        chan int64
	acc       int64
}

func (f *flipflopActor) Poll(ctx context.Context, msgs []pipeline.Message) bool {
	for range msgs {
		level := atomic.LoadInt64(&f.level)
		newLevel := 0
		if level == 0 {
			newLevel = 1
		} else {
			newLevel = 0
		}
		swapped := atomic.CompareAndSwapInt64(&f.level, level, int64(newLevel))
		f.c.Assert(swapped, check.IsTrue)

		if atomic.AddInt64(&f.acc, 1)%int64(f.syncCount) == 0 {
			f.ch <- 0
		}
	}
	return true
}

// An actor can only be polled by one goroutine at the same time.
func (s *actorSuite) TestConcurrentPollSameActor(c *check.C) {
	concurrency := 4
	sys, router := NewSystemBuilder("test").WorkerNumber(concurrency).Build()
	sys.Start(context.Background())

	syncCount := 1_000_000
	ch := make(chan int64)
	fa := &flipflopActor{
		c:         c,
		ch:        ch,
		syncCount: syncCount,
	}
	id := ID(777)
	mb := NewMailbox(id, defaultMsgBatchSizePerActor)
	c.Assert(sys.Spawn(mb, fa), check.IsNil)

	// Test 5 seconds
	timer := time.After(5 * time.Second)
	for {
		total := int64(0)
		for i := 0; i < syncCount; i++ {
			_ = router.Send(id, pipeline.TickMessage())
		}
		total += int64(syncCount)
		select {
		case acc := <-ch:
			c.Assert(acc, check.Equals, total)
		case <-timer:
			wait(c, time.Second, func() {
				err := sys.Stop()
				c.Assert(err, check.IsNil)
			})
			return
		}
	}
}

type closedActor struct {
	acc int
	ch  chan int
}

func (c *closedActor) Poll(ctx context.Context, msgs []pipeline.Message) bool {
	c.acc += len(msgs)
	c.ch <- c.acc
	// closed
	return false
}

func (s *actorSuite) TestPollStoppedActor(c *check.C) {
	ctx := context.Background()
	sys, router := NewSystemBuilder("test").WorkerNumber(2).Build()
	sys.Start(ctx)

	id := ID(777)
	// To avoid blocking, use a large buffer.
	cap := defaultMsgBatchSizePerActor * 4
	mb := NewMailbox(id, cap)
	ch := make(chan int)
	c.Assert(sys.Spawn(mb, &closedActor{ch: ch}), check.IsNil)

	for i := 0; i < (cap - 1); i++ {
		c.Assert(mb.Send(pipeline.TickMessage()), check.IsNil)
	}
	// Trigger scheduling
	c.Assert(router.Send(id, pipeline.TickMessage()), check.IsNil)

	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			c.Fatal("timeout")
		case acc := <-ch:
			if acc == cap {
				// To complete the test, acc must be cap.
				wait(c, time.Second, func() {
					err := sys.Stop()
					c.Assert(err, check.IsNil)
				})
				return
			}
		}
	}
}

func (s *actorSuite) TestStoppedActorIsRemovedFromRouter(c *check.C) {
	ctx := context.Background()
	sys, router := NewSystemBuilder("test").WorkerNumber(2).Build()
	sys.Start(ctx)

	id := ID(777)
	mb := NewMailbox(id, defaultMsgBatchSizePerActor)
	ch := make(chan int)
	c.Assert(sys.Spawn(mb, &closedActor{ch: ch}), check.IsNil)

	// Trigger scheduling
	c.Assert(router.Send(id, pipeline.TickMessage()), check.IsNil)
	timeout := time.After(5 * time.Second)
	select {
	case <-timeout:
		c.Fatal("timeout")
	case <-ch:
	}
	c.Assert(router.Send(id, pipeline.TickMessage()), check.ErrorMatches, ".*actor not found.*")

	wait(c, time.Second, func() {
		err := sys.Stop()
		c.Assert(err, check.IsNil)
	})
}

type reopenedActor struct {
	trigger bool
}

func (r *reopenedActor) Poll(ctx context.Context, msgs []pipeline.Message) bool {
	r.trigger = !r.trigger
	return r.trigger
}

func (s *actorSuite) TestMustNotReopenActor(c *check.C) {
	idCh := make(chan ID)
	handler := func(msg string, id ID) {
		select {
		case idCh <- id:
		default:
		}
	}
	sys, router := NewSystemBuilder("test").WorkerNumber(1).handleFatal(handler).Build()
	ctx := context.Background()
	sys.Start(ctx)

	id := ID(777)
	// To avoid blocking, use a large buffer.
	cap := defaultMsgBatchSizePerActor * 4
	mb := NewMailbox(id, cap)
	c.Assert(sys.Spawn(mb, &reopenedActor{}), check.IsNil)

	for i := 0; i < (cap - 1); i++ {
		c.Assert(mb.Send(pipeline.TickMessage()), check.IsNil)
	}
	// Trigger scheduling
	c.Assert(router.Send(id, pipeline.TickMessage()), check.IsNil)

	timeout := time.After(5 * time.Second)
	select {
	case <-timeout:
		c.Fatal("timeout")
	case fatalID := <-idCh:
		c.Assert(fatalID, check.Equals, id)
	}

	wait(c, time.Second, func() {
		err := sys.Stop()
		c.Assert(err, check.IsNil)
	})
}

// Run the benchmark
// go test -benchmem -run='^$' -bench '^(BenchmarkActorSendReceive)$' github.com/pingcap/ticdc/pkg/actor
func BenchmarkActorSendReceive(b *testing.B) {
	ctx := context.Background()
	sys, router := NewSystemBuilder("test").WorkerNumber(2).Build()
	sys.Start(ctx)

	id := ID(777)
	size := defaultMsgBatchSizePerActor * 4
	ch := make(chan pipeline.Message, size)
	fa := &forwardActor{
		ch: ch,
	}
	mb := NewMailbox(id, size)
	err := sys.Spawn(mb, fa)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("BenchmarkActorSendReceive", func(b *testing.B) {
		for total := 1; total <= size; total *= 2 {
			b.Run(fmt.Sprintf("%d message(s)", total), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					for j := 0; j < total; j++ {
						err = router.Send(id, pipeline.TickMessage())
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

	if err := sys.Stop(); err != nil {
		b.Fatal(err)
	}
}

// Run the benchmark
// go test -benchmem -run='^$' -bench '^(BenchmarkPollActor)$' github.com/pingcap/ticdc/pkg/actor
func BenchmarkPollActor(b *testing.B) {
	ctx := context.Background()
	sys, router := NewSystemBuilder("test").WorkerNumber(2).Build()
	sys.Start(ctx)

	actorCount := int(math.Exp2(15))
	// To avoid blocking, use a large buffer.
	ch := make(chan pipeline.Message, actorCount)

	b.Run("BenchmarkPollActor", func(b *testing.B) {
		id := 1
		for total := 1; total <= actorCount; total *= 2 {
			for ; id <= total; id++ {
				fa := &forwardActor{
					ch: ch,
				}
				mb := NewMailbox(ID(id), 1)
				err := sys.Spawn(mb, fa)
				if err != nil {
					b.Fatal(err)
				}
			}

			b.ResetTimer()
			b.Run(fmt.Sprintf("%d actor(s)", total), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					for j := 1; j <= total; j++ {
						err := router.Send(ID(j), pipeline.TickMessage())
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

	if err := sys.Stop(); err != nil {
		b.Fatal(err)
	}
}
