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

// TODO remove following line once revive supports generic.
//revive:disable:receiver-naming

package actor

import (
	"container/list"
	"context"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/actor/message"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// The max number of workers of a system.
	maxWorkerNum = 64
	// DefaultActorBatchSize is the default size of polled actor batch.
	DefaultActorBatchSize = 1
	// DefaultMsgBatchSizePerActor is the default size of receive message batch.
	DefaultMsgBatchSizePerActor = 64
)

var (
	errActorStopped  = cerrors.ErrActorStopped.FastGenByArgs()
	errActorNotFound = cerrors.ErrActorNotFound.FastGenByArgs()
)

// procState is the state of a proc.
//
//	┌---------┐  Actor.Close()
//	| Running |-----------------┐
//	└--+------┘                 v
//	   |                    ┌--------┐
//	   | System.Close()     | Closed |
//	   v                    └--------┘
//	┌---------------┐           ^
//	| MailboxClosed |-----------┘
//	└---------------┘ Mailbox Empty
type procState uint64

const (
	// Running state, proc can be polled.
	procStateRunning procState = iota
	// Mailbox closed, proc is about to be closed.
	procStateMailboxClosed
	// Closed, both mailbox and actor is closed, proc can not be polled.
	procStateClosed
)

// proc is wrapper of a running actor.
type proc[T any] struct {
	state uint64
	mb    Mailbox[T]
	actor Actor[T]
}

// batchReceiveMsgs receives messages into batchMsg.
func (p *proc[T]) batchReceiveMsgs(batchMsg []message.Message[T]) int {
	n := 0
	max := len(batchMsg)
	for i := 0; i < max; i++ {
		msg, ok := p.mb.Receive()
		if !ok {
			// Stop receive if there is no more messages.
			break
		}
		batchMsg[i] = msg
		n++
	}
	return n
}

// isClosed returns ture, means its mailbox and actor are closed.
// isClosed is thread-safe.
func (p *proc[T]) isClosed() bool {
	return atomic.LoadUint64(&p.state) == uint64(procStateClosed)
}

// closeMailbox close mailbox and set state to closed for graceful close.
// onSystemStop is thread-safe.
func (p *proc[T]) onSystemStop() {
	// Running -> MailboxClosed
	if atomic.CompareAndSwapUint64(
		&p.state, uint64(procStateRunning), uint64(procStateMailboxClosed)) {
		p.mb.close()
	}
}

// closeMailbox close mailbox and set state to closed.
// onMailboxEmpty is thread-safe.
func (p *proc[T]) onMailboxEmpty() {
	// MailboxClosed -> Close
	if atomic.CompareAndSwapUint64(
		&p.state, uint64(procStateMailboxClosed), uint64(procStateClosed)) {
		p.actor.OnClose()
	}
}

// onActorClosed closes all mailbox and actor.
// onActorClosed is thread-safe.
func (p *proc[T]) onActorClosed() {
	if atomic.CompareAndSwapUint64(
		&p.state, uint64(procStateRunning), uint64(procStateClosed)) {
		p.mb.close()
		p.actor.OnClose()
		return
	}
	if atomic.CompareAndSwapUint64(
		&p.state, uint64(procStateMailboxClosed), uint64(procStateClosed)) {
		p.actor.OnClose()
	}
}

type readyState int

const (
	// Running state, system needs to poll actors.
	readyStateRunning readyState = 0
	// It's about to stop, system needs to graceful stop actors.
	readyStateStopping readyState = 1
	// Stopped, system stops polling.
	readyStateStopped readyState = 2
)

// ready is a centralized notification struct, shared by a router and a system.
// It schedules notification and actors.
type ready[T any] struct {
	sync.Mutex
	cond *sync.Cond

	// TODO: replace with a memory efficient queue,
	// e.g., an array based queue to save allocation.
	queue list.List
	// In the set, an actor is either polling by system
	// or is pending to be polled.
	procs map[ID]struct{}
	state readyState

	metricDropMessage prometheus.Counter
}

func (rd *ready[T]) prepareStop() {
	rd.Lock()
	rd.state = readyStateStopping
	rd.Unlock()
}

func (rd *ready[T]) stop() {
	rd.Lock()
	rd.state = readyStateStopped
	rd.Unlock()
	rd.cond.Broadcast()
}

// enqueueLocked enqueues ready proc.
// If the proc is already enqueued, it ignores.
// Set force to true to force enqueue. It is useful to force the proc to be
// polled again.
func (rd *ready[T]) enqueueLocked(p *proc[T], force bool) error {
	if p.isClosed() {
		return errActorStopped
	}
	id := p.mb.ID()
	if _, ok := rd.procs[id]; !ok || force {
		rd.queue.PushBack(p)
		rd.procs[id] = struct{}{}
	}
	return nil
}

// schedule schedules the proc to system.
func (rd *ready[T]) schedule(p *proc[T]) error {
	rd.Lock()
	err := rd.enqueueLocked(p, false)
	rd.Unlock()
	if err != nil {
		return err
	}
	// Notify system to poll the proc.
	rd.cond.Signal()
	return nil
}

// scheduleN schedules a slice of procs to system.
// It ignores stopped procs.
func (rd *ready[T]) scheduleN(procs []*proc[T]) {
	rd.Lock()
	for _, p := range procs {
		_ = rd.enqueueLocked(p, false)
	}
	rd.Unlock()
	rd.cond.Broadcast()
}

// batchReceiveProcs receives ready procs into batchP.
func (rd *ready[T]) batchReceiveProcs(batchP []*proc[T]) int {
	n := 0
	max := len(batchP)
	for i := 0; i < max; i++ {
		if rd.queue.Len() == 0 {
			// Stop receive if there is no more ready procs.
			break
		}
		element := rd.queue.Front()
		rd.queue.Remove(element)
		p := element.Value.(*proc[T])
		batchP[i] = p
		n++
	}
	return n
}

// Router send messages to actors.
type Router[T any] struct {
	rd *ready[T]

	// Map of ID to proc
	procs sync.Map
}

// NewRouter returns a new router.
func NewRouter[T any](name string) *Router[T] {
	r := &Router[T]{
		rd: &ready[T]{},
	}
	r.rd.cond = sync.NewCond(&r.rd.Mutex)
	r.rd.procs = make(map[ID]struct{})
	r.rd.queue.Init()
	r.rd.metricDropMessage = dropMsgCount.WithLabelValues(name)
	return r
}

// Send a message to an actor. It's a non-blocking send.
// ErrMailboxFull when the actor full.
// ErrActorNotFound when the actor not found.
func (r *Router[T]) Send(id ID, msg message.Message[T]) error {
	value, ok := r.procs.Load(id)
	if !ok {
		return errActorNotFound
	}
	p := value.(*proc[T])
	err := p.mb.Send(msg)
	if err != nil {
		return err
	}
	return r.rd.schedule(p)
}

// SendB sends a message to an actor, blocks when it's full.
// ErrActorNotFound when the actor not found.
// Canceled or DeadlineExceeded when the context is canceled or done.
func (r *Router[T]) SendB(ctx context.Context, id ID, msg message.Message[T]) error {
	value, ok := r.procs.Load(id)
	if !ok {
		return errActorNotFound
	}
	p := value.(*proc[T])
	err := p.mb.SendB(ctx, msg)
	if err != nil {
		return err
	}
	return r.rd.schedule(p)
}

// Broadcast a message to all actors in the router.
// The message may be dropped when context is canceled.
func (r *Router[T]) Broadcast(ctx context.Context, msg message.Message[T]) {
	batchSize := 128
	ps := make([]*proc[T], 0, batchSize)
	r.procs.Range(func(key, value interface{}) bool {
		p := value.(*proc[T])
		if err := p.mb.SendB(ctx, msg); err != nil {
			log.Warn("failed to send to message",
				zap.Error(err), zap.Uint64("id", uint64(p.mb.ID())),
				zap.Reflect("msg", msg))
			// Skip schedule the proc.
			return true
		}
		ps = append(ps, p)
		if len(ps) == batchSize {
			r.rd.scheduleN(ps)
			ps = ps[:0]
		}
		return true
	})

	if len(ps) != 0 {
		r.rd.scheduleN(ps)
	}
}

func (r *Router[T]) insert(id ID, p *proc[T]) error {
	_, exist := r.procs.LoadOrStore(id, p)
	if exist {
		return cerrors.ErrActorDuplicate.FastGenByArgs()
	}
	return nil
}

func (r *Router[T]) remove(id ID) bool {
	_, present := r.procs.LoadAndDelete(id)
	return present
}

// SystemBuilder is a builder of a system.
type SystemBuilder[T any] struct {
	name                 string
	numWorker            int
	actorBatchSize       int
	msgBatchSizePerActor int

	fatalHandler func(string, ID)
}

// NewSystemBuilder returns a new system builder.
func NewSystemBuilder[T any](name string) *SystemBuilder[T] {
	defaultWorkerNum := maxWorkerNum
	goMaxProcs := runtime.GOMAXPROCS(0)
	if goMaxProcs*8 < defaultWorkerNum {
		defaultWorkerNum = goMaxProcs * 8
	}

	return &SystemBuilder[T]{
		name:                 name,
		numWorker:            defaultWorkerNum,
		actorBatchSize:       DefaultActorBatchSize,
		msgBatchSizePerActor: DefaultMsgBatchSizePerActor,
	}
}

// WorkerNumber sets the number of workers of a system.
func (b *SystemBuilder[T]) WorkerNumber(numWorker int) *SystemBuilder[T] {
	if numWorker <= 0 {
		numWorker = 1
	} else if numWorker > maxWorkerNum {
		numWorker = maxWorkerNum
	}
	b.numWorker = numWorker
	return b
}

// Throughput sets the throughput per-poll of a system.
func (b *SystemBuilder[T]) Throughput(
	actorBatchSize, msgBatchSizePerActor int,
) *SystemBuilder[T] {
	if actorBatchSize <= 0 {
		actorBatchSize = 1
	}
	if msgBatchSizePerActor <= 0 {
		msgBatchSizePerActor = 1
	}

	b.actorBatchSize = actorBatchSize
	b.msgBatchSizePerActor = msgBatchSizePerActor
	return b
}

// handleFatal sets the fatal handler of a system.
func (b *SystemBuilder[T]) handleFatal(
	fatalHandler func(string, ID),
) *SystemBuilder[T] {
	b.fatalHandler = fatalHandler
	return b
}

// Build builds a system and a router.
func (b *SystemBuilder[T]) Build() (*System[T], *Router[T]) {
	router := NewRouter[T](b.name)
	metricWorkingDurations := make([]prometheus.Counter, b.numWorker)
	for i := range metricWorkingDurations {
		metricWorkingDurations[i] = workingDuration.WithLabelValues(b.name, strconv.Itoa(i))
	}
	return &System[T]{
		name:                 b.name,
		numWorker:            b.numWorker,
		actorBatchSize:       b.actorBatchSize,
		msgBatchSizePerActor: b.msgBatchSizePerActor,

		rd:     router.rd,
		router: router,

		fatalHandler: b.fatalHandler,

		metricTotalWorkers:     totalWorkers.WithLabelValues(b.name),
		metricWorkingWorkers:   workingWorkers.WithLabelValues(b.name),
		metricWorkingDurations: metricWorkingDurations,
		metricSystemPollLoop:   pollCounter.WithLabelValues(b.name, "system"),
		metricActorPollLoop:    pollCounter.WithLabelValues(b.name, "actor"),
		metricSlowPollDuration: slowPollActorDuration.WithLabelValues(b.name),
		metricProcBatch:        batchSizeCounter.WithLabelValues(b.name, "proc"),
		metricMsgBatch:         batchSizeCounter.WithLabelValues(b.name, "msg"),
	}, router
}

// System is the runtime of Actors.
type System[T any] struct {
	name                 string
	numWorker            int
	actorBatchSize       int
	msgBatchSizePerActor int

	rd     *ready[T]
	router *Router[T]
	wg     *errgroup.Group
	cancel context.CancelFunc

	fatalHandler func(string, ID)

	// Metrics
	metricTotalWorkers     prometheus.Gauge
	metricWorkingWorkers   prometheus.Gauge
	metricWorkingDurations []prometheus.Counter
	metricSystemPollLoop   prometheus.Counter
	metricActorPollLoop    prometheus.Counter
	metricSlowPollDuration prometheus.Observer
	metricProcBatch        prometheus.Counter
	metricMsgBatch         prometheus.Counter
}

// Start the system. Cancelling the context to stop the system.
// Start is not thread-safe.
func (s *System[T]) Start(ctx context.Context) {
	s.wg, ctx = errgroup.WithContext(ctx)
	ctx, s.cancel = context.WithCancel(ctx)

	s.metricTotalWorkers.Add(float64(s.numWorker))
	for i := 0; i < s.numWorker; i++ {
		id := i
		s.wg.Go(func() error {
			pctx := pprof.WithLabels(ctx, pprof.Labels("actor", s.name))
			pprof.SetGoroutineLabels(pctx)

			s.poll(pctx, id)
			return nil
		})
	}
}

// Stop the system, cancels all actors. It should be called after Start.
// Messages sent before this call will be received by actors.
// Stop is not thread-safe.
func (s *System[T]) Stop() {
	// Cancel context-aware work currently being polled.
	if s.cancel != nil {
		s.cancel()
	}
	// Before stopping any actor, set ready status to readyStateStopping, so
	// any actor that is polled after this line will be closed by the system.
	s.rd.prepareStop()
	// Notify all actors in the system.
	s.router.Broadcast(context.Background(), message.StopMessage[T]())
	// Wake workers to close ready actors.
	s.rd.stop()
	s.metricTotalWorkers.Add(-float64(s.numWorker))
	// Worker goroutines never return errors.
	_ = s.wg.Wait()
}

// Spawn spawns an actor in the system.
// Spawn is thread-safe.
func (s *System[T]) Spawn(mb Mailbox[T], actor Actor[T]) error {
	id := mb.ID()
	p := &proc[T]{mb: mb, actor: actor}
	return s.router.insert(id, p)
}

// The main poll of actor system.
func (s *System[T]) poll(ctx context.Context, id int) {
	batchPBuf := make([]*proc[T], s.actorBatchSize)
	batchMsgBuf := make([]message.Message[T], s.msgBatchSizePerActor)
	rd := s.rd
	rd.Lock()

	// Approximate current time. It is updated when calling `now`.
	var approximateCurrentTime time.Time
	now := func() time.Time {
		approximateCurrentTime = time.Now()
		return approximateCurrentTime
	}
	// Start time of polling procs.
	systemPollStartTime := now()
	// The last time of recording metrics.
	lastRecordMetricTime := systemPollStartTime
	procBatchCnt, systemPollLoopCnt := 0, 0
	msgBatchCnt, actorPollLoopCnt := 0, 0
	s.metricWorkingWorkers.Inc()
	for {
		// Recording batch and loop metrics.
		// We update metrics every `metricsInterval` to reduce overhead.
		if approximateCurrentTime.Sub(lastRecordMetricTime) > metricsInterval {
			lastRecordMetricTime = approximateCurrentTime
			s.metricProcBatch.Add(float64(procBatchCnt))
			s.metricSystemPollLoop.Add(float64(systemPollLoopCnt))
			procBatchCnt, systemPollLoopCnt = 0, 0
			s.metricMsgBatch.Add(float64(msgBatchCnt))
			s.metricActorPollLoop.Add(float64(actorPollLoopCnt))
			msgBatchCnt, actorPollLoopCnt = 0, 0
		}

		var batchP []*proc[T]
		for {
			// Batch receive ready procs.
			n := rd.batchReceiveProcs(batchPBuf)
			if n != 0 {
				batchP = batchPBuf[:n]
				procBatchCnt += n
				systemPollLoopCnt++
				break
			}
			// Recording working metrics.
			systemPollDuration := now().Sub(systemPollStartTime)
			s.metricWorkingDurations[id].Add(systemPollDuration.Seconds())
			s.metricWorkingWorkers.Dec()
			if rd.state == readyStateStopped {
				rd.Unlock()
				return
			}
			// Park the poll until awoken by Send or Broadcast.
			rd.cond.Wait()
			systemPollStartTime = now()
			s.metricWorkingWorkers.Inc()
		}
		rd.Unlock()

		actorPollLoopCnt += len(batchP)
		actorPollStartTime := now()
		for _, p := range batchP {
			closed := p.isClosed()
			if closed {
				s.handleFatal(
					"closed actor can never receive new messages again",
					p.mb.ID())
			}

			// Batch receive actor's messages.
			n := p.batchReceiveMsgs(batchMsgBuf)
			if n == 0 {
				continue
			}
			batchMsg := batchMsgBuf[:n]
			msgBatchCnt += n

			// Poll actor.
			running := p.actor.Poll(ctx, batchMsg)
			if !running {
				// Close the actor and mailbox.
				p.onActorClosed()
			}
			actorPollDuration := now().Sub(actorPollStartTime)
			actorPollStartTime = approximateCurrentTime
			if actorPollDuration > slowPollThreshold {
				// Prometheus histogram is expensive, we only record slow poll.
				s.metricSlowPollDuration.Observe(actorPollDuration.Seconds())
				if actorPollDuration > 10*slowPollThreshold { // 1s
					log.Warn("actor poll received messages too slow",
						zap.Duration("duration", actorPollDuration),
						zap.Uint64("id", uint64(p.mb.ID())),
						zap.String("name", s.name))
				}
			}
		}

		rd.Lock()
		for _, p := range batchP {
			if rd.state != readyStateRunning {
				// System is about to stop.
				p.onSystemStop()
			}
			remainMsgCount := p.mb.len()
			if remainMsgCount == 0 {
				// At this point, there is no more message needs to be polled,
				// or the actor is closing.
				// Delete the actor from ready set.
				delete(rd.procs, p.mb.ID())
				p.onMailboxEmpty()
			} else {
				// Force enqueue to poll remaining messages if it's running.
				_ = rd.enqueueLocked(p, true)
			}
			if p.isClosed() {
				// Remove closed actor from router.
				present := s.router.remove(p.mb.ID())
				if !present {
					s.handleFatal(
						"try to remove non-existent actor", p.mb.ID())
				}

				// Drop all remaining messages.
				if remainMsgCount != 0 {
					// TODO: we may need to release resources hold by messages.
					rd.metricDropMessage.Add(float64(remainMsgCount))
				}
			}
		}
	}
}

func (s *System[T]) handleFatal(msg string, id ID) {
	handler := defaultFatalHandler
	if s.fatalHandler != nil {
		handler = s.fatalHandler
	}
	handler(msg, id)
}

func defaultFatalHandler(msg string, id ID) {
	log.Panic(msg, zap.Uint64("id", uint64(id)))
}
