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
	"container/list"
	"context"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/actor/message"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	// The max number of workers of a system.
	maxWorkerNum = 64
	// The default size of polled actor batch.
	defaultActorBatchSize = 1
	// The default size of receive message batch.
	defaultMsgBatchSizePerActor = 64
)

var (
	errActorStopped  = cerrors.ErrActorStopped.FastGenByArgs()
	errActorNotFound = cerrors.ErrActorNotFound.FastGenByArgs()
)

// proc is wrapper of a running actor.
type proc struct {
	mb     Mailbox
	actor  Actor
	closed uint64
}

// batchReceiveMsgs receives messages into batchMsg.
func (p *proc) batchReceiveMsgs(batchMsg []message.Message) int {
	n := 0
	max := len(batchMsg)
	for i := 0; i < max; i++ {
		msg, ok := p.mb.tryReceive()
		if !ok {
			// Stop receive if there is no more messages.
			break
		}
		batchMsg[i] = msg
		n++
	}
	return n
}

// close its actor.
// close is threadsafe.
func (p *proc) close() {
	atomic.StoreUint64(&p.closed, 1)
}

// isClosed returns ture, means its actor is closed.
// isClosed is threadsafe.
func (p *proc) isClosed() bool {
	return atomic.LoadUint64(&p.closed) == 1
}

// ready is a centralize notification struct, shared by a router and a system.
// It schedules notification and actors.
type ready struct {
	sync.Mutex
	cond *sync.Cond

	// TODO: replace with a memory efficient queue,
	// e.g., an array based queue to save allocation.
	queue list.List
	// In the set, an actor is either polling by system
	// or is pending to be polled.
	procs   map[ID]struct{}
	stopped bool

	metricDropMessage prometheus.Counter
}

func (rd *ready) stop() {
	rd.Lock()
	rd.stopped = true
	rd.Unlock()
	rd.cond.Broadcast()
}

// enqueueLocked enqueues ready proc.
// If the proc is already enqueued, it ignores.
// If the proc is closed, it ignores, drop messages in mailbox and return error.
// Set force to true to force enqueue. It is useful to force the proc to be
// polled again.
func (rd *ready) enqueueLocked(p *proc, force bool) error {
	if p.isClosed() {
		// Drop all remaining messages.
		counter := 0
		_, ok := p.mb.tryReceive()
		for ; ok; _, ok = p.mb.tryReceive() {
			counter++
		}
		rd.metricDropMessage.Add(float64(counter))
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
func (rd *ready) schedule(p *proc) error {
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
func (rd *ready) scheduleN(procs []*proc) {
	rd.Lock()
	for _, p := range procs {
		_ = rd.enqueueLocked(p, false)
	}
	rd.Unlock()
	rd.cond.Broadcast()
}

// batchReceiveProcs receives ready procs into batchP.
func (rd *ready) batchReceiveProcs(batchP []*proc) int {
	n := 0
	max := len(batchP)
	for i := 0; i < max; i++ {
		if rd.queue.Len() == 0 {
			// Stop receive if there is no more ready procs.
			break
		}
		element := rd.queue.Front()
		rd.queue.Remove(element)
		p := element.Value.(*proc)
		batchP[i] = p
		n++
	}
	return n
}

// Router send messages to actors.
type Router struct {
	rd *ready

	// Map of ID to proc
	procs sync.Map
}

func newRouter(name string) *Router {
	r := &Router{
		rd: &ready{},
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
func (r *Router) Send(id ID, msg message.Message) error {
	value, ok := r.procs.Load(id)
	if !ok {
		return errActorNotFound
	}
	p := value.(*proc)
	err := p.mb.Send(msg)
	if err != nil {
		return err
	}
	return r.rd.schedule(p)
}

// SendB sends a message to an actor, blocks when it's full.
// ErrActorNotFound when the actor not found.
// Canceled or DeadlineExceeded when the context is canceled or done.
func (r *Router) SendB(ctx context.Context, id ID, msg message.Message) error {
	value, ok := r.procs.Load(id)
	if !ok {
		return errActorNotFound
	}
	p := value.(*proc)
	err := p.mb.SendB(ctx, msg)
	if err != nil {
		return err
	}
	return r.rd.schedule(p)
}

// Broadcast a message to all actors in the router.
// The message may be dropped when a actor is full.
func (r *Router) Broadcast(msg message.Message) {
	batchSize := 128
	ps := make([]*proc, 0, batchSize)
	r.procs.Range(func(key, value interface{}) bool {
		p := value.(*proc)
		if err := p.mb.Send(msg); err != nil {
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

func (r *Router) insert(id ID, p *proc) error {
	_, exist := r.procs.LoadOrStore(id, p)
	if exist {
		return cerrors.ErrActorDuplicate.FastGenByArgs()
	}
	return nil
}

func (r *Router) remove(id ID) bool {
	_, present := r.procs.LoadAndDelete(id)
	return present
}

// SystemBuilder is a builder of a system.
type SystemBuilder struct {
	name                 string
	numWorker            int
	actorBatchSize       int
	msgBatchSizePerActor int

	fatalHandler func(string, ID)
}

// NewSystemBuilder returns a new system builder.
func NewSystemBuilder(name string) *SystemBuilder {
	defaultWorkerNum := maxWorkerNum
	goMaxProcs := runtime.GOMAXPROCS(0)
	if goMaxProcs*8 < defaultWorkerNum {
		defaultWorkerNum = goMaxProcs * 8
	}

	return &SystemBuilder{
		name:                 name,
		numWorker:            defaultWorkerNum,
		actorBatchSize:       defaultActorBatchSize,
		msgBatchSizePerActor: defaultMsgBatchSizePerActor,
	}
}

// WorkerNumber sets the number of workers of a system.
func (b *SystemBuilder) WorkerNumber(numWorker int) *SystemBuilder {
	if numWorker <= 0 {
		numWorker = 1
	} else if numWorker > maxWorkerNum {
		numWorker = maxWorkerNum
	}
	b.numWorker = numWorker
	return b
}

// Throughput sets the throughput per-poll of a system.
func (b *SystemBuilder) Throughput(
	actorBatchSize, msgBatchSizePerActor int,
) *SystemBuilder {
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
func (b *SystemBuilder) handleFatal(
	fatalHandler func(string, ID),
) *SystemBuilder {
	b.fatalHandler = fatalHandler
	return b
}

// Build builds a system and a router.
func (b *SystemBuilder) Build() (*System, *Router) {
	router := newRouter(b.name)
	metricWorkingDurations := make([]prometheus.Counter, b.numWorker)
	for i := range metricWorkingDurations {
		metricWorkingDurations[i] =
			workingDuration.WithLabelValues(b.name, strconv.Itoa(i))
	}
	return &System{
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
		metricPollDuration:     pollActorDuration.WithLabelValues(b.name),
		metricProcBatch:        batchSizeHistogram.WithLabelValues(b.name, "proc"),
		metricMsgBatch:         batchSizeHistogram.WithLabelValues(b.name, "msg"),
	}, router
}

// System is the runtime of Actors.
type System struct {
	name                 string
	numWorker            int
	actorBatchSize       int
	msgBatchSizePerActor int

	rd     *ready
	router *Router
	wg     *errgroup.Group
	cancel context.CancelFunc

	fatalHandler func(string, ID)

	// Metrics
	metricTotalWorkers     prometheus.Gauge
	metricWorkingWorkers   prometheus.Gauge
	metricWorkingDurations []prometheus.Counter
	metricPollDuration     prometheus.Observer
	metricProcBatch        prometheus.Observer
	metricMsgBatch         prometheus.Observer
}

// Start the system. Cancelling the context to stop the system.
// Start is not threadsafe.
func (s *System) Start(ctx context.Context) {
	s.wg, ctx = errgroup.WithContext(ctx)
	ctx, s.cancel = context.WithCancel(ctx)

	s.metricTotalWorkers.Add(float64(s.numWorker))
	for i := 0; i < s.numWorker; i++ {
		id := i
		s.wg.Go(func() error {
			defer pprof.SetGoroutineLabels(ctx)
			ctx = pprof.WithLabels(ctx, pprof.Labels("actor", s.name))
			pprof.SetGoroutineLabels(ctx)

			s.poll(ctx, id)
			return nil
		})
	}
}

// Stop the system, cancels all actors. It should be called after Start.
// Stop is not threadsafe.
func (s *System) Stop() error {
	s.metricTotalWorkers.Add(-float64(s.numWorker))
	if s.cancel != nil {
		s.cancel()
	}
	s.rd.stop()
	return s.wg.Wait()
}

// Spawn spawns an actor in the system.
// Spawn is threadsafe.
func (s *System) Spawn(mb Mailbox, actor Actor) error {
	id := mb.ID()
	p := &proc{mb: mb, actor: actor}
	return s.router.insert(id, p)
}

const slowReceiveThreshold = time.Second

// The main poll of actor system.
func (s *System) poll(ctx context.Context, id int) {
	batchPBuf := make([]*proc, s.actorBatchSize)
	batchMsgBuf := make([]message.Message, s.msgBatchSizePerActor)
	rd := s.rd
	rd.Lock()

	startTime := time.Now()
	s.metricWorkingWorkers.Inc()
	for {
		var batchP []*proc
		for {
			if rd.stopped {
				rd.Unlock()
				return
			}
			// Batch receive ready procs.
			n := rd.batchReceiveProcs(batchPBuf)
			if n != 0 {
				batchP = batchPBuf[:n]
				s.metricProcBatch.Observe(float64(n))
				break
			}
			// Recording metrics.
			s.metricWorkingDurations[id].Add(time.Since(startTime).Seconds())
			s.metricWorkingWorkers.Dec()
			// Park the poll until it is awakened.
			rd.cond.Wait()
			startTime = time.Now()
			s.metricWorkingWorkers.Inc()
		}
		rd.Unlock()

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
			s.metricMsgBatch.Observe(float64(n))

			// Poll actor.
			pollStartTime := time.Now()
			running := p.actor.Poll(ctx, batchMsg)
			if !running {
				// Close the actor.
				p.close()
			}
			receiveDuration := time.Since(pollStartTime)
			if receiveDuration > slowReceiveThreshold {
				log.Warn("actor handle received messages too slow",
					zap.Duration("duration", receiveDuration),
					zap.Uint64("id", uint64(p.mb.ID())),
					zap.String("name", s.name))
			}
			s.metricPollDuration.Observe(receiveDuration.Seconds())
		}

		rd.Lock()
		for _, p := range batchP {
			if p.mb.len() == 0 {
				// At this point, there is no more message needs to be polled
				// by the actor. Delete the actor from ready set.
				delete(rd.procs, p.mb.ID())
			} else {
				// Force enqueue to poll remaining messages.
				// Also it drops remaining messages if proc is stopped.
				_ = rd.enqueueLocked(p, true)
			}
			if p.isClosed() {
				// Remove closed actor from router.
				present := s.router.remove(p.mb.ID())
				if !present {
					s.handleFatal(
						"try to remove non-existent actor", p.mb.ID())
				}
			}
		}
	}
}

func (s *System) handleFatal(msg string, id ID) {
	handler := defaultFatalhandler
	if s.fatalHandler != nil {
		handler = s.fatalHandler
	}
	handler(msg, id)
}

func defaultFatalhandler(msg string, id ID) {
	log.Panic(msg, zap.Uint64("id", uint64(id)))
}
