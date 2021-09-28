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
	"sync"
	"time"

	"github.com/pingcap/log"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Actor is a universal primitive of concurrent computation.
// See more https://en.wikipedia.org/wiki/Actor_model
type Actor interface {
	// Poll handles messages that are sent to actor's mailbox.
	//
	// The ctx is only for cancellation, and an actor must be aware of
	// the cancellation.
	//
	// If it returns true, then the actor will be rescheduled and polled later.
	// If it returns false, then the actor will be removed from Router and
	// polled if there are still messages in its mailbox.
	// Once it returns false, it must always return false.
	//
	// We choose message to have a concrete type instead of an interface to save
	// memory allocation.
	Poll(ctx context.Context, msgs []pipeline.Message) (closed bool)
}

// ID is ID of actors.
type ID int

// Mailbox sends messages to an actor.
// Mailbox is threadsafe.
type Mailbox interface {
	ID() ID
	// Send a message to its actor.
	// It's a non-blocking send, returns ErrMailboxFull when it's full.
	Send(msg pipeline.Message) error
	// SendB sends a message to its actor, blocks when it's full.
	// It may return context.Canceled or context.DeadlineExceeded.
	SendB(ctx context.Context, msg pipeline.Message) error

	// Try to receive a message.
	// It is must not block and should only be called by System.
	tryReceive() (pipeline.Message, bool)
	// Return the length of a mailbox.
	// It should only be called by System.
	len() int
}

// NewMailbox creates a fixed capacity mailbox.
func NewMailbox(id ID, cap int) Mailbox {
	return &mailbox{
		id: id,
		ch: make(chan pipeline.Message, cap),
	}
}

var _ Mailbox = (*mailbox)(nil)

type mailbox struct {
	id ID
	ch chan pipeline.Message
}

func (m *mailbox) ID() ID {
	return m.id
}

var errMailboxFull = cerrors.ErrMailboxFull.FastGenByArgs()

func (m *mailbox) Send(msg pipeline.Message) error {
	select {
	case m.ch <- msg:
		return nil
	default:
		return errMailboxFull
	}
}

func (m *mailbox) SendB(ctx context.Context, msg pipeline.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.ch <- msg:
		return nil
	}
}

func (m *mailbox) tryReceive() (msg pipeline.Message, ok bool) {
	select {
	case msg, ok = <-m.ch:
		return
	default:
	}
	return
}

func (m *mailbox) len() int {
	return len(m.ch)
}

// proc is wrapper of a running actor.
type proc struct {
	mb     Mailbox
	actor  Actor
	closed bool
}

// ready is a centralize notification struct, shared by a router and a system.
// It schedules notification and actors.
type ready struct {
	sync.Mutex
	cond *sync.Cond

	// TODO: replace with a memory efficient queue,
	// e.g., an array based queue to save allocation.
	queue   list.List
	procs   map[ID]struct{}
	stopped bool
}

func (rd *ready) stop() {
	rd.Lock()
	rd.stopped = true
	rd.Unlock()
	rd.cond.Broadcast()
}

func (rd *ready) enqueueLocked(p *proc, force bool) {
	id := p.mb.ID()
	if _, ok := rd.procs[id]; !ok || force {
		rd.queue.PushBack(p)
		rd.procs[id] = struct{}{}
	}
}

func (rd *ready) schedule(p *proc) {
	rd.Lock()
	rd.enqueueLocked(p, false)
	rd.Unlock()
	rd.cond.Signal()
}

func (rd *ready) scheduleN(procs []*proc) {
	rd.Lock()
	for _, p := range procs {
		rd.enqueueLocked(p, false)
	}
	rd.Unlock()
	rd.cond.Broadcast()
}

// Router send messages to actors.
type Router struct {
	rd *ready

	// Map of ID to proc
	procs sync.Map
}

func newRouter() *Router {
	r := &Router{
		rd: &ready{},
	}
	r.rd.cond = sync.NewCond(&r.rd.Mutex)
	r.rd.procs = make(map[ID]struct{})
	r.rd.queue.Init()
	return r
}

var errActorNotFound = cerrors.ErrActorNotFound.FastGenByArgs()

// Send a message to an actor. It's a non-blocking send.
// ErrMailboxFull when the actor full.
// ErrActorNotFound when the actor not found.
func (r *Router) Send(id ID, msg pipeline.Message) error {
	value, ok := r.procs.Load(id)
	if !ok {
		return errActorNotFound
	}
	p := value.(*proc)
	err := p.mb.Send(msg)
	if err != nil {
		return err
	}
	r.rd.schedule(p)
	return nil
}

// SendB sends a message to an actor, blocks when it's full.
// ErrActorNotFound when the actor not found.
// Canceled or DeadlineExceeded when the context is canceled or done.
func (r *Router) SendB(ctx context.Context, id ID, msg pipeline.Message) error {
	value, ok := r.procs.Load(id)
	if !ok {
		return errActorNotFound
	}
	p := value.(*proc)
	err := p.mb.SendB(ctx, msg)
	if err != nil {
		return err
	}
	r.rd.schedule(p)
	return nil
}

// Broadcast a message to all actors in the router.
// The message may be dropped when a actor is full.
func (r *Router) Broadcast(msg pipeline.Message) {
	batchSize := 128
	ps := make([]*proc, 0, batchSize)
	r.procs.Range(func(key, value interface{}) bool {
		p := value.(*proc)
		if err := p.mb.Send(msg); err != nil {
			log.Warn("failed to send to message",
				zap.Error(err), zap.Uint64("id", uint64(p.mb.ID())),
				zap.Reflect("msg", msg))
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
	return
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

// The max number of workers of a system.
const maxWorkerNum = 64

// The default size of polled actor batch.
const defaultActorBatchSize = 1

// The default size of receive message batch.
const defaultMsgBatchSizePerActor = 64

// NewSystemBuilder returns a new system builder.
func NewSystemBuilder(name string) *SystemBuilder {
	defaultWorkerNum := maxWorkerNum
	goMaxProcs := runtime.GOMAXPROCS(0)
	if goMaxProcs*8 < defaultWorkerNum {
		defaultWorkerNum = goMaxProcs * 8
	}

	return &SystemBuilder{
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
	router := newRouter()
	return &System{
		name:                 b.name,
		numWorker:            b.numWorker,
		actorBatchSize:       b.actorBatchSize,
		msgBatchSizePerActor: b.msgBatchSizePerActor,

		rd:     router.rd,
		router: router,

		fatalHandler: b.fatalHandler,

		metricTotalWorkers:    totalWorkers.WithLabelValues(b.name),
		metricWorkingWorkers:  workingWorkers.WithLabelValues(b.name),
		metricWorkingDuration: workingDuration.WithLabelValues(b.name),
		metricPollDuration:    pollMsgDuration.WithLabelValues(b.name),
		metricProcBatch:       batchSizeHistogram.WithLabelValues(b.name, "proc"),
		metricMsgBatch:        batchSizeHistogram.WithLabelValues(b.name, "msg"),
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
	metricTotalWorkers    prometheus.Gauge
	metricWorkingWorkers  prometheus.Gauge
	metricWorkingDuration prometheus.Counter
	metricPollDuration    prometheus.Observer
	metricProcBatch       prometheus.Observer
	metricMsgBatch        prometheus.Observer
}

// Start the system. Cancelling the context to stop the system.
// Start is not threadsafe.
func (s *System) Start(ctx context.Context) {
	s.wg, ctx = errgroup.WithContext(ctx)
	ctx, s.cancel = context.WithCancel(ctx)

	s.metricTotalWorkers.Add(float64(s.numWorker))
	for i := 0; i < s.numWorker; i++ {
		s.wg.Go(func() error {
			s.poll(ctx)
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
func (s *System) poll(ctx context.Context) {
	batchP := make([]*proc, 0, s.actorBatchSize)
	batchMsg := make([]pipeline.Message, 0, s.msgBatchSizePerActor)
	s.metricProcBatch.Observe(float64(len(batchP)))
	rd := s.rd
	rd.Lock()

	startTime := time.Now()
	s.metricWorkingWorkers.Add(1)
	for {
		batchP = batchP[:0]
		for {
			if rd.stopped {
				rd.Unlock()
				return
			}
			for i := 0; i < s.actorBatchSize; i++ {
				if rd.queue.Len() == 0 {
					break
				}
				element := rd.queue.Front()
				rd.queue.Remove(element)
				p := element.Value.(*proc)
				batchP = append(batchP, p)
			}
			if len(batchP) != 0 {
				break
			}
			// Recording metrics.
			s.metricWorkingDuration.Add(time.Since(startTime).Seconds())
			s.metricWorkingWorkers.Add(-1)
			rd.cond.Wait()
			startTime = time.Now()
			s.metricWorkingWorkers.Add(1)
		}
		rd.Unlock()

		for _, p := range batchP {
			batchMsg = batchMsg[:0]
			for i := 0; i < s.msgBatchSizePerActor; i++ {
				msg, ok := p.mb.tryReceive()
				if !ok {
					break
				}
				batchMsg = append(batchMsg, msg)
			}
			if len(batchMsg) == 0 {
				continue
			}
			s.metricMsgBatch.Observe(float64(len(batchMsg)))

			pollStartTime := time.Now()
			close := !p.actor.Poll(ctx, batchMsg)
			if close {
				if !p.closed {
					p.closed = true
					present := s.router.remove(p.mb.ID())
					if !present {
						s.handleFatal(
							"try to remove non-existent actor", p.mb.ID())
					}
				}
			}
			if p.closed && !close {
				s.handleFatal(
					"closed actor can never receive new messages again",
					p.mb.ID())
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
				delete(rd.procs, p.mb.ID())
			} else {
				// Force enqueue to poll remaining messages.
				rd.enqueueLocked(p, true)
			}
		}
	}
}

func (s *System) handleFatal(msg string, id ID) {
	handler := defautlFatalhandler
	if s.fatalHandler != nil {
		handler = s.fatalHandler
	}
	handler(msg, id)
}

func defautlFatalhandler(msg string, id ID) {
	log.Panic(msg, zap.Uint64("id", uint64(id)))
}
