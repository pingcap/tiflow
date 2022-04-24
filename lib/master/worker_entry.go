package master

import (
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/lib/statusutil"
	"github.com/hanfei1991/microcosm/model"
)

// workerEntryState is the state of a worker
// internal to WorkerManager. It is NOT part of
// the public API of Dataflow Engine.
type workerEntryState int32

const (
	workerEntryWait = workerEntryState(iota + 1)
	workerEntryCreated
	workerEntryNormal
	workerEntryOffline
	workerEntryTombstone
)

// The following is the state-transition diagram.
// Refer to ../doc/worker_entry_fsm.puml for a UML version.
//
// workerEntryCreated            workerEntryWait
//      │  │                            │  │
//      │  │                            │  │
//      │ heartbeat              heartbeat │
//      │  │                            │  │
//      │  │                            │  │
//      │  └────► workerEntryNormal ◄───┘  │
//      │         │                        │
//      │         │                        │
//    timeout   timeout                  timeout
//      │         │                        │
//      ▼         ▼                        ▼
// workerEntryOffline ─────────► workerEntryTombstone
//                    callback

// workerEntry records the state of a worker managed by
// WorkerManager.
type workerEntry struct {
	id         libModel.WorkerID
	executorID model.ExecutorID

	mu       sync.Mutex
	expireAt time.Time
	state    workerEntryState

	statusReaderMu sync.RWMutex
	statusReader   *statusutil.Reader
}

func newWorkerEntry(
	id libModel.WorkerID,
	executorID model.ExecutorID,
	expireAt time.Time,
	state workerEntryState,
	initWorkerStatus *libModel.WorkerStatus,
) *workerEntry {
	var stReader *statusutil.Reader
	if initWorkerStatus != nil {
		stReader = statusutil.NewReader(initWorkerStatus)
	}
	return &workerEntry{
		id:           id,
		executorID:   executorID,
		expireAt:     expireAt,
		state:        state,
		statusReader: stReader,
	}
}

func newWaitingWorkerEntry(
	id libModel.WorkerID,
	lastStatus *libModel.WorkerStatus,
) *workerEntry {
	return newWorkerEntry(id, "", time.Time{}, workerEntryWait, lastStatus)
}

// String implements fmt.Stringer, note the implementation is not thread safe
func (e *workerEntry) String() string {
	return fmt.Sprintf("{worker-id:%s, executor-id:%s, state:%d}",
		e.id, e.executorID, e.state)
}

func (e *workerEntry) State() workerEntryState {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.state
}

func (e *workerEntry) MarkAsTombstone() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.state == workerEntryWait || e.state == workerEntryOffline {
		// Only workerEntryWait and workerEntryOffline are allowed
		// to transition to workerEntryTombstone.
		e.state = workerEntryTombstone
		return
	}

	log.L().Panic("Unreachable", zap.Stringer("entry", e))
}

func (e *workerEntry) IsTombstone() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.state == workerEntryTombstone
}

func (e *workerEntry) MarkAsOnline(executor model.ExecutorID, expireAt time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.state == workerEntryCreated || e.state == workerEntryWait {
		e.state = workerEntryNormal
		e.expireAt = expireAt
		e.executorID = executor
		return
	}

	log.L().Panic("Unreachable", zap.Stringer("entry", e))
}

func (e *workerEntry) MarkAsOffline() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.state == workerEntryCreated || e.state == workerEntryNormal {
		e.state = workerEntryOffline
		return
	}

	log.L().Panic("Unreachable", zap.Stringer("entry", e))
}

func (e *workerEntry) StatusReader() *statusutil.Reader {
	e.statusReaderMu.RLock()
	defer e.statusReaderMu.RUnlock()

	return e.statusReader
}

func (e *workerEntry) InitStatus(status *libModel.WorkerStatus) {
	e.statusReaderMu.Lock()
	defer e.statusReaderMu.Unlock()

	if e.statusReader != nil {
		log.L().Panic("double InitStatus", zap.Stringer("entry", e))
	}

	e.statusReader = statusutil.NewReader(status)
}

func (e *workerEntry) SetExpireTime(expireAt time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.expireAt = expireAt
}

func (e *workerEntry) ExpireTime() time.Time {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.expireAt
}
