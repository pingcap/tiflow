package statusutil

import (
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/containers"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
)

const (
	maxPendingStatusNum = 1024
)

// Reader is used to cache worker status and can be consumed in a FIFO way.
type Reader struct {
	q            containers.Queue[*libModel.WorkerStatus]
	lastReceived *libModel.WorkerStatus
}

// NewReader creates a new Reader instance.
func NewReader(init *libModel.WorkerStatus) *Reader {
	return &Reader{
		q:            containers.NewDeque[*libModel.WorkerStatus](),
		lastReceived: init,
	}
}

// HasNewUpdate checks whether there is pending status in receiving queue.
func (r *Reader) HasNewUpdate() bool {
	return r.q.Size() > 0
}

// Receive pops a pending status from receiving queue and return it.
func (r *Reader) Receive() (*libModel.WorkerStatus, bool) {
	st, ok := r.q.Pop()
	if !ok {
		return nil, false
	}
	r.lastReceived = st
	return st, true
}

// OnAsynchronousNotification is used to cache new status, if the backoff queue
// is full, the status will be dropped and return ErrTooManyStatusUpdates.
func (r *Reader) OnAsynchronousNotification(newStatus *libModel.WorkerStatus) error {
	if s := r.q.Size(); s > maxPendingStatusNum {
		return derror.ErrTooManyStatusUpdates.GenWithStackByArgs(s)
	}
	r.q.Add(newStatus)
	return nil
}

// Status returns the latest received worker status
func (r *Reader) Status() *libModel.WorkerStatus {
	return r.lastReceived
}
