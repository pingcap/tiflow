package statusutil

import (
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pkg/containers"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
)

const (
	maxPendingStatusNum = 1024
)

type Reader struct {
	q            containers.Queue[*libModel.WorkerStatus]
	lastReceived *libModel.WorkerStatus
}

func NewReader(init *libModel.WorkerStatus) *Reader {
	return &Reader{
		q:            containers.NewDeque[*libModel.WorkerStatus](),
		lastReceived: init,
	}
}

func (r *Reader) HasNewUpdate() bool {
	return r.q.Size() > 0
}

func (r *Reader) Receive() (*libModel.WorkerStatus, bool) {
	st, ok := r.q.Pop()
	if !ok {
		return nil, false
	}
	r.lastReceived = st
	return st, true
}

func (r *Reader) OnAsynchronousNotification(newStatus *libModel.WorkerStatus) error {
	if s := r.q.Size(); s > maxPendingStatusNum {
		return derror.ErrTooManyStatusUpdates.GenWithStackByArgs(s)
	}
	r.q.Add(newStatus)
	return nil
}

func (r *Reader) Status() *libModel.WorkerStatus {
	return r.lastReceived
}
