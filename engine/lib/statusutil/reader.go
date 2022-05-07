// Copyright 2022 PingCAP, Inc.
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

package statusutil

import (
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pkg/containers"
	derror "github.com/pingcap/tiflow/engine/pkg/errors"
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
