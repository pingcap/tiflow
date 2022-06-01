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

package worker

import (
	"time"

	"go.uber.org/atomic"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	"github.com/pingcap/tiflow/engine/pkg/errctx"
	derror "github.com/pingcap/tiflow/engine/pkg/errors"
)

type workerExitFsmState = int32

const (
	workerNormal = workerExitFsmState(iota + 1)
	workerHalfExit
	workerExited
)

const (
	workerExitWaitForMasterTimeout = time.Second * 15
)

// ExitController implements the exit sequence of
// a worker. This object is thread-safe.
type ExitController struct {
	workerExitFsm atomic.Int32
	halfExitTime  atomic.Time
	errCenter     *errctx.ErrCenter
	masterClient  MasterInfoProvider

	// clock is to facilitate unit testing.
	clock clock.Clock
}

// NewExitController returns a new ExitController.
func NewExitController(
	masterClient MasterInfoProvider,
	errCenter *errctx.ErrCenter,
	clock clock.Clock,
) *ExitController {
	return &ExitController{
		workerExitFsm: *atomic.NewInt32(workerNormal),
		errCenter:     errCenter,
		masterClient:  masterClient,
		clock:         clock,
	}
}

// PollExit is called in each tick of the worker.
// Returning an error other than ErrWorkerHalfExit
// means that the worker is ready to exit.
func (c *ExitController) PollExit() error {
	err := c.errCenter.CheckError()
	if err == nil {
		return nil
	}

	switch c.workerExitFsm.Load() {
	case workerNormal:
		c.workerExitFsm.CAS(workerNormal, workerHalfExit)
		c.halfExitTime.Store(c.clock.Now())
		return derror.ErrWorkerHalfExit.FastGenByArgs()
	case workerHalfExit:
		if c.masterClient.IsMasterSideClosed() {
			c.workerExitFsm.Store(workerExited)
			return err
		}
		// workerExitWaitForMasterTimeout is used for the case that
		// 'master is busy to reply or ignore reply for some bugs when heartbeat is ok'.
		// Master need wait (WorkerTimeoutDuration + WorkerTimeoutGracefulDuration) to know worker had already exited without halfExit,
		// so workerExitWaitForMasterTimeout < (WorkerTimeoutDuration + WorkerTimeoutGracefulDuration) is reasonable.
		sinceStartExiting := c.clock.Since(c.halfExitTime.Load())
		if sinceStartExiting > workerExitWaitForMasterTimeout {
			// TODO log worker ID and master ID.
			log.L().Warn("Exiting worker cannot get acknowledgement from master")
			return err
		}
		return derror.ErrWorkerHalfExit.FastGenByArgs()
	case workerExited:
		return err
	default:
		log.L().Panic("unreachable")
	}
	return nil
}

// ForceExit forces a quick exit without notifying the
// master. It should be used when suicide is required when
// we have lost contact with the master.
func (c *ExitController) ForceExit(errIn error) {
	c.errCenter.OnError(errIn)
	c.workerExitFsm.Store(workerExited)
}

// IsExiting indicates whether the worker is performing
// an exit sequence.
func (c *ExitController) IsExiting() bool {
	return c.workerExitFsm.Load() == workerHalfExit
}
