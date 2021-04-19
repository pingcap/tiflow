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

package owner

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

type Owner struct {
	etcdWorker *orchestrator.EtcdWorker
	reactor    *ownerReactor

	tickInterval time.Duration
}

func NewOwner(etcdClient *etcd.Client, pdClient pd.Client, credential *security.Credential) (*Owner, error) {
	state := model.NewGlobalState()
	bootstrapper := newBootstrapper(pdClient, credential)
	cfManager := newChangeFeedManager(state, bootstrapper)
	gcManager := newGCManager(pdClient, 600)
	reactor := newOwnerReactor(state, cfManager, gcManager)

	etcdWorker, err := orchestrator.NewEtcdWorker(etcdClient, kv.EtcdKeyBase, reactor, state)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Owner{
		etcdWorker:   etcdWorker,
		reactor:      reactor,
		tickInterval: 100 * time.Millisecond,
	}, nil
}

func (o *Owner) Run(ctx context.Context) error {
	failpoint.Inject("owner-run-with-error", func() {
		failpoint.Return(errors.New("owner run with injected error"))
	})
	// TODO pass session here
	err := o.etcdWorker.Run(ctx, nil, o.tickInterval)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (o *Owner) EnqueueJob(adminJob model.AdminJob) error {
	if o.reactor.changeFeedManager == nil {
		// TODO better error
		return errors.New("changeFeed manager is nil")
	}

	err := o.reactor.changeFeedManager.AddAdminJob(context.TODO(), adminJob)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO wait the admin job is executed and check the result, and we need a callback for admin job

	return nil
}

func (o *Owner) TriggerRebalance(cfID model.ChangeFeedID) {
	// TODO
}

func (o *Owner) ManualSchedule(cfID model.ChangeFeedID, toCapture model.CaptureID, tableID model.TableID) {
	// TODO
}

func (o *Owner) AsyncStop() {
	o.reactor.AsyncStop()
}

type ownerReactor struct {
	state             *ownerReactorState
	changeFeedManager changeFeedManager
	changeFeedRunners map[model.ChangeFeedID]changeFeedRunner

	gcManager *gcManager

	close int32
}

func newOwnerReactor(state *ownerReactorState, cfManager changeFeedManager, gcManager *gcManager) *ownerReactor {
	return &ownerReactor{
		state:             state,
		changeFeedManager: cfManager,
		changeFeedRunners: make(map[model.ChangeFeedID]changeFeedRunner),
		gcManager:         gcManager,
	}
}

func (o *ownerReactor) Tick(ctx context.Context, _ orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	if atomic.LoadInt32(&o.close) != 0 {
		return nil, cerror.ErrReactorFinished.GenWithStackByArgs()
	}
	cfOps, err := o.changeFeedManager.GetChangeFeedOperations(ctx)
	if err != nil {
		// TODO graceful exit
		return nil, errors.Trace(err)
	}

	for _, operation := range cfOps {
		switch operation.op {
		case startChangeFeedOperation:
			log.Info("start changeFeed", zap.String("cfID", operation.changeFeedID))
			o.changeFeedRunners[operation.changeFeedID] = operation.runner
		case stopChangeFeedOperation:
			log.Info("stop changeFeed", zap.String("cfID", operation.changeFeedID))
			// We try to close the changeFeedRunner only if it is not already closed.
			// It will already have been closed if the changeFeedRunner itself has returned an error in the last tick,
			// in which case, the changeFeedRunner is closed and an AdminStop is queued to notify the processors.
			// Since the changeFeedManager is ignorant of the status of the changeFeedRunner, it will process the AdminStop
			// and ask us to close the feed by returning a stopChangeFeedOperation.
			if _, ok := o.changeFeedRunners[operation.changeFeedID]; ok {
				o.changeFeedRunners[operation.changeFeedID].Close()
				delete(o.changeFeedRunners, operation.changeFeedID)
			}
		default:
			panic("unreachable")
		}
	}

	for cfID, runner := range o.changeFeedRunners {
		err := runner.Tick(ctx)
		if err != nil {
			log.Warn("error running changeFeed owner", zap.Error(err))
			err := o.changeFeedManager.AddAdminJob(ctx, model.AdminJob{
				CfID: cfID,
				Type: model.AdminStop,
				Error: &model.RunningError{
					Addr:    util.CaptureAddrFromCtx(ctx),
					Code:    "CDC-owner-1001",
					Message: err.Error(),
				},
			})
			if err != nil {
				// TODO is this error recoverable?
				return nil, errors.Trace(err)
			}
			o.changeFeedRunners[cfID].Close()
			delete(o.changeFeedRunners, cfID)
		}
	}

	err = o.doUpdateGCSafePoint(ctx)
	if err != nil {
		return nil, err
	}

	return o.state, nil
}

func (o *ownerReactor) doUpdateGCSafePoint(ctx context.Context) error {
	gcSafePoint := o.changeFeedManager.GetGCSafePointUpperBound()
	actual, err := o.gcManager.updateGCSafePoint(ctx, gcSafePoint)
	if err != nil {
		return errors.Trace(err)
	}

	if actual > gcSafePoint {
		log.Warn("gcSafePoint lost",
			zap.Uint64("expected", gcSafePoint),
			zap.Uint64("actual", actual))

		// TODO fail all changefeeds whose checkpoint ts is less than the actual safepoint
	}

	return nil
}

func (o *ownerReactor) AsyncStop() {
	atomic.StoreInt32(&o.close, 1)
}
