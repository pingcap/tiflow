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
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/clientv3"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/etcd"
	"github.com/pingcap/ticdc/pkg/orchestrator"
	"github.com/pingcap/ticdc/pkg/security"
	pd "github.com/tikv/pd/client"
)

const (
	// CDCServiceSafePointID is the ID of CDC service in pd.UpdateServiceGCSafePoint.
	cdcServiceSafePointID = "ticdc"
	// GCSafepointUpdateInterval is the minimual interval that CDC can update gc safepoint
	gcSafepointUpdateInterval = time.Duration(1 * time.Minute)
)

type Owner struct {
	etcdWorker *orchestrator.EtcdWorker
	reactor    *ownerReactor
	leaseID    clientv3.LeaseID

	tickInterval time.Duration
}

func NewOwner(etcdClient *etcd.Client, pdClient pd.Client, credential *security.Credential, leaseID clientv3.LeaseID) (*Owner, error) {
	state := model.NewGlobalState()
	reactor := newOwnerReactor(pdClient, credential, newGCManager(pdClient), leaseID)

	etcdWorker, err := orchestrator.NewEtcdWorker(etcdClient, kv.EtcdKeyBase, reactor, state)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Owner{
		etcdWorker:   etcdWorker,
		reactor:      reactor,
		tickInterval: 100 * time.Millisecond,
		leaseID:      leaseID,
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

func (o *Owner) EnqueueJob(adminJob model.AdminJob) {
	o.reactor.pushOwnerJob(&ownerJob{
		tp:           ownerJobTypeAdminJob,
		adminJob:     &adminJob,
		changefeedID: adminJob.CfID,
	})
}

func (o *Owner) TriggerRebalance(cfID model.ChangeFeedID) {
	o.reactor.pushOwnerJob(&ownerJob{
		tp:           ownerJobTypeRebalance,
		changefeedID: cfID,
	})
}

func (o *Owner) ManualSchedule(cfID model.ChangeFeedID, toCapture model.CaptureID, tableID model.TableID) {
	o.reactor.pushOwnerJob(&ownerJob{
		tp:              ownerJobTypeManualSchedule,
		changefeedID:    cfID,
		targetCaptureID: toCapture,
		tableID:         tableID,
	})
}

func (o *Owner) AsyncStop() {
	o.reactor.AsyncStop()
}

type ownerJobType int

// All AdminJob types
const (
	ownerJobTypeRebalance ownerJobType = iota
	ownerJobTypeManualSchedule
	ownerJobTypeAdminJob
)

type ownerJob struct {
	tp           ownerJobType
	changefeedID model.ChangeFeedID

	// for ManualSchedule only
	targetCaptureID model.CaptureID
	// for ManualSchedule only
	tableID model.TableID

	// for Admin Job only
	adminJob *model.AdminJob
}

type ownerReactor struct {
	changefeeds map[model.ChangeFeedID]*changefeed

	pdClient   pd.Client
	credential *security.Credential
	gcManager  *gcManager

	ownerJobQueue   []*ownerJob
	ownerJobQueueMu sync.Mutex

	leaseID clientv3.LeaseID

	close int32
}

func newOwnerReactor(pdClient pd.Client, credential *security.Credential, gcManager *gcManager, leaseID clientv3.LeaseID) *ownerReactor {
	return &ownerReactor{
		changefeeds: make(map[model.ChangeFeedID]*changefeed),
		pdClient:    pdClient,
		credential:  credential,
		gcManager:   gcManager,
		leaseID:     leaseID,
	}
}

func (o *ownerReactor) Tick(ctx context.Context, rawState orchestrator.ReactorState) (nextState orchestrator.ReactorState, err error) {
	state := rawState.(*model.GlobalReactorState)
	state.CheckLeaseExpired(o.leaseID)
	o.handleJob()
	for changefeedID, changefeedState := range state.Changefeeds {
		if changefeedState.Info == nil {
			o.cleanUpChangefeed(changefeedState)
			continue
		}
		cfReactor, exist := o.changefeeds[changefeedID]
		if !exist {
			cfReactor = newChangefeed(o.pdClient, o.credential, o.gcManager)
		}
		cfReactor.Tick(ctx, changefeedState, state.Captures)
	}
	if len(o.changefeeds) != len(state.Changefeeds) {
		for changefeedID, cfReactor := range o.changefeeds {
			if _, exist := state.Changefeeds[changefeedID]; exist {
				continue
			}
			cfReactor.Close()
			delete(o.changefeeds, changefeedID)
		}
	}

	err = o.gcManager.updateGCSafePoint(ctx, state)
	if err != nil {
		return nil, err
	}
	if atomic.LoadInt32(&o.close) != 0 {
		for _, cfReactor := range o.changefeeds {
			cfReactor.Close()
		}
		return state, cerror.ErrReactorFinished.GenWithStackByArgs()
	}
	return state, nil
}

func (o *ownerReactor) cleanUpChangefeed(state *model.ChangefeedReactorState) {
	state.PatchInfo(func(info *model.ChangeFeedInfo) (*model.ChangeFeedInfo, bool, error) {
		return nil, info != nil, nil
	})
	state.PatchStatus(func(status *model.ChangeFeedStatus) (*model.ChangeFeedStatus, bool, error) {
		return nil, status != nil, nil
	})
	for captureID := range state.TaskStatuses {
		state.PatchTaskStatus(captureID, func(status *model.TaskStatus) (*model.TaskStatus, bool, error) {
			return nil, status != nil, nil
		})
	}
	for captureID := range state.TaskPositions {
		state.PatchTaskPosition(captureID, func(position *model.TaskPosition) (*model.TaskPosition, bool, error) {
			return nil, position != nil, nil
		})
	}
	for captureID := range state.Workloads {
		state.PatchTaskWorkload(captureID, func(workload model.TaskWorkload) (model.TaskWorkload, bool, error) {
			return nil, workload != nil, nil
		})
	}
}

func (o *ownerReactor) handleJob() {
	jobs := o.takeOnwerJobs()
	for _, job := range jobs {
		changefeedID := job.changefeedID
		cfReactor, exist := o.changefeeds[changefeedID]
		if !exist {
			log.Warn("") // TODO
		}
		switch job.tp {
		case ownerJobTypeAdminJob:
			cfReactor.feedStateManager.PushAdminJob(job.adminJob)
		case ownerJobTypeManualSchedule:
			cfReactor.scheduler.MoveTable(job.tableID, job.targetCaptureID)
		case ownerJobTypeRebalance:
			cfReactor.scheduler.Rebalance()
		}
	}
}

func (o *ownerReactor) takeOnwerJobs() []*ownerJob {
	o.ownerJobQueueMu.Lock()
	defer o.ownerJobQueueMu.Unlock()

	jobs := o.ownerJobQueue
	o.ownerJobQueue = nil
	return jobs
}

func (o *ownerReactor) pushOwnerJob(job *ownerJob) {
	o.ownerJobQueueMu.Lock()
	defer o.ownerJobQueueMu.Unlock()
	o.ownerJobQueue = append(o.ownerJobQueue, job)
}

func (o *ownerReactor) AsyncStop() {
	atomic.StoreInt32(&o.close, 1)
}
