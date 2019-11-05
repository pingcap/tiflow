// Copyright 2019 PingCAP, Inc.
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

package roles

import (
	"context"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/model"
	"github.com/pingcap/tidb-cdc/cdc/roles/storage"
	"go.uber.org/zap"
)

// Owner is used to process etcd information for a capture with owner role
type Owner interface {
	// GetResolvedTS gets resolvedTS of a ChangeFeed
	GetResolvedTS(changeFeedID string) (uint64, error)

	// GetCheckpointTS gets CheckpointTS of a ChangeFeed
	GetCheckpointTS(changeFeedID string) (uint64, error)

	// Run a goroutine to handle Owner logic
	Run(ctx context.Context, tickTime time.Duration) error

	// IsOwner checks whether it has campaigned as owner
	IsOwner(ctx context.Context) bool
}

// OwnerDDLHandler defines the ddl handler for Owner
// which can pull ddl jobs and execute ddl jobs
type OwnerDDLHandler interface {

	// PullDDL pulls the ddl jobs and returns resolvedTS of DDL Puller and job list.
	PullDDL() (resolvedTS uint64, jobs []*timodel.Job, err error)

	// ExecDDL executes the ddl job
	ExecDDL(*timodel.Job) error
}

// ChangeFeedInfoRWriter defines the Reader and Writer for ChangeFeedInfo
type ChangeFeedInfoRWriter interface {
	// Read the changefeed info from storage such as etcd.
	Read(ctx context.Context) (map[model.ChangeFeedID]model.ProcessorsInfos, error)
	// Write the changefeed info to storage such as etcd.
	Write(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedInfo) error
}

// TODO edit sub change feed
type ownerImpl struct {
	changeFeedInfos map[model.ChangeFeedID]*model.ChangeFeedInfo

	ddlHandler OwnerDDLHandler
	cfRWriter  ChangeFeedInfoRWriter

	ddlResolvedTS uint64
	targetTS      uint64
	ddlJobHistory []*timodel.Job

	l sync.RWMutex

	etcdClient *clientv3.Client
	manager    Manager
}

// NewOwner creates a new ownerImpl instance
func NewOwner(targetTS uint64, cli *clientv3.Client, manager Manager) *ownerImpl {
	owner := &ownerImpl{
		changeFeedInfos: make(map[model.ChangeFeedID]*model.ChangeFeedInfo),
		ddlHandler:      NewDDLHandler(),
		cfRWriter:       storage.NewChangeFeedInfoEtcdRWriter(cli),
		etcdClient:      cli,
		manager:         manager,
	}
	return owner
}

func (o *ownerImpl) loadChangeFeedInfos(ctx context.Context) error {
	infos, err := o.cfRWriter.Read(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: handle changefeed changed and the table of sub changefeed changed
	// TODO: find the first index of one changefeed in ddl jobs
	for changeFeedID, etcdChangeFeedInfo := range infos {
		// new changefeed, no subchangefeed assignd
		if len(etcdChangeFeedInfo) == 0 {
			createdInfo, err := o.assignChangeFeed(ctx, changeFeedID)
			if err != nil {
				return err
			}
			etcdChangeFeedInfo = createdInfo
		}

		if cfInfo, exist := o.changeFeedInfos[changeFeedID]; exist {
			cfInfo.ProcessorInfos = etcdChangeFeedInfo
		} else {
			o.changeFeedInfos[changeFeedID] = &model.ChangeFeedInfo{
				Status:          model.ChangeFeedSyncDML,
				ResolvedTS:      0,
				CheckpointTS:    0,
				ProcessorInfos:  etcdChangeFeedInfo,
				DDLCurrentIndex: 0,
			}
		}
	}
	return nil
}

func (o *ownerImpl) flushChangeFeedInfos(ctx context.Context) error {
	return errors.Trace(o.cfRWriter.Write(ctx, o.changeFeedInfos))
}

func (o *ownerImpl) pullDDLJob() error {
	ddlResolvedTS, ddlJobs, err := o.ddlHandler.PullDDL()
	if err != nil {
		return errors.Trace(err)
	}
	o.ddlResolvedTS = ddlResolvedTS
	o.ddlJobHistory = append(o.ddlJobHistory, ddlJobs...)
	return nil
}

func (o *ownerImpl) getChangeFeedInfo(changeFeedID string) (*model.ChangeFeedInfo, error) {
	info, exist := o.changeFeedInfos[changeFeedID]
	if !exist {
		return nil, errors.NotFoundf("ChangeFeed(%s) in ChangeFeedInfos", changeFeedID)
	}
	return info, nil
}

func (o *ownerImpl) GetResolvedTS(changeFeedID string) (uint64, error) {
	o.l.RLock()
	defer o.l.RUnlock()
	cfInfo, err := o.getChangeFeedInfo(changeFeedID)
	if err != nil {
		return 0, err
	}
	return cfInfo.ResolvedTS, nil
}

func (o *ownerImpl) GetCheckpointTS(changeFeedID string) (uint64, error) {
	o.l.RLock()
	defer o.l.RUnlock()
	cfInfo, err := o.getChangeFeedInfo(changeFeedID)
	if err != nil {
		return 0, err
	}
	return cfInfo.CheckpointTS, nil
}

func (o *ownerImpl) calcResolvedTS() error {
	for _, cfInfo := range o.changeFeedInfos {
		if cfInfo.Status != model.ChangeFeedSyncDML {
			continue
		}
		minResolvedTS := o.targetTS

		// calc the min of all resolvedTS in captures
		for _, pStatus := range cfInfo.ProcessorInfos {
			if minResolvedTS > pStatus.ResolvedTS {
				minResolvedTS = pStatus.ResolvedTS
			}
		}

		// if minResolvedTS is greater than ddlResolvedTS,
		// it means that ddlJobHistory in memory is not intact,
		// there are some ddl jobs which finishedTS is smaller than minResolvedTS we don't know.
		// so we need to call `pullDDLJob`, update the ddlJobHistory and ddlResolvedTS.
		if minResolvedTS > o.ddlResolvedTS {
			if err := o.pullDDLJob(); err != nil {
				return errors.Trace(err)
			}
			if minResolvedTS > o.ddlResolvedTS {
				minResolvedTS = o.ddlResolvedTS
			}
		}

		// if minResolvedTS is greater than the finishedTS of ddl job which is not executed,
		// we need to execute this ddl job
		if len(o.ddlJobHistory) > cfInfo.DDLCurrentIndex &&
			minResolvedTS > o.ddlJobHistory[cfInfo.DDLCurrentIndex].BinlogInfo.FinishedTS {
			minResolvedTS = o.ddlJobHistory[cfInfo.DDLCurrentIndex].BinlogInfo.FinishedTS
			cfInfo.Status = model.ChangeFeedWaitToExecDDL
		}
		cfInfo.ResolvedTS = minResolvedTS
	}
	return nil
}

func (o *ownerImpl) handleDDL(ctx context.Context) error {
waitCheckpointTSLoop:
	for changeFeedID, cfInfo := range o.changeFeedInfos {
		if cfInfo.Status != model.ChangeFeedWaitToExecDDL {
			continue waitCheckpointTSLoop
		}
		todoDDLJob := o.ddlJobHistory[cfInfo.DDLCurrentIndex]

		// Check if all the checkpointTs of capture are achieving global resolvedTS(which is equal to todoDDLJob.FinishedTS)
		for _, pInfo := range cfInfo.ProcessorInfos {
			if pInfo.CheckPointTS != todoDDLJob.BinlogInfo.FinishedTS {
				continue waitCheckpointTSLoop
			}
		}

		// Execute DDL Job asynchronously
		cfInfo.Status = model.ChangeFeedExecDDL
		go func(changeFeedID string, cfInfo *model.ChangeFeedInfo) {
			err := o.ddlHandler.ExecDDL(todoDDLJob)
			o.l.Lock()
			defer o.l.Unlock()
			// If DDL executing failed, pause the changefeed and print log
			if err != nil {
				cfInfo.Status = model.ChangeFeedDDLExecuteFailed
				log.Error("Execute DDL failed",
					zap.String("ChangeFeedID", changeFeedID),
					zap.Error(err),
					zap.Reflect("ddlJob", todoDDLJob))
				return
			}
			if cfInfo.Status != model.ChangeFeedExecDDL {
				log.Fatal("changeFeedState must be ChangeFeedExecDDL when DDL is executed",
					zap.String("ChangeFeedID", changeFeedID),
					zap.String("ChangeFeedState", cfInfo.Status.String()))
			}
			cfInfo.DDLCurrentIndex += 1
			cfInfo.Status = model.ChangeFeedSyncDML
		}(changeFeedID, cfInfo)
	}
	return nil
}

func (o *ownerImpl) Run(ctx context.Context, tickTime time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tickTime):
			if !o.IsOwner(ctx) {
				continue
			}
			err := o.run(ctx)
			if err != nil {
				return err
			}
		}
	}
}

func (o *ownerImpl) run(ctx context.Context) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-cctx.Done():
		case <-o.manager.RetireNotify():
			cancel()
		}
	}()

	o.l.Lock()
	defer o.l.Unlock()
	err := o.loadChangeFeedInfos(cctx)
	if err != nil {
		return errors.Trace(err)
	}
	err = o.calcResolvedTS()
	if err != nil {
		return errors.Trace(err)
	}
	err = o.handleDDL(cctx)
	if err != nil {
		return errors.Trace(err)
	}
	err = o.flushChangeFeedInfos(cctx)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (o *ownerImpl) IsOwner(_ context.Context) bool {
	return o.manager.IsOwner()
}

// assignChangeFeed handels newly added changefeed with following steps:
// 1. assign tables to captures
// 2. create subchangefeed info for each capture, and persist to storage
func (o *ownerImpl) assignChangeFeed(ctx context.Context, changefeedID string) (model.ProcessorsInfos, error) {
	cinfo, err := kv.GetChangeFeedDetail(ctx, o.etcdClient, changefeedID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(cinfo.TableIDs) == 0 {
		return nil, errors.Errorf("no table ids in changefeed %s", changefeedID)
	}

	_, captures, err := kv.GetCaptures(ctx, o.etcdClient)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(captures) == 0 {
		return nil, errors.New("no available capture")
	}

	result := make(map[string]*model.SubChangeFeedInfo, len(captures))

	// assign tables with simple round robin
	tableInfos := make([][]*model.ProcessTableInfo, len(captures))
	for _, id := range cinfo.TableIDs {
		captureIndex := id % uint64(len(captures))
		tableInfos[captureIndex] = append(tableInfos[captureIndex], &model.ProcessTableInfo{
			StartTS: cinfo.StartTS,
			ID:      id,
		})
	}

	// persist changefeed synchronization status to storage
	err = kv.PutChangeFeedStatus(ctx, o.etcdClient, changefeedID,
		&model.ChangeFeedInfo{
			CheckpointTS: cinfo.StartTS,
			ResolvedTS:   0,
		})
	if err != nil {
		return nil, err
	}

	// create subchangefeed info and persist to storage
	for i := range tableInfos {
		key := kv.GetEtcdKeySubChangeFeed(changefeedID, captures[i].ID)
		info := &model.SubChangeFeedInfo{
			CheckPointTS: 0, // TODO: refine checkpointTS
			ResolvedTS:   0,
			TableInfos:   tableInfos[i],
		}
		sinfo, err := info.Marshal()
		if err != nil {
			return nil, err
		}
		_, err = o.etcdClient.Put(ctx, key, sinfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		result[captures[i].ID] = info
	}

	return result, nil
}
