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
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"
)

type ProcessTableInfo struct {
	ID      uint64 `json:"id"`
	StartTS uint64 `json:"start-ts"`
}

// SubChangeFeedInfo records the process information of a capture
type SubChangeFeedInfo struct {
	// The maximum event CommitTS that has been synchronized. This is updated by corresponding processor.
	CheckPointTS uint64 `json:"checkpoint-ts"`
	// The event that satisfies CommitTS <= ResolvedTS can be synchronized. This is updated by corresponding processor.
	ResolvedTS uint64 `json:"resolved-ts"`
	// Table information list, containing tables that processor should process, updated by ownrer, processor is read only.
	TableInfos []*ProcessTableInfo `json:"table-infos"`
}

func (scfi *SubChangeFeedInfo) String() string {
	data, err := json.Marshal(scfi)
	if err != nil {
		log.Error("fail to marshal ChangeFeedDetail to json", zap.Error(err))
	}
	return string(data)
}

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

type CaptureID = string
type ChangeFeedID = string
type ProcessorsInfos = map[CaptureID]*SubChangeFeedInfo

type ChangeFeedStatus int

const (
	ChangeFeedUnknown ChangeFeedStatus = iota
	ChangeFeedSyncDML
	ChangeFeedWaitToExecDDL
	ChangeFeedExecDDL
	ChangeFeedDDLExecuteFailed
)

func (s ChangeFeedStatus) String() string {
	switch s {
	case ChangeFeedUnknown:
		return "Unknown"
	case ChangeFeedSyncDML:
		return "SyncDML"
	case ChangeFeedWaitToExecDDL:
		return "WaitToExecDDL"
	case ChangeFeedExecDDL:
		return "ExecDDL"
	case ChangeFeedDDLExecuteFailed:
		return "DDLExecuteFailed"
	}
	return ""
}

// ChangeFeedInfo stores information about a ChangeFeed
type ChangeFeedInfo struct {
	status       ChangeFeedStatus
	resolvedTS   uint64
	checkpointTS uint64

	processorInfos  ProcessorsInfos
	ddlCurrentIndex int
}

// Status gets the status of the changefeed info.
func (c *ChangeFeedInfo) Status() ChangeFeedStatus {
	return c.status
}

// ResolvedTS gets the resolvedTS for the changefeed info.
func (c *ChangeFeedInfo) ResolvedTS() uint64 {
	return c.resolvedTS
}

// CheckpointTS gets the checkpointTS for the changefeed info.
func (c *ChangeFeedInfo) CheckpointTS() uint64 {
	return c.checkpointTS
}

// OwnerDDLHandler defines the ddl handler for Owner
// which can pull ddl jobs and execute ddl jobs
type OwnerDDLHandler interface {

	// PullDDL pulls the ddl jobs and returns resolvedTS of DDL Puller and job list.
	PullDDL() (resolvedTS uint64, jobs []*model.Job, err error)

	// ExecDDL executes the ddl job
	ExecDDL(*model.Job) error
}

// ChangeFeedInfoRWriter defines the Reader and Writer for ChangeFeedInfo
type ChangeFeedInfoRWriter interface {
	// Read the changefeed info from storage such as etcd.
	Read(ctx context.Context) (map[ChangeFeedID]ProcessorsInfos, error)
	// Write the changefeed info to storage such as etcd.
	Write(ctx context.Context, infos map[ChangeFeedID]*ChangeFeedInfo) error
}

// TODO edit sub change feed
type ownerImpl struct {
	changeFeedInfos map[ChangeFeedID]*ChangeFeedInfo

	ddlHandler OwnerDDLHandler
	cfRWriter  ChangeFeedInfoRWriter

	ddlResolvedTS uint64
	targetTS      uint64
	ddlJobHistory []*model.Job

	l sync.RWMutex

	manager Manager
}

// NewOwner creates a new ownerImpl instance
func NewOwner(targetTS uint64, manager Manager) *ownerImpl {
	owner := &ownerImpl{
		changeFeedInfos: make(map[ChangeFeedID]*ChangeFeedInfo),
		ddlHandler:      NewDDLHandler(),
		cfRWriter:       NewCfInfoReadWriter(),
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
		if cfInfo, exist := o.changeFeedInfos[changeFeedID]; exist {
			cfInfo.processorInfos = etcdChangeFeedInfo
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

func (o *ownerImpl) getChangeFeedInfo(changeFeedID string) (*ChangeFeedInfo, error) {
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
	return cfInfo.resolvedTS, nil
}

func (o *ownerImpl) GetCheckpointTS(changeFeedID string) (uint64, error) {
	o.l.RLock()
	defer o.l.RUnlock()
	cfInfo, err := o.getChangeFeedInfo(changeFeedID)
	if err != nil {
		return 0, err
	}
	return cfInfo.checkpointTS, nil
}

func (o *ownerImpl) calcResolvedTS() error {
	for _, cfInfo := range o.changeFeedInfos {
		if cfInfo.status != ChangeFeedSyncDML {
			continue
		}
		minResolvedTS := o.targetTS

		// calc the min of all resolvedTS in captures
		for _, pStatus := range cfInfo.processorInfos {
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
		if len(o.ddlJobHistory) > cfInfo.ddlCurrentIndex &&
			minResolvedTS > o.ddlJobHistory[cfInfo.ddlCurrentIndex].BinlogInfo.FinishedTS {
			minResolvedTS = o.ddlJobHistory[cfInfo.ddlCurrentIndex].BinlogInfo.FinishedTS
			cfInfo.status = ChangeFeedWaitToExecDDL
		}
		cfInfo.resolvedTS = minResolvedTS
	}
	return nil
}

func (o *ownerImpl) handleDDL(ctx context.Context) error {
waitCheckpointTSLoop:
	for changeFeedID, cfInfo := range o.changeFeedInfos {
		if cfInfo.status != ChangeFeedWaitToExecDDL {
			continue waitCheckpointTSLoop
		}
		todoDDLJob := o.ddlJobHistory[cfInfo.ddlCurrentIndex]

		// Check if all the checkpointTs of capture are achieving global resolvedTS(which is equal to todoDDLJob.FinishedTS)
		for _, pInfo := range cfInfo.processorInfos {
			if pInfo.CheckPointTS != todoDDLJob.BinlogInfo.FinishedTS {
				continue waitCheckpointTSLoop
			}
		}

		// Execute DDL Job asynchronously
		cfInfo.status = ChangeFeedExecDDL
		go func(changeFeedID string, cfInfo *ChangeFeedInfo) {
			err := o.ddlHandler.ExecDDL(todoDDLJob)
			o.l.Lock()
			defer o.l.Unlock()
			// If DDL executing failed, pause the changefeed and print log
			if err != nil {
				cfInfo.status = ChangeFeedDDLExecuteFailed
				log.Error("Execute DDL failed",
					zap.String("ChangeFeedID", changeFeedID),
					zap.Error(err),
					zap.Reflect("ddlJob", todoDDLJob))
				return
			}
			if cfInfo.status != ChangeFeedExecDDL {
				log.Fatal("changeFeedState must be ChangeFeedExecDDL when DDL is executed",
					zap.String("ChangeFeedID", changeFeedID),
					zap.String("ChangeFeedState", cfInfo.status.String()))
			}
			cfInfo.ddlCurrentIndex += 1
			cfInfo.status = ChangeFeedSyncDML
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
