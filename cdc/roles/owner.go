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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"
)

type ProcessTableInfo struct {
	ID      uint64 `json:"id"`
	StartTS uint64 `json:"start-ts"`
}

// Owner is used to process etcd information for a capture with owner role
type Owner interface {
	// GetResolvedTS gets resolvedTS of a ChangeFeed
	GetResolvedTS(changeFeedID string) (uint64, error)

	// GetCheckpointTS gets CheckpointTS of a ChangeFeed
	GetCheckpointTS(changeFeedID string) (uint64, error)

	// Run a goroutine to handle Owner logic
	Run(context.Context, time.Duration) error
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

type ChangeFeedInfo struct {
	status       ChangeFeedStatus
	resolvedTS   uint64
	checkpointTS uint64

	processorInfos  ProcessorsInfos
	ddlCurrentIndex int
}

func (c *ChangeFeedInfo) Status() ChangeFeedStatus {
	return c.status
}

func (c *ChangeFeedInfo) ResolvedTS() uint64 {
	return c.resolvedTS
}

func (c *ChangeFeedInfo) CheckpointTS() uint64 {
	return c.checkpointTS
}

type ddlExecResult struct {
	changeFeedID ChangeFeedID
	job          *model.Job
	err          error
}

type OwnerDDLHandler interface {
	PullDDL() (resolvedTS uint64, jobs []*model.Job, err error)
	ExecDDL(*model.Job) error
}

type ChangeFeedInfoRWriter interface {
	Read(ctx context.Context) (map[ChangeFeedID]ProcessorsInfos, error)
	Write(ctx context.Context, infos map[ChangeFeedID]*ChangeFeedInfo) error
}

// TODO edit sub change feed
type ownerImpl struct {
	changeFeedInfos map[ChangeFeedID]*ChangeFeedInfo

	OwnerDDLHandler
	ChangeFeedInfoRWriter

	ddlResolvedTS  uint64
	targetTS       uint64
	ddlJobHistory  []*model.Job
	finishedDDLJob chan ddlExecResult

	l sync.RWMutex
}

func (o *ownerImpl) loadChangeFeedInfos(ctx context.Context) error {
	infos, err := o.ChangeFeedInfoRWriter.Read(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	// TODO: handle changefeed changed and the table of sub changefeed changed
	// TODO: find the first index of one changefeed in ddl jobs
	for changeFeedId, etcdChangeFeedInfo := range infos {
		if cfInfo, exist := o.changeFeedInfos[changeFeedId]; exist {
			cfInfo.processorInfos = etcdChangeFeedInfo
		}
	}
	return nil
}

func (o *ownerImpl) flushChangeFeedInfos(ctx context.Context) error {
	return errors.Trace(o.ChangeFeedInfoRWriter.Write(ctx, o.changeFeedInfos))
}

func (o *ownerImpl) pullDDLJob() error {
	ddlResolvedTS, ddlJobs, err := o.PullDDL()
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
		for _, pStatus := range cfInfo.processorInfos {
			if minResolvedTS > pStatus.ResolvedTS {
				minResolvedTS = pStatus.ResolvedTS
			}
		}
		if minResolvedTS > o.ddlResolvedTS {
			if err := o.pullDDLJob(); err != nil {
				return errors.Trace(err)
			}
			if minResolvedTS > o.ddlResolvedTS {
				minResolvedTS = o.ddlResolvedTS
			}
		}
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
			break
		}
		todoDDLJob := o.ddlJobHistory[cfInfo.ddlCurrentIndex]
		for _, pInfo := range cfInfo.processorInfos {
			if pInfo.CheckPointTS != todoDDLJob.BinlogInfo.FinishedTS {
				continue waitCheckpointTSLoop
			}
		}
		cfInfo.status = ChangeFeedExecDDL
		go func() {
			err := o.ExecDDL(todoDDLJob)
			o.finishedDDLJob <- ddlExecResult{changeFeedID, todoDDLJob, err}
		}()
	}
handleFinishedDDLLoop:
	for {
		select {
		case ddlExecRes, ok := <-o.finishedDDLJob:
			if !ok {
				break handleFinishedDDLLoop
			}
			cfInfo, exist := o.changeFeedInfos[ddlExecRes.changeFeedID]
			if !exist {
				return errors.NotFoundf("the changeFeedStatus of ChangeFeed(%s)", ddlExecRes.changeFeedID)
			}
			if ddlExecRes.err != nil {
				cfInfo.status = ChangeFeedDDLExecuteFailed
				log.Error("Execute DDL failed",
					zap.String("ChangeFeedID", ddlExecRes.changeFeedID),
					zap.Error(ddlExecRes.err),
					zap.Reflect("ddlJob", ddlExecRes.job))
				continue
			}
			if cfInfo.status != ChangeFeedExecDDL {
				log.Fatal("changeFeedState must be ChangeFeedExecDDL when DDL is executed",
					zap.String("ChangeFeedID", ddlExecRes.changeFeedID),
					zap.String("ChangeFeedState", cfInfo.status.String()))
			}
			cfInfo.ddlCurrentIndex += 1
			cfInfo.status = ChangeFeedSyncDML
		default:
			break handleFinishedDDLLoop
		}
	}
	return nil
}

func (o *ownerImpl) Run(ctx context.Context, tickTime time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(tickTime):
			err := o.run(ctx)
			if err != nil {
				return err
			}
		}
	}
}

func (o *ownerImpl) run(ctx context.Context) error {
	//o.l.Lock()
	//defer o.l.Unlock()
	err := o.loadChangeFeedInfos(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	err = o.calcResolvedTS()
	if err != nil {
		return errors.Trace(err)
	}
	err = o.handleDDL(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	err = o.flushChangeFeedInfos(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
