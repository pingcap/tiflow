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

package cdc

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pmodel "github.com/pingcap/parser/model"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/roles"
	"github.com/pingcap/ticdc/cdc/roles/storage"
	"github.com/pingcap/ticdc/cdc/schema"
	"go.uber.org/zap"
)

// OwnerDDLHandler defines the ddl handler for Owner
// which can pull ddl jobs and execute ddl jobs
type OwnerDDLHandler interface {
	// PullDDL pulls the ddl jobs and returns resolvedTs of DDL Puller and job list.
	PullDDL() (resolvedTs uint64, jobs []*model.DDL, err error)

	// ExecDDL executes the ddl job
	ExecDDL(ctx context.Context, sinkURI string, ddl *model.DDL) error
}

// ChangeFeedInfoRWriter defines the Reader and Writer for changeFeedInfo
type ChangeFeedInfoRWriter interface {
	// Read the changefeed info from storage such as etcd.
	Read(ctx context.Context) (map[model.ChangeFeedID]*model.ChangeFeedDetail, map[model.ChangeFeedID]model.ProcessorsInfos, error)
	// Write the changefeed info to storage such as etcd.
	Write(ctx context.Context, infos map[model.ChangeFeedID]*model.ChangeFeedInfo) error
}

type changeFeedInfo struct {
	ID     string
	detail *model.ChangeFeedDetail
	*model.ChangeFeedInfo

	schema         *schema.Storage
	Status         model.ChangeFeedStatus
	TargetTs       uint64
	ProcessorInfos model.ProcessorsInfos

	client          *clientv3.Client
	DDLCurrentIndex int
	ddlHandler      OwnerDDLHandler
	ddlResolvedTs   uint64
	ddlJobHistory   []*model.DDL

	tables        map[uint64]schema.TableName
	orphanTables  map[uint64]model.ProcessTableInfo
	toCleanTables map[uint64]struct{}
	infoWriter    *storage.OwnerSubCFInfoEtcdWriter
}

// String implements fmt.Stringer interface.
func (c *changeFeedInfo) String() string {
	format := "{\n ID: %s\n detail: %+v\n info: %+v\n Status: %v\n ProcessorInfos: %+v\n tables: %+v\n orphanTables: %+v\n toCleanTables: %v\n DDLCurrentIndex: %d\n ddlResolvedTs: %d\n ddlJobHistory: %+v\n}\n\n"
	s := fmt.Sprintf(format,
		c.ID, c.detail, c.ChangeFeedInfo, c.Status, c.ProcessorInfos, c.tables,
		c.orphanTables, c.toCleanTables, c.DDLCurrentIndex, c.ddlResolvedTs, c.ddlJobHistory)

	if c.DDLCurrentIndex < len(c.ddlJobHistory) {
		job := c.ddlJobHistory[c.DDLCurrentIndex]
		s += fmt.Sprintf("next to exec job: %s query: %s\n\n", job, job.Job.Query)
	}

	return s
}

// filter return true if we should not sync the table to downstream.
// we can add configuration support at the detail.
func filter(_ *model.ChangeFeedDetail, table schema.TableName) bool {
	ignoreSchema := []string{"INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA", "mysql"}

	log.Debug("filter table", zap.Stringer("table", table))

	for _, schema := range ignoreSchema {
		if schema == table.Schema {
			return true
		}
	}

	return false
}

func (c *changeFeedInfo) addTable(id, startTs uint64, table schema.TableName) {
	if filter(c.detail, table) {
		return
	}

	c.tables[id] = table
	c.orphanTables[id] = model.ProcessTableInfo{
		ID:      id,
		StartTs: startTs,
	}
}

func (c *changeFeedInfo) removeTable(id uint64) {
	delete(c.tables, id)

	if _, ok := c.orphanTables[id]; ok {
		delete(c.orphanTables, id)
	} else {
		c.toCleanTables[id] = struct{}{}
	}
}

func (c *changeFeedInfo) selectCapture(captures map[string]*model.CaptureInfo) string {
	return c.minimumTablesCapture(captures)
}

func (c *changeFeedInfo) minimumTablesCapture(captures map[string]*model.CaptureInfo) string {
	if len(captures) == 0 {
		return ""
	}

	for id := range captures {
		// We have not dispatch any table to this capture yet.
		if _, ok := c.ProcessorInfos[id]; !ok {
			return id
		}
	}

	var minCount int = math.MaxInt64
	var minID string

	for id, pinfo := range c.ProcessorInfos {
		if len(pinfo.TableInfos) < minCount {
			minID = id
			minCount = len(pinfo.TableInfos)
		}
	}

	return minID
}

func (c *changeFeedInfo) tryBalance(ctx context.Context, captures map[string]*model.CaptureInfo) {
	c.cleanTables(ctx)
	c.banlanceOrphanTables(ctx, captures)
}

func (c *changeFeedInfo) restoreTableInfos(infoSnapshot *model.SubChangeFeedInfo, captureID string) {
	c.ProcessorInfos[captureID].TableInfos = infoSnapshot.TableInfos
}

func (c *changeFeedInfo) cleanTables(ctx context.Context) {
	var cleanIDs []uint64

cleanLoop:
	for id := range c.toCleanTables {
		captureID, subInfo, ok := findSubChangefeedWithTable(c.ProcessorInfos, id)
		if !ok {
			log.Warn("ignore clean table id", zap.Uint64("id", id))
			cleanIDs = append(cleanIDs, id)
			continue
		}

		infoClone := subInfo.Clone()
		subInfo.RemoveTable(id)

		newInfo, err := c.infoWriter.Write(ctx, c.ID, captureID, subInfo, true)
		if err == nil {
			c.ProcessorInfos[captureID] = newInfo
		}
		switch errors.Cause(err) {
		case model.ErrFindPLockNotCommit:
			c.restoreTableInfos(infoClone, captureID)
			log.Info("write table info delay, wait plock resolve",
				zap.String("changefeed", c.ID),
				zap.String("capture", captureID))
		case nil:
			log.Info("cleanup table success",
				zap.Uint64("table id", id),
				zap.String("capture id", captureID))
			log.Debug("after remove", zap.Stringer("subchangefeed info", subInfo))
			cleanIDs = append(cleanIDs, id)
		default:
			c.restoreTableInfos(infoClone, captureID)
			log.Error("fail to put sub changefeed info", zap.Error(err))
			break cleanLoop
		}
	}

	for _, id := range cleanIDs {
		delete(c.toCleanTables, id)
	}
}

func findSubChangefeedWithTable(infos model.ProcessorsInfos, tableID uint64) (captureID string, info *model.SubChangeFeedInfo, ok bool) {
	for id, info := range infos {
		for _, table := range info.TableInfos {
			if table.ID == tableID {
				return id, info, true
			}
		}
	}

	return "", nil, false
}

func (c *changeFeedInfo) banlanceOrphanTables(ctx context.Context, captures map[string]*model.CaptureInfo) {
	if len(captures) == 0 {
		return
	}

	for tableID, orphan := range c.orphanTables {
		captureID := c.selectCapture(captures)
		if len(captureID) == 0 {
			return
		}

		info := c.ProcessorInfos[captureID]
		if info == nil {
			info = new(model.SubChangeFeedInfo)
		}
		infoClone := info.Clone()
		info.TableInfos = append(info.TableInfos, &model.ProcessTableInfo{
			ID:      tableID,
			StartTs: orphan.StartTs,
		})

		newInfo, err := c.infoWriter.Write(ctx, c.ID, captureID, info, false)
		if err == nil {
			c.ProcessorInfos[captureID] = newInfo
		}
		switch errors.Cause(err) {
		case model.ErrFindPLockNotCommit:
			c.restoreTableInfos(infoClone, captureID)
			log.Info("write table info delay, wait plock resolve",
				zap.String("changefeed", c.ID),
				zap.String("capture", captureID))
		case nil:
			log.Info("dispatch table success",
				zap.Uint64("table id", tableID),
				zap.Uint64("start ts", orphan.StartTs),
				zap.String("capture", captureID))
			delete(c.orphanTables, tableID)
		default:
			c.restoreTableInfos(infoClone, captureID)
			log.Error("fail to put sub changefeed info", zap.Error(err))
			return
		}
	}
}

func (c *changeFeedInfo) applyJob(job *pmodel.Job) error {
	log.Info("apply job", zap.String("sql", job.Query), zap.Int64("job id", job.ID))

	schamaName, tableName, _, err := c.schema.HandleDDL(job)
	if err != nil {
		return errors.Trace(err)
	}

	// case table id set may change
	switch job.Type {
	case pmodel.ActionCreateTable:
		addID := uint64(job.BinlogInfo.TableInfo.ID)
		c.addTable(addID, job.BinlogInfo.FinishedTS, schema.TableName{Schema: schamaName, Table: tableName})
	case pmodel.ActionDropTable:
		dropID := uint64(job.TableID)
		c.removeTable(dropID)
	case pmodel.ActionRenameTable:
		// no id change just update name
		c.tables[uint64(job.TableID)] = schema.TableName{Schema: schamaName, Table: tableName}
	case pmodel.ActionTruncateTable:
		dropID := uint64(job.TableID)
		c.removeTable(dropID)

		addID := uint64(job.BinlogInfo.TableInfo.ID)
		c.addTable(addID, job.BinlogInfo.FinishedTS, schema.TableName{Schema: schamaName, Table: tableName})
	default:
	}

	return nil
}

type ownerImpl struct {
	changeFeedInfos map[model.ChangeFeedID]*changeFeedInfo

	cfRWriter ChangeFeedInfoRWriter

	l sync.RWMutex

	pdEndpoints []string
	pdClient    pd.Client
	etcdClient  *clientv3.Client
	manager     roles.Manager

	captureWatchC      <-chan *CaptureInfoWatchResp
	cancelWatchCapture func()
	captures           map[model.CaptureID]*model.CaptureInfo
}

// NewOwner creates a new ownerImpl instance
func NewOwner(pdEndpoints []string, cli *clientv3.Client, manager roles.Manager) (*ownerImpl, error) {
	ctx, cancel := context.WithCancel(context.Background())
	infos, watchC, err := newCaptureInfoWatch(ctx, cli)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	captures := make(map[model.CaptureID]*model.CaptureInfo, len(infos))
	for _, info := range infos {
		captures[info.ID] = info
	}

	pdClient, err := pd.NewClient(pdEndpoints, pd.SecurityOption{})
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	owner := &ownerImpl{
		pdEndpoints:        pdEndpoints,
		pdClient:           pdClient,
		changeFeedInfos:    make(map[model.ChangeFeedID]*changeFeedInfo),
		cfRWriter:          storage.NewChangeFeedInfoEtcdRWriter(cli),
		etcdClient:         cli,
		manager:            manager,
		captureWatchC:      watchC,
		captures:           captures,
		cancelWatchCapture: cancel,
	}

	return owner, nil
}

func (o *ownerImpl) addCapture(info *model.CaptureInfo) {
	o.captures[info.ID] = info
}

func (o *ownerImpl) removeCapture(info *model.CaptureInfo) {
	delete(o.captures, info.ID)
}

func (o *ownerImpl) handleWatchCapture() error {
	for resp := range o.captureWatchC {
		if resp.Err != nil {
			return errors.Trace(resp.Err)
		}

		if resp.IsDelete {
			o.removeCapture(resp.Info)
		} else {
			o.addCapture(resp.Info)
		}
	}

	log.Info("handleWatchCapture quit")
	return nil
}

func (o *ownerImpl) loadChangeFeedInfos(ctx context.Context) error {
	changefeeds, pinfos, err := o.cfRWriter.Read(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	for changeFeedID, etcdChangeFeedInfo := range pinfos {
		var cfInfo *changeFeedInfo
		var exist bool

		if cfInfo, exist = o.changeFeedInfos[changeFeedID]; exist {
			cfInfo.ProcessorInfos = etcdChangeFeedInfo
			continue
		}

		detail := changefeeds[changeFeedID]
		log.Info("find new changefeed", zap.Reflect("detail", detail),
			zap.Uint64("checkpoint ts", detail.GetCheckpointTs()))

		// we find a new changefeed, init changefeed info here.
		var targetTs uint64
		changefeed, ok := changefeeds[changeFeedID]
		if !ok {
			return errors.Annotatef(model.ErrChangeFeedNotExists, "id:%s", changeFeedID)
		}

		if changefeed.TargetTs == uint64(0) {
			targetTs = uint64(math.MaxUint64)
		} else {
			targetTs = changefeed.TargetTs
		}

		schemaStorage, err := createSchemaStore(o.pdEndpoints)
		if err != nil {
			return errors.Annotate(err, "create schema store failed")
		}

		err = schemaStorage.HandlePreviousDDLJobIfNeed(detail.GetCheckpointTs())
		if err != nil {
			return errors.Annotate(err, "handle ddl job failed")
		}

		ddlHandler := newDDLHandler(o.pdClient, detail.GetCheckpointTs())

		tables := make(map[uint64]schema.TableName)
		orphanTables := make(map[uint64]model.ProcessTableInfo)
		for id, table := range schemaStorage.CloneTables() {
			if filter(detail, table) {
				continue
			}

			tables[id] = table
			orphanTables[id] = model.ProcessTableInfo{
				ID:      id,
				StartTs: changefeed.StartTs,
			}
		}

		o.changeFeedInfos[changeFeedID] = &changeFeedInfo{
			detail:        detail,
			ID:            changeFeedID,
			client:        o.etcdClient,
			ddlHandler:    ddlHandler,
			schema:        schemaStorage,
			tables:        tables,
			orphanTables:  orphanTables,
			toCleanTables: make(map[uint64]struct{}),
			ChangeFeedInfo: &model.ChangeFeedInfo{
				SinkURI:      changefeed.SinkURI,
				ResolvedTs:   0,
				CheckpointTs: detail.GetCheckpointTs(),
			},
			Status:          model.ChangeFeedSyncDML,
			TargetTs:        targetTs,
			ProcessorInfos:  etcdChangeFeedInfo,
			DDLCurrentIndex: 0,
			infoWriter:      storage.NewOwnerSubCFInfoEtcdWriter(o.etcdClient),
		}
	}

	for _, info := range o.changeFeedInfos {
		info.tryBalance(ctx, o.captures)
	}

	return nil
}

func (o *ownerImpl) flushChangeFeedInfos(ctx context.Context) error {
	infos := make(map[model.CaptureID]*model.ChangeFeedInfo)
	for id, info := range o.changeFeedInfos {
		infos[id] = info.ChangeFeedInfo
	}
	return errors.Trace(o.cfRWriter.Write(ctx, infos))
}

func (c *changeFeedInfo) pullDDLJob() error {
	ddlResolvedTs, ddlJobs, err := c.ddlHandler.PullDDL()
	if err != nil {
		return errors.Trace(err)
	}
	c.ddlResolvedTs = ddlResolvedTs
	c.ddlJobHistory = append(c.ddlJobHistory, ddlJobs...)
	return nil
}

// calcResolvedTs update every changefeed's resolve ts and checkpoint ts.
func (o *ownerImpl) calcResolvedTs() error {
	for _, cfInfo := range o.changeFeedInfos {
		if cfInfo.Status != model.ChangeFeedSyncDML {
			continue
		}

		// ProcessorInfos don't contains the whole set table id now.
		if len(cfInfo.orphanTables) > 0 {
			continue
		}

		minResolvedTs := cfInfo.TargetTs
		minCheckpointTs := cfInfo.TargetTs

		if len(cfInfo.tables) == 0 {
			minCheckpointTs = cfInfo.CheckpointTs
		} else {
			// calc the min of all resolvedTs in captures
			for _, pStatus := range cfInfo.ProcessorInfos {
				if minResolvedTs > pStatus.ResolvedTs {
					minResolvedTs = pStatus.ResolvedTs
				}

				if minCheckpointTs > pStatus.CheckPointTs {
					minCheckpointTs = pStatus.CheckPointTs
				}
			}
		}

		// if minResolvedTs is greater than ddlResolvedTs,
		// it means that ddlJobHistory in memory is not intact,
		// there are some ddl jobs which finishedTs is smaller than minResolvedTs we don't know.
		// so we need to call `pullDDLJob`, update the ddlJobHistory and ddlResolvedTs.
		if minResolvedTs > cfInfo.ddlResolvedTs {
			if err := cfInfo.pullDDLJob(); err != nil {
				return errors.Trace(err)
			}

			if minResolvedTs > cfInfo.ddlResolvedTs {
				minResolvedTs = cfInfo.ddlResolvedTs
			}
		}

		// if minResolvedTs is greater than the finishedTS of ddl job which is not executed,
		// we need to execute this ddl job
		if len(cfInfo.ddlJobHistory) > cfInfo.DDLCurrentIndex &&
			minResolvedTs > cfInfo.ddlJobHistory[cfInfo.DDLCurrentIndex].Job.BinlogInfo.FinishedTS {
			minResolvedTs = cfInfo.ddlJobHistory[cfInfo.DDLCurrentIndex].Job.BinlogInfo.FinishedTS
			cfInfo.Status = model.ChangeFeedWaitToExecDDL
		}

		cfInfo.ResolvedTs = minResolvedTs

		if minCheckpointTs > cfInfo.CheckpointTs {
			cfInfo.CheckpointTs = minCheckpointTs
		}

		log.Debug("update changefeed", zap.String("id", cfInfo.ID),
			zap.Uint64("checkpoint ts", minCheckpointTs),
			zap.Uint64("resolved ts", minResolvedTs))
	}
	return nil
}

// handleDDL check if we can change the status to be `ChangeFeedExecDDL` and execute the DDL asynchronously
// if the status is in ChangeFeedWaitToExecDDL.
// After executing the DDL successfully, the status will be changed to be ChangeFeedSyncDML.
func (o *ownerImpl) handleDDL(ctx context.Context) error {
waitCheckpointTsLoop:
	for changeFeedID, cfInfo := range o.changeFeedInfos {
		if cfInfo.Status != model.ChangeFeedWaitToExecDDL {
			continue
		}
		todoDDLJob := cfInfo.ddlJobHistory[cfInfo.DDLCurrentIndex]

		// Check if all the checkpointTs of capture are achieving global resolvedTs(which is equal to todoDDLJob.FinishedTS)
		for _, pInfo := range cfInfo.ProcessorInfos {
			if pInfo.CheckPointTs != todoDDLJob.Job.BinlogInfo.FinishedTS {
				continue waitCheckpointTsLoop
			}
		}

		// Execute DDL Job asynchronously
		cfInfo.Status = model.ChangeFeedExecDDL
		log.Debug("apply job", zap.Stringer("job", todoDDLJob.Job),
			zap.String("query", todoDDLJob.Job.Query),
			zap.Uint64("ts", todoDDLJob.Job.BinlogInfo.FinishedTS))

		err := cfInfo.applyJob(todoDDLJob.Job)
		if err != nil {
			return errors.Trace(err)
		}

		cfInfo.banlanceOrphanTables(context.Background(), o.captures)

		err = cfInfo.ddlHandler.ExecDDL(ctx, cfInfo.SinkURI, todoDDLJob)
		// o.l.Lock()
		// defer o.l.Unlock()
		// If DDL executing failed, pause the changefeed and print log
		if err != nil {
			cfInfo.Status = model.ChangeFeedDDLExecuteFailed
			log.Error("Execute DDL failed",
				zap.String("ChangeFeedID", changeFeedID),
				zap.Error(err),
				zap.Reflect("ddlJob", todoDDLJob))
			return err
		}
		log.Info("Execute DDL succeeded",
			zap.String("ChangeFeedID", changeFeedID),
			zap.Reflect("ddlJob", todoDDLJob))
		if cfInfo.Status != model.ChangeFeedExecDDL {
			log.Fatal("changeFeedState must be ChangeFeedExecDDL when DDL is executed",
				zap.String("ChangeFeedID", changeFeedID),
				zap.String("ChangeFeedState", cfInfo.Status.String()))
		}
		cfInfo.DDLCurrentIndex += 1
		cfInfo.Status = model.ChangeFeedSyncDML
		return nil
	}

	return nil
}

// TODO avoid this tick style, this means we get `tickTime` latency here.
func (o *ownerImpl) Run(ctx context.Context, tickTime time.Duration) error {
	defer o.cancelWatchCapture()
	handleWatchCaptureC := make(chan error, 1)
	go func() {
		err := o.handleWatchCapture()
		if err != nil {
			handleWatchCaptureC <- err
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-handleWatchCaptureC:
			return errors.Annotate(err, "handleWatchCapture failed")
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

	err = o.calcResolvedTs()
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

func (o *ownerImpl) writeDebugInfo(w io.Writer) {
	for _, info := range o.changeFeedInfos {
		// fmt.Fprintf(w, "%+v\n", *info)
		fmt.Fprintf(w, "%s\n", info)
	}
}
